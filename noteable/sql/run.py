from typing import List, Optional

import pandas as pd
import sqlalchemy
import sqlparse
from jinjasql import JinjaSql


class ResultSet:
    """
    Results of a SQL query.
    """

    # Result of a SELECT or perhaps INSERT INTO ... RETURNING projecting a result set.
    keys: Optional[List[str]] = None
    rows: Optional[list] = None

    # In case of an INSERT, UPDATE, or DELETE statement.
    rowcount: Optional[int] = None

    has_results_to_report: bool = True

    def __init__(self, sqla_result, sql, config):
        if sqla_result.returns_rows:
            self.keys = list(sqla_result.keys())
            self.rows = sqla_result.fetchall()
        elif sqla_result.rowcount != -1:
            # Was either DDL or perhaps DML like an INSERT or UPDATE statement
            # that just talks about number or rows affected server-side.
            self.rowcount = sqla_result.rowcount
        else:
            # CREATE TABLE or somesuch DDL that ran successfully and offers
            # no constructive feedback whatsoever.
            self.has_results_to_report = False

    @property
    def is_scalar_value(self) -> bool:
        return self.has_results_to_report and (
            (self.rowcount is not None) or (len(self.rows) == 1 and len(self.rows[0]) == 1)
        )

    @property
    def scalar_value(self):
        """Return either the only row / column value, or the affected num of rows
        from an INSERT/DELETE/UPDATE statement as bare scalar"""

        # Should only be called if self.is_scalar_value
        if self.rowcount is not None:
            return self.rowcount
        else:
            return self.rows[0][0]

    @property
    def can_become_dataframe(self) -> bool:
        return self.has_results_to_report and self.rows is not None

    def to_dataframe(self) -> Optional[pd.DataFrame]:
        "Returns a Pandas DataFrame instance built from the result set, if possible."

        # Should only be called if self.can_become_dataframe is True

        # Worst case will be a zero row but defined columns dataframe.
        return pd.DataFrame(self.rows, columns=self.keys)


def interpret_rowcount(rowcount):
    if rowcount < 0:
        result = "Done."
    else:
        if rowcount != 1:
            noun = 'rows'
        else:
            noun = 'row'

        result = f"{rowcount} {noun} affected."

    return result


# some dialects have autocommit
# specific dialects break when commit is used:
_COMMIT_BLACKLIST_DIALECTS = {
    "athena",
    "clickhouse",
    "ingres",
    "mssql",
    "teradata",
    "vertica",
}


def add_commit_blacklist_dialect(dialect: str):
    """Add a dialect to the blacklist of dialects that do not support commit."""

    if "+" in dialect:
        raise ValueError("Dialects do not have '+' inside (thats a dialect+driver combo)")

    _COMMIT_BLACKLIST_DIALECTS.add(dialect)


def _commit(conn, config):
    """Issues a commit, if appropriate for current config and dialect"""

    _should_commit = config.autocommit and all(
        dialect not in str(conn.dialect) for dialect in _COMMIT_BLACKLIST_DIALECTS
    )

    if _should_commit:
        try:
            conn.session.execute("commit")
        except sqlalchemy.exc.OperationalError:
            pass  # not all engines can commit


jinja_sql = JinjaSql(param_style='numeric')

##
# Why we use JinjaSql in numeric mode when feeding into SQLA text() expressions.
#
# We originally used 'named' format, which worked great in combination with SQLA text() parameter
# syntax, up until trying to do object attribute dereferencing in jinja expressions
# ("select id from foo where id = {{myobj.id}}"), at which time the resulting de-templated query
# and bind params would be "select id from foo where id = :myobj.id" and {'myobj.id': 1}, which was
# at least self-consistent until SQLA text() tries to convert that into the database driver's preferred parameter format
# (say, pyformat) and would end up with *closing the parens too early* due to seeing the non-identifier
# token '.': "select id from foo where id = %(myobj)s.id" with bind dict {'myobj.id': 1}, and croak
# due to not finding exactly 'myobj' as key in the bind dict (had it made it past that, the query
# still would have failed because the trailing '.id' would have been presented to the DB, which was never
# the intent).
#
# So, to ensure that we pass only brain-dead-simple paramater expansion needs into the SQL text() expression,
# we now drive jinja in numeric mode, so that it replaces templated variable expressions with just
# a colon and a number ("select id from foo where id = :1"). It hands us back a list instead of a dict
# as the bind parameters, but is easy enough to transform that list [12] into the dict of
# string keys -> values ({'1': 12}) which SQLA's text() + conn.execute() expect for such a text() template.
#
# People don't much combine JinjaSQL on top of SQLA, in that they solve 'conditional composed SQL'
# completely differently. We're special I guess?
##


def run(conn, sql, config, user_namespace, skip_boxing_scalar_result: bool):

    if sql.strip():
        for statement in sqlparse.split(sql):
            first_word = sql.strip().split()[0].lower()
            if first_word == "begin":
                raise Exception("ipython_sql does not support transactions")

            query, bind_list = jinja_sql.prepare_query(statement, user_namespace)

            # Convert bind_list from positional list to dict per needs of a paramaterized text()
            # construct.
            bind_dict = {str(idx + 1): elem for (idx, elem) in enumerate(bind_list)}

            txt = sqlalchemy.sql.text(query)
            result = conn.session.execute(txt, bind_dict)

            _commit(conn=conn, config=config)

            if result and config.feedback:
                print(interpret_rowcount(result.rowcount))

        resultset = ResultSet(result, statement, config)

        if resultset.has_results_to_report:
            if resultset.can_become_dataframe:
                if skip_boxing_scalar_result and resultset.is_scalar_value:
                    return resultset.scalar_value
                else:
                    return resultset.to_dataframe()
            else:
                # Must have been INSERT/UPDATE/DELETE statement
                # just returning a rowcount.
                return resultset.rowcount
