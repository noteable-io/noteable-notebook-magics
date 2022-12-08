from typing import List

import sqlalchemy
import sqlparse
from jinjasql import JinjaSql
import pandas as pd


class ResultSet:
    """
    Results of a SQL query.
    """

    def __init__(self, sqla_result, sql, config):
        self.keys: List[str] = list(sqla_result.keys())
        self.rows: list = sqla_result.fetchall()

    @property
    def is_scalar_value(self) -> bool:
        return len(self.rows) == 1 and len(self.rows[0]) == 1

    @property
    def scalar_value(self):
        """Return the only row / column value as bare scalar"""

        # Should only be called if self.is_scalar_value
        return self.rows[0][0]

    def to_dataframe(self) -> pd.DataFrame:
        "Returns a Pandas DataFrame instance built from the result set."

        return pd.DataFrame(self.rows, columns=(self.rows and self.keys) or [])


def interpret_rowcount(rowcount):
    if rowcount < 0:
        result = "Done."
    else:
        result = "%d rows affected." % rowcount
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


jinja_sql = JinjaSql(param_style='named')


def run(conn, sql, config, user_namespace, skip_boxing_scalar_result: bool):

    if sql.strip():
        for statement in sqlparse.split(sql):
            first_word = sql.strip().split()[0].lower()
            if first_word == "begin":
                raise Exception("ipython_sql does not support transactions")

            query, bind_params = jinja_sql.prepare_query(statement, user_namespace)
            txt = sqlalchemy.sql.text(query)
            result = conn.session.execute(txt, bind_params)

            _commit(conn=conn, config=config)
            if result and config.feedback:
                print(interpret_rowcount(result.rowcount))

        resultset = ResultSet(result, statement, config)

        if skip_boxing_scalar_result and resultset.is_scalar_value:
            return resultset.scalar_value
        else:
            return resultset.to_dataframe()
