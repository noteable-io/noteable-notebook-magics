import sqlparse
from jinjasql import JinjaSql

from noteable.sql.connection import ResultSet


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

            result: ResultSet = conn.execute(query, bind_dict)

        # What to return from the final query?
        if result.has_results_to_report:
            if result.can_become_dataframe:
                if skip_boxing_scalar_result and result.is_scalar_value:
                    return result.scalar_value
                else:
                    return result.to_dataframe()
            else:
                # Must have been INSERT/UPDATE/DELETE statement
                # just returning a rowcount.
                return result.rowcount
