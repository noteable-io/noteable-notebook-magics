import sys

from IPython.core.magic import Magics, cell_magic, line_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments
from sqlalchemy.exc import (
    DatabaseError,
    InterfaceError,
    InternalError,
    OperationalError,
    ProgrammingError,
)

from noteable.sql.connection import get_connection_registry
import noteable.sql.parse
import noteable.sql.run
from noteable.sql.meta_commands import MetaCommandException, run_meta_command

try:
    from traitlets import Bool
    from traitlets.config.configurable import Configurable
except ImportError:
    from IPython.config.configurable import Configurable
    from IPython.utils.traitlets import Bool
try:
    from pandas.core.frame import DataFrame, Series
except ImportError:
    DataFrame = None
    Series = None


@magics_class
class SqlMagic(Magics, Configurable):
    """Runs SQL statement on a database, specified by SQLAlchemy connect string.

    Provides the %%sql magic."""

    autopandas = Bool(
        False,
        config=True,
        help="Return Pandas DataFrames instead of regular result sets",
    )
    feedback = Bool(True, config=True, help="Print number of rows affected by DML")
    autocommit = Bool(True, config=True, help="Set autocommit mode")

    def __init__(self, shell):
        Configurable.__init__(self, config=shell.config)
        Magics.__init__(self, shell=shell)

        # Add ourself to the list of module configurable via %config
        self.shell.configurables.append(self)

    @needs_local_scope
    @line_magic("sql")
    @cell_magic("sql")
    @magic_arguments()
    @argument("line", default="", nargs="*", type=str, help="sql")
    def execute(self, line="", cell="", local_ns={}):  # noqa: C901
        """Runs SQL statement against a database, specified by SQLAlchemy connect string.

        If no database connection has been established, first word
        should be a SQLAlchemy connection string, or the user@db name
        of an established connection.

        Examples::

          %%sql postgresql://me:mypw@localhost/mydb
          SELECT * FROM mytable

          %%sql me@mydb
          DELETE FROM mytable

          %%sql
          DROP TABLE mytable

        SQLAlchemy connect string syntax examples:

          postgresql://me:mypw@localhost/mydb
          sqlite://
          duckdb:///:memory:
          mysql+pymysql://me:mypw@localhost/mydb

        """

        # save globals and locals so they can be referenced in bind vars
        user_ns = self.shell.user_ns.copy()
        user_ns.update(local_ns)

        command_text = line + "\n" + cell

        parsed = noteable.sql.parse.parse(command_text, self)

        datasource_sql_cell_hande = parsed["connection"]

        # Get ahold of the connection to use. Original sql-magic lifecycle was to create connections
        # on the fly within cells via magic invocations. Said connection would then become the
        # default for use for future invocations that lacked an explicit connection string (hence
        # use of `set()` here. But at Noteable, we bootstrap the kernel's datasources from info in Vault
        # at startup time, and don't really advise / support creating new ones on the fly within kernel
        # sessions. So, of this get-and-set-default-and-or-create behavior that this `set()` call performs, we only really
        # expect to use the get portion. If an unknown datasource handle (say, handle of a datasource created _after_
        # kernel launch / our bootstrapping) gets passed into here, an exception will be raised.
        try:
            conn = get_connection_registry().get(datasource_sql_cell_hande)
        except noteable.sql.connection.UnknownConnectionError as e:
            # Cell referenced a datasource we don't know about. Exception will have a short + sweet message.
            eprint(str(e))
            return None

        if not parsed["sql"]:
            # Nothing at all to do. Perhaps cell was all comments (stripped away inside of parse()) or otherwise blank?
            return

        try:
            if parsed["sql"].startswith('\\'):
                # Is a meta command, say, to introspect schema or table such as "\describe foo"
                # Will make calls to display() as well as handle doing any result_var assignments into
                # self.shell.user_ns.
                run_meta_command(self.shell, conn, parsed["sql"], parsed.get("result_var"))
                return

            # Is a vanilla SQL statement. Run it.
            result = noteable.sql.run.run(
                conn,
                parsed["sql"],
                self,
                user_ns,
                skip_boxing_scalar_result=parsed['skip_boxing_scalar_result'],
            )

            if parsed["result_var"]:
                # Silently assign the result to this named variable, ENG-4711.
                self.shell.user_ns[parsed["result_var"]] = result

            # Always return query results into the default ipython _ variable
            return result

        except (
            ProgrammingError,
            InternalError,
            InterfaceError,
            DatabaseError,
            OperationalError,
            MetaCommandException,
        ) as e:
            # Normal syntax errors, missing table, etc. should come back as
            # ProgrammingError. And the rest indicate something fundamentally
            # broken at the DBAPI layer.
            #
            # BUT of course sqlite returns ALL errors as OperationalError. Sigh.

            is_fatal = not isinstance(e, (ProgrammingError, MetaCommandException)) and not (
                isinstance(e, OperationalError) and "sqlite" in str(e)
            )

            if is_fatal:
                #
                # Some sort of DBAPI-level error. Let's be conservative an err on the
                # side of force-closing all of the engine's connections. This happens
                # to Databricks if you leave the connection open and idle too long, but
                # the existing connection is completely poisoned. We gots to
                # dispose of the existing connection pool and make sure we get a brand
                # new connection next time so that when the user does what users
                # do in the face of errors (just hit the run button again), we want
                # to try to give 'em a different experience the next go around.
                #
                # "Restart Kernel" is too big of a hammer here.
                #
                conn.reset_connection_pool()

                eprint(
                    "Encoutered the following unexpected exception while trying to run the statement."
                    " Closed the connection just to be safe. Re-run the cell to try again!\n\n"
                )
            raise


def load_ipython_extension(ip):
    """Load the extension in IPython."""

    # this fails in both Firefox and Chrome for OS X.
    # I get the error: TypeError: IPython.CodeCell.config_defaults is undefined

    # js = "IPython.CodeCell.config_defaults.highlight_modes['magic_sql'] = {'reg':[/^%%sql/]};"
    ip.register_magics(SqlMagic)


def eprint(error_message: str) -> None:
    print(error_message, file=sys.stderr)
