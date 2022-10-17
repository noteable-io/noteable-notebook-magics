import json
import re

from IPython.core.magic import Magics, cell_magic, line_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from sqlalchemy.exc import (
    DatabaseError,
    InterfaceError,
    InternalError,
    OperationalError,
    ProgrammingError,
)

import noteable_magics.sql.connection
import noteable_magics.sql.parse
import noteable_magics.sql.run

try:
    from traitlets import Bool, Int, Unicode
    from traitlets.config.configurable import Configurable
except ImportError:
    from IPython.config.configurable import Configurable
    from IPython.utils.traitlets import Bool, Int, Unicode
try:
    from pandas.core.frame import DataFrame, Series
except ImportError:
    DataFrame = None
    Series = None


@magics_class
class SqlMagic(Magics, Configurable):
    """Runs SQL statement on a database, specified by SQLAlchemy connect string.

    Provides the %%sql magic."""

    displaycon = Bool(True, config=True, help="Show connection string after execute")
    autolimit = Int(
        0,
        config=True,
        allow_none=True,
        help="Automatically limit the size of the returned result sets",
    )
    style = Unicode(
        "DEFAULT",
        config=True,
        help="Set the table printing style to any of prettytable's defined styles (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM)",
    )
    short_errors = Bool(
        True,
        config=True,
        help="Don't display the full traceback on SQL Programming Error",
    )
    displaylimit = Int(
        None,
        config=True,
        allow_none=True,
        help="Automatically limit the number of rows displayed (full result set is still stored)",
    )
    autopandas = Bool(
        False,
        config=True,
        help="Return Pandas DataFrames instead of regular result sets",
    )
    column_local_vars = Bool(
        False, config=True, help="Return data into local variables from column names"
    )
    feedback = Bool(True, config=True, help="Print number of rows affected by DML")
    dsn_filename = Unicode(
        "odbc.ini",
        config=True,
        help="Path to DSN file. "
        "When the first argument is of the form [section], "
        "a sqlalchemy connection string is formed from the "
        "matching section in the DSN file.",
    )
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
    @argument("-l", "--connections", action="store_true", help="list active connections")
    @argument("-x", "--close", type=str, help="close a session by name")
    @argument("-c", "--creator", type=str, help="specify creator function for new connection")
    @argument(
        "-s",
        "--section",
        type=str,
        help="section of dsn_file to be used for generating a connection string",
    )
    @argument(
        "-p",
        "--persist",
        action="store_true",
        help="create a table name in the database from the named DataFrame",
    )
    @argument(
        "--append",
        action="store_true",
        help="create, or append to, a table name in the database from the named DataFrame",
    )
    @argument(
        "-a",
        "--connection_arguments",
        type=str,
        help="specify dictionary of connection arguments to pass to SQL driver",
    )
    @argument("-f", "--file", type=str, help="Run SQL from file at this path")
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

        line = noteable_magics.sql.parse.without_sql_comment(parser=self.execute.parser, line=line)
        args = parse_argstring(self.execute, line)
        if args.connections:
            return noteable_magics.sql.connection.Connection.connections
        elif args.close:
            return noteable_magics.sql.connection.Connection._close(args.close)

        # save globals and locals so they can be referenced in bind vars
        user_ns = self.shell.user_ns.copy()
        user_ns.update(local_ns)

        command_text = " ".join(args.line) + "\n" + cell

        if args.file:
            with open(args.file, "r") as infile:
                command_text = infile.read() + "\n" + command_text

        parsed = noteable_magics.sql.parse.parse(command_text, self)

        connect_str = parsed["connection"]
        if args.section:
            connect_str = noteable_magics.sql.parse.connection_from_dsn_section(args.section, self)

        if args.connection_arguments:
            try:
                # check for string deliniators, we need to strip them for json parse
                raw_args = args.connection_arguments
                if len(raw_args) > 1:
                    targets = ['"', "'"]
                    head = raw_args[0]
                    tail = raw_args[-1]
                    if head in targets and head == tail:
                        raw_args = raw_args[1:-1]
                args.connection_arguments = json.loads(raw_args)
            except Exception as e:
                print(e)
                raise e
        else:
            args.connection_arguments = {}
        if args.creator:
            args.creator = user_ns[args.creator]

        # Get ahold of the connection to use. Original sql-magic lifecycle was to create connections
        # on the fly within cells via magic invocations. Said connection would then become the
        # default for use for future invocations that lacked an explicit connection string (hence
        # use of `set()` here. But at Noteable, we bootstrap the kernel's datasources from info in Vault
        # at startup time, and don't really advise / support creating new ones on the fly within kernel
        # sessions. So, of this get-and-set-default-and-or-create behavior that this `set()` call performs, we only really
        # expect to use the get portion. If an unknown datasource handle (say, handle of a datasource created _after_
        # kernel launch / our bootstrapping) gets passed into here, an exception will be raised.
        try:
            conn = noteable_magics.sql.connection.Connection.set(
                connect_str,
                displaycon=self.displaycon,
                connect_args=args.connection_arguments,
                creator=args.creator,
            )
        except noteable_magics.sql.connection.UnknownConnectionError as e:
            # Cell referenced a datasource we don't know about. Exception will have a short + sweet message.
            print(e)
            return None

        if args.persist:
            return self._persist_dataframe(parsed["sql"], conn, user_ns, append=False)

        if args.append:
            return self._persist_dataframe(parsed["sql"], conn, user_ns, append=True)

        if not parsed["sql"]:
            return

        try:
            result = noteable_magics.sql.run.run(conn, parsed["sql"], self, user_ns)

            if result is not None and not isinstance(result, str) and self.column_local_vars:
                # Instead of returning values, set variables directly in the
                # users namespace. Variable names given by column names

                if self.autopandas:
                    keys = result.keys()
                else:
                    keys = result.keys
                    result = result.dict()

                if self.feedback:
                    print("Returning data to local variables [{}]".format(", ".join(keys)))

                self.shell.user_ns.update(result)

                return None
            else:
                if parsed["result_var"]:
                    # Silently assign the result to this named variable, ENG-4711.
                    self.shell.user_ns.update({parsed["result_var"]: result})

                # Always return query results into the default ipython _ variable
                return result

        except (
            ProgrammingError,
            InternalError,
            InterfaceError,
            DatabaseError,
            OperationalError,
        ) as e:

            # Normal syntax errors, missing table, etc. should come back as
            # ProgrammingError. And the rest indicate something fundamentally
            # broken at the DBAPI layer.
            #
            # BUT of course sqlite returns ALL errors as OperationalError. Sigh.

            is_fatal = not isinstance(e, ProgrammingError) and not (
                isinstance(e, OperationalError) and "sqlite" in str(e)
            )

            if not is_fatal:
                if self.short_errors:
                    print(e)
                else:
                    raise
            else:
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
                conn._engine.dispose()
                conn._session = None
                print(
                    "Encoutered the following unexpected exception while trying to run the statement."
                    " Closed the connection just to be safe. Re-run the cell to try again!\n\n"
                )
                raise

    legal_sql_identifier = re.compile(r"^[A-Za-z0-9#_$]+")

    def _persist_dataframe(self, raw, conn, user_ns, append=False):
        """Implements PERSIST, which writes a DataFrame to the RDBMS"""
        if not DataFrame:
            raise ImportError("Must `pip install pandas` to use DataFrames")

        frame_name = raw.strip(";")

        # Get the DataFrame from the user namespace
        if not frame_name:
            raise SyntaxError("Syntax: %sql --persist <name_of_data_frame>")
        try:
            frame = eval(frame_name, user_ns)
        except SyntaxError:
            raise SyntaxError("Syntax: %sql --persist <name_of_data_frame>")
        if not isinstance(frame, DataFrame) and not isinstance(frame, Series):
            raise TypeError("%s is not a Pandas DataFrame or Series" % frame_name)

        # Make a suitable name for the resulting database table
        table_name = frame_name.lower()
        table_name = self.legal_sql_identifier.search(table_name).group(0)

        if_exists = "append" if append else "fail"
        frame.to_sql(table_name, conn.session.engine, if_exists=if_exists)
        return "Persisted %s" % table_name


def load_ipython_extension(ip):
    """Load the extension in IPython."""

    # this fails in both Firefox and Chrome for OS X.
    # I get the error: TypeError: IPython.CodeCell.config_defaults is undefined

    # js = "IPython.CodeCell.config_defaults.highlight_modes['magic_sql'] = {'reg':[/^%%sql/]};"
    ip.register_magics(SqlMagic)
