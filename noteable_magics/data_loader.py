import mimetypes

import pandas as pd
import sql.connection
import sql.magic
from IPython.core.magic import Magics, line_cell_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments
from IPython.utils.process import arg_split
from traitlets import Bool, Int, Unicode
from traitlets.config import Configurable

ENV_VAR_PREFIX = "SQL"
EXCEL_MIMETYPES = {
    "application/vnd.ms-excel",  # .xls
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # .xlsx
}
DB_NAME = "noteable"
CONN_NAME = f"@{DB_NAME}"


@magics_class
class NoteableDataLoaderMagic(Magics, Configurable):
    return_head = Bool(
        True, config=True, help="Return the first N rows from the loaded pandas dataframe"
    )
    display_example = Bool(True, config=True, help="Show example SQL query")
    display_connection_str = Bool(False, config=True, help="Show connection string after execute")
    pandas_limit = Int(10, config=True, help="The limit of rows to returns in the pandas dataframe")
    sqlite_db_location = Unicode(
        "sqlite:////tmp/ntbl.db", config=True, help="Where to store the sqlite database."
    )

    @line_cell_magic("create_or_replace_data_view")
    @magic_arguments()
    @argument("line", default="", nargs="*", type=str, help="Noteable SQL")
    @argument(
        "filepath", type=str, nargs=1, help="The filepath to the source file to use as a database"
    )
    @argument(
        "tablename",
        type=str,
        nargs=1,
        help="The name of the database table to load the file data into",
    )
    @argument(
        "-d", "--delimeter", type=str, default=",", required=False, help="Tabular data delimeter"
    )
    def execute(self, line="", cell=""):
        # workaround for https://github.com/ipython/ipython/issues/12729
        # TODO: switch back to parse_argstring in IPython 8.0
        argv = arg_split(line, posix=True, strict=False)
        args = self.execute.parser.parse_args(argv)
        source_file_path = args.filepath[0]
        tablename = args.tablename[0]

        mimetype, _ = mimetypes.guess_type(source_file_path)
        if mimetype == "text/csv" or source_file_path.endswith(".csv"):
            tmp_df = pd.read_csv(source_file_path, sep=args.delimeter)
        elif mimetype in EXCEL_MIMETYPES:
            tmp_df = pd.read_excel(source_file_path)
        elif mimetype == "application/json":
            tmp_df = pd.read_json(source_file_path)
        elif source_file_path.endswith(".feather"):
            tmp_df = pd.read_feather(source_file_path)
        elif source_file_path.endswith(".parquet"):
            tmp_df = pd.read_parquet(source_file_path)
        else:
            raise ValueError(f"File mimetype {mimetype} is not supported")

        if CONN_NAME not in sql.connection.Connection.connections:
            conn = sql.connection.Connection.set(self.sqlite_db_location, displaycon=False)
            conn.name = CONN_NAME
            sql.connection.Connection.connections[conn.name] = conn
            sql.connection.Connection.connections.pop(self.sqlite_db_location)
        else:
            conn = sql.connection.Connection.connections[CONN_NAME]

        tmp_df.to_sql(tablename, conn.session, if_exists="replace")

        if self.display_connection_str:
            print(f"Connect with: %sql {conn.name}")
        if self.display_example:
            print(
                "Create a SQL cell and then input query. "
                f"Example: \"SELECT * FROM '{tablename}' LIMIT 10\""
            )
        if self.return_head:
            return tmp_df.head(self.pandas_limit)
