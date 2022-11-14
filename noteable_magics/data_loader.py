import mimetypes
from typing import Optional

import pandas as pd
from IPython.core.magic import Magics, line_cell_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments
from IPython.utils.process import arg_split
from traitlets import Bool, Int
from traitlets.config import Configurable

from noteable_magics.sql.connection import Connection

EXCEL_MIMETYPES = {
    "application/vnd.ms-excel",  # .xls
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # .xlsx
}
LOCAL_DB_CONN_HANDLE = "@noteable"
duckdb_location = "duckdb:///:memory:"


def get_db_connection(sql_cell_handle_or_human_name: str) -> Optional['Connection']:
    """Return the sql.connection.Connection corresponding to the requested
        datasource sql_cell_handle or human name.

    If the cell handle happens to correspond to the 'local database' DuckDB database,
    then we will bootstrap it upon demand. Otherwise, try to find and return
    the connection.

    Will return None if the given handle isn't @noteable and isn't present in
    the connections dict already (created after this kernel was launched?)
    """
    if (
        sql_cell_handle_or_human_name == LOCAL_DB_CONN_HANDLE
        and sql_cell_handle_or_human_name not in Connection.connections
    ):
        # Bootstrap the DuckDB database if asked and needed.
        return Connection.set(
            duckdb_location,
            human_name="Local Database",
            name=LOCAL_DB_CONN_HANDLE,
        )
    else:
        # If, say, they created the datasource *after* this kernel was launched, then
        # this will come up empty and the caller should handle gracefully.
        for conn in Connection.connections.values():
            if (
                conn.name == sql_cell_handle_or_human_name
                or conn.human_name == sql_cell_handle_or_human_name
            ):
                return conn


@magics_class
class NoteableDataLoaderMagic(Magics, Configurable):
    return_head = Bool(
        True, config=True, help="Return the first N rows from the loaded pandas dataframe"
    )
    display_example = Bool(True, config=True, help="Show example SQL query")
    display_connection_str = Bool(False, config=True, help="Show connection string after execute")
    pandas_limit = Int(10, config=True, help="The limit of rows to returns in the pandas dataframe")

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
    @argument(
        "-i",
        "--include-index",
        action="store_true",
        help="Store index column from dataframe in sql",
    )
    @argument(
        "-c",
        "--connection",
        type=str,
        default=LOCAL_DB_CONN_HANDLE,
        required=False,
        help="Connection name or handle identifying the datasource to populate. Defaults to local DuckDB datasource.",
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

        conn = get_db_connection(args.connection)
        if not conn:
            raise ValueError(
                f"Could not find datasource identified by {args.connection!r}. Perhaps restart the kernel?"
            )

        tmp_df.to_sql(tablename, conn.session, if_exists="replace", index=args.include_index)

        if self.display_connection_str:
            print(f"Connect with: %sql {conn.name}")

        if self.display_example:
            if conn.human_name:
                noun = f'{conn.human_name!r}'
            else:
                # Hmm. "Legacy" created datasource. Err on the engine's dialect name?
                noun = conn._engine.dialect.name
            print(
                f"""Create a {noun} SQL cell and then input query. """
                f"Example: 'SELECT * FROM \"{tablename}\" LIMIT 10'"
            )

        if self.return_head:
            return tmp_df.head(self.pandas_limit)
