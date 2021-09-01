import pkg_resources
import sql.run

__version__ = pkg_resources.get_distribution("noteable_magics").version

from .data_loader import NoteableDataLoaderMagic, get_connection
from .logging import configure_logging
from .ntbl import NTBLMagic


def load_ipython_extension(ipython):
    sql.run._COMMIT_BLACKLIST_DIALECTS = (
        "athena",
        "bigquery",
        "clickhouse",
        "ingres",
        "mssql",
        "teradata",
        "vertica",
    )
    # initialize the noteable database connection
    get_connection()
    configure_logging(False, "INFO", "DEBUG")
    ipython.register_magics(NoteableDataLoaderMagic, NTBLMagic)
