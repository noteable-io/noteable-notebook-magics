import pkg_resources

__version__ = pkg_resources.get_distribution("noteable_magics").version

from .data_loader import LOCAL_DB_CONN_HANDLE, NoteableDataLoaderMagic, get_db_connection
from .datasources import bootstrap_datasources
from .logging import configure_logging
from .ntbl import NTBLMagic
from .sql.magic import SqlMagic
from .sql.run import add_commit_blacklist_dialect


def load_ipython_extension(ipython):

    # Initialize any remote datasource connections
    bootstrap_datasources()

    # Always prevent sql-magic from trying to autocommit bigquery,
    # for the legacy datasource support for Expel and whomever.
    add_commit_blacklist_dialect('bigquery')

    # Initialize the noteable local (sqlite) database connection
    get_db_connection(LOCAL_DB_CONN_HANDLE)

    configure_logging(False, "INFO", "DEBUG")

    ipython.register_magics(NoteableDataLoaderMagic, NTBLMagic, SqlMagic)
