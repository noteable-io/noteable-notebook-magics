import pkg_resources

__version__ = pkg_resources.get_distribution("noteable_magics").version

from sql.run import add_commit_blacklist_dialect

from .data_loader import NoteableDataLoaderMagic, get_local_db_connection
from .datasources import bootstrap_datasources
from .logging import configure_logging
from .ntbl import NTBLMagic


def load_ipython_extension(ipython):

    # Initialize any remote datasource connections
    bootstrap_datasources()

    # Always prevent sql-magic from trying to autocommit bigquery,
    # for the legacy datasource support for Expel and whomever.
    add_commit_blacklist_dialect('bigquery')

    # Initialize the noteable local (sqlite) database connection
    get_local_db_connection()

    configure_logging(False, "INFO", "DEBUG")

    ipython.register_magics(NoteableDataLoaderMagic, NTBLMagic)
