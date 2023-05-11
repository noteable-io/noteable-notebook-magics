import pkg_resources

__version__ = pkg_resources.get_distribution("noteable").version

from .data_loader import NoteableDataLoaderMagic
from .datasources import bootstrap_datasources
from .logging import configure_logging
from .ntbl import NTBLMagic
from .sql.connection import bootstrap_duckdb
from .sql.magic import SqlMagic


def load_ipython_extension(ipython):
    configure_logging(False, "INFO", "DEBUG")

    # Initialize any remote datasource connections.
    bootstrap_datasources()

    # Initialize the noteable local (duck_db) database connection.
    bootstrap_duckdb()

    # Register all of our magics.
    ipython.register_magics(NoteableDataLoaderMagic, NTBLMagic, SqlMagic)
