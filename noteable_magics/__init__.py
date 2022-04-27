import pkg_resources

__version__ = pkg_resources.get_distribution("noteable_magics").version

from .data_loader import NoteableDataLoaderMagic, get_local_db_connection
from .datasources import bootstrap_datasources
from .logging import configure_logging
from .ntbl import NTBLMagic


def load_ipython_extension(ipython):

    # initialize the noteable local (sqlite) database connection
    get_local_db_connection()

    configure_logging(False, "INFO", "DEBUG")

    ipython.register_magics(NoteableDataLoaderMagic, NTBLMagic)
