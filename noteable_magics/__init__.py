__version__ = "1.0.0"

from .data_loader import NoteableDataLoaderMagic
from .logging import configure_logging
from .ntbl import NTBLMagic


def load_ipython_extension(ipython):
    configure_logging(False, "INFO", "DEBUG")
    ipython.register_magics(NoteableDataLoaderMagic, NTBLMagic)
