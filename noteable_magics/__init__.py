__version__ = "0.2.0"

from .data_loader import NoteableDataLoaderMagic
from .ntbl import NTBLMagic


def load_ipython_extension(ipython):
    ipython.register_magics(NoteableDataLoaderMagic, NTBLMagic)
