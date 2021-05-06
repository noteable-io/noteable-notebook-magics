__version__ = "1.0.0"

from .data_loader import NoteableDataLoaderMagic
from .ntbl import NTBLMagic


def load_ipython_extension(ipython):
    ipython.register_magics(NoteableDataLoaderMagic, NTBLMagic)
