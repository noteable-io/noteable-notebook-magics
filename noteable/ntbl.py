from dataclasses import dataclass
from typing import List

import click
import structlog
from click.exceptions import Abort, Exit, UsageError
from IPython.core.magic import Magics, line_cell_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments
from IPython.utils.process import arg_split
from rich import print as rprint
from traitlets.config import Configurable

from .command import NTBLCommand
from .planar_ally_client.api import PlanarAllyAPI
from .planar_ally_client.errors import PlanarAllyError
from .util import catch_em_all

from .datasets.impl import pull_dataset_verbosely, push_dataset_verbosely

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class ContextObject:
    planar_ally: PlanarAllyAPI
    magic: "NTBLMagic"


@magics_class
class NTBLMagic(Magics, Configurable):
    @catch_em_all
    @line_cell_magic("ntbl")
    @magic_arguments()
    @argument("line", default="", nargs="*", type=str, help="Noteable magic")
    def execute(self, line="", cell=""):
        argv = arg_split(line, posix=True, strict=False)
        argv.extend(arg_split(cell, posix=True, strict=False))

        try:
            ctx_obj = None  # XXX TODO see if can do away with altogether?
            with ntbl_magic.make_context(info_name="%ntbl", args=argv, obj=ctx_obj) as ctx:
                return ntbl_magic.invoke(ctx)
        except UsageError as e:
            e.show()
            raise
        except Exit as ex:
            if ex.exit_code != 0:
                raise ex
        except Abort:
            rprint("[red]Aborted[/red]")
            raise
        except PlanarAllyError as e:
            logger.exception("got an error from planar-ally")
            rprint(f"[red]{e.user_error()}[/red]")
            raise

        return None


@click.group(name="%ntbl", help="Noteable magic")
def ntbl_magic():
    pass


@ntbl_magic.command(help="Change planar-ally log level", cls=NTBLCommand, hidden=True)
@click.option("--app-level", help="New application log level", required=False, type=click.STRING)
@click.option("--ext-level", help="New external log level", required=False, type=click.STRING)
@click.option('--rtu-level', help="Set RTU-related log level", required=False, type=click.STRING)
def change_log_level(app_level, ext_level, rtu_level):
    planar_ally = PlanarAllyAPI()
    change_log_level(app_log_level=app_level, ext_log_level=ext_level, rtu_log_level=rtu_level)


@ntbl_magic.group(help="Push local updates to a remote store")
def push():
    pass


@ntbl_magic.group(help="Pull remote updates to the local file system")
def pull():
    pass


@push.command(name="datasets", cls=NTBLCommand)
@click.argument("path", nargs=-1)
def datasets_push(path: List[str]):
    """Push dataset files to the remote store

    PATH is the path of the dataset to push (e.g. My first dataset/data.csv, My first dataset).
    """

    push_dataset_verbosely(_join_path(path))


@pull.command(name="datasets", cls=NTBLCommand)
@click.argument("path", nargs=-1)
def datasets_pull(path: List[str]):
    """Push dataset files to the remote store

    PATH is the path of the dataset to pull (e.g. My first dataset/data.csv, My first dataset).
    """

    pull_dataset_verbosely(_join_path(path))


def _join_path(path_list: List[str]) -> str:
    path = " ".join(path_list)
    if "/" not in path:
        # The user is trying to push/pull the whole dataset
        path = f"{path}/"

    return path
