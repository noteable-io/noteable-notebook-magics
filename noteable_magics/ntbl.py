import os
from dataclasses import dataclass
from pathlib import PurePath
from typing import Any, Dict, Iterable, List

import click
import structlog
from click.exceptions import Abort, Exit, UsageError
from IPython.core.magic import Magics, line_cell_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments
from IPython.utils.process import arg_split
from rich import print as rprint
from rich.progress import Progress, TaskID
from rich.syntax import Syntax
from rich.table import Table
from traitlets import Float, Unicode
from traitlets.config import Configurable

from .command import NTBLCommand, OutputModel
from .git_service import GitDiff, GitService, GitStatus, GitUser
from .planar_ally_client.api import DatasetOperationStream, PlanarAllyAPI
from .planar_ally_client.errors import PlanarAllyError
from .planar_ally_client.types import (
    FileKind,
    FileProgressEndMessage,
    FileProgressStartMessage,
    FileProgressUpdateMessage,
    RemoteStatus,
    StreamErrorMessage,
    UserMessage,
)

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class ContextObject:
    planar_ally: PlanarAllyAPI
    git: GitService
    magic: "NTBLMagic"


@magics_class
class NTBLMagic(Magics, Configurable):
    planar_ally_api_url = Unicode(
        "http://localhost:7000/api", config=True, help="The URL to connect to for planar-ally"
    )
    planar_ally_default_timeout_seconds = Float(
        60.0,
        config=True,
        help="The total default timeout seconds when making a request to planar-ally",
    )
    project_dir = Unicode("project", config=True, help="The project path, relative or absolute")

    git_user_name = Unicode(
        "Noteable Kernel", config=True, help="The name of the user creating git commits"
    )
    git_user_email = Unicode(
        "engineering@noteable.io", config=True, help="The email of the user creating git commits"
    )

    @line_cell_magic("ntbl")
    @magic_arguments()
    @argument("line", default="", nargs="*", type=str, help="Noteable magic")
    def execute(self, line="", cell=""):
        argv = arg_split(line, posix=True, strict=False)
        argv.extend(arg_split(cell, posix=True, strict=False))

        ctx_obj = self._build_ctx()

        try:
            with ntbl_magic.make_context(info_name="%ntbl", args=argv, obj=ctx_obj) as ctx:
                return ntbl_magic.invoke(ctx)
        except UsageError as e:
            e.show()
        except Exit as ex:
            if ex.exit_code != 0:
                raise ex
        except Abort:
            rprint("[red]Aborted[/red]")
        except PlanarAllyError as e:
            logger.exception("got an error from planar-ally")
            rprint(f"[red]{e.user_error()}[/red]")

        return None

    def _build_ctx(self):
        planar_ally = PlanarAllyAPI(
            self.planar_ally_api_url,
            default_total_timeout_seconds=self.planar_ally_default_timeout_seconds,
        )
        git_service = GitService(
            self._get_full_project_path(),
            GitUser(name=self.git_user_name, email=self.git_user_email),
        )
        return ContextObject(planar_ally, git_service, magic=self)

    def _get_full_project_path(self) -> str:
        project_dir = PurePath(self.project_dir)
        if project_dir.is_absolute():
            return str(project_dir)
        return os.path.join(os.getcwd(), self.project_dir)


@click.group(name="%ntbl", help="Noteable magic")
def ntbl_magic():
    pass


@ntbl_magic.group(help="Commands related to this file's status")
def status():
    pass


@ntbl_magic.group(help="Show the full changes made to the local file system")
def diff():
    pass


@ntbl_magic.group(help="Push local updates to a remote store")
def push():
    pass


@ntbl_magic.group(help="Pull remote updates to the local file system")
def pull():
    pass


git_table_kwargs = {
    "show_header": False,
    "show_lines": True,
    "expand": True,
}


class ProjectStatusOutput(OutputModel):
    status: GitStatus

    def get_human_readable_output(self) -> Iterable[Any]:
        if self.status.has_changes():
            results = ["To push your changes use: %ntbl push project"]
            if self.status.changes_staged_for_commit:
                staged_table = Table(title="Changes staged", **git_table_kwargs)
                for change in self.status.changes_staged_for_commit:
                    staged_table.add_row(change.type.name.lower(), change.path, style="green")
                results.append(staged_table)

            if self.status.changes_not_staged_for_commit:
                not_staged_table = Table(title="Changes not staged", **git_table_kwargs)
                for change in self.status.changes_not_staged_for_commit:
                    not_staged_table.add_row(change.type.name.lower(), change.path, style="red")
                results.append(not_staged_table)

            if self.status.untracked_files:
                untracked_table = Table(title="Untracked files", **git_table_kwargs)
                for filename in self.status.untracked_files:
                    untracked_table.add_row(filename, style="red")
                results.append(untracked_table)
            return results
        return ["Up to date"]


class ProjectRemoteStatusOutput(OutputModel):
    status: RemoteStatus

    def get_human_readable_output(self) -> Iterable[Any]:
        if self.status.has_changes():
            changes = Table(title="Changes in S3 compared to local file system", **git_table_kwargs)
            for change in self.status.file_changes:
                changes.add_row(change.change_prefix, change.path, style=change.style)
            return ["To pull remote changes use: %ntbl pull project", changes]
        return ["Up to date"]


@status.command(name="project", help="Get the status about the current project", cls=NTBLCommand)
@click.option("--remote", help="Show the remote status compared to the local kernel", is_flag=True)
@click.pass_obj
def project_status(obj: ContextObject, remote: bool):
    if remote:
        remote_status = obj.planar_ally.fs(FileKind.project).get_remote_status("")
        return ProjectRemoteStatusOutput(status=remote_status)
    return ProjectStatusOutput(status=obj.git.status())


class SuccessfulUserMessageOutput(OutputModel):
    response: UserMessage

    def get_human_readable_output(self) -> Iterable[Any]:
        return [f"[green]{self.response.message}[/green]"]


@push.command(
    name="project", help="Push the project file changes to the remote store", cls=NTBLCommand
)
@click.pass_obj
def project_push(obj: ContextObject):
    resp = obj.planar_ally.fs(FileKind.project).push("")
    return SuccessfulUserMessageOutput(response=resp)


def process_file_update_stream(path: str, stream: DatasetOperationStream):
    expect_single_file = not path.endswith("/")
    got_file_update_msg = False
    error_message = None
    complete_message = None

    with Progress() as progress:
        tasks_by_file_path: Dict[str, TaskID] = {}

        for msg in stream:
            if isinstance(msg, StreamErrorMessage):
                error_message = msg.content.detail
                break
            elif isinstance(msg, FileProgressUpdateMessage):
                got_file_update_msg = True

                if msg.content.file_name not in tasks_by_file_path:
                    tasks_by_file_path[msg.content.file_name] = progress.add_task(
                        msg.content.file_name, total=100.0
                    )

                progress.update(
                    tasks_by_file_path[msg.content.file_name],
                    completed=msg.content.percent_complete * 100.0,
                )
            elif isinstance(msg, FileProgressStartMessage):
                progress.console.print(msg.content.message)
            elif isinstance(msg, FileProgressEndMessage) and got_file_update_msg:
                complete_message = msg.content.message

    if error_message:
        rprint(f"[red]{error_message}[/red]")
    elif complete_message:
        rprint(f"[green]{complete_message}[/green]")

    if not got_file_update_msg:
        if expect_single_file:
            rprint(f"[red]{path} not found[/red]")
        else:
            rprint(f"[red]No files found in dataset '{path.rstrip('/')}'[/red]")


@push.command(name="datasets", cls=NTBLCommand)
@click.argument("path", nargs=-1)
@click.pass_obj
def datasets_push(obj: ContextObject, path: List[str]):
    """Push dataset files to the remote store

    PATH is the path of the dataset to push (e.g. My first dataset/data.csv, My first dataset).
    """
    path = " ".join(path)
    if "/" not in path:
        # The user is trying to push the whole dataset
        path = f"{path}/"

    with obj.planar_ally.dataset_fs().push(path) as stream:
        process_file_update_stream(path, stream)


@pull.command(
    name="project", help="Pull the project file changes from the remote store", cls=NTBLCommand
)
@click.pass_obj
def project_pull(obj: ContextObject):
    resp = obj.planar_ally.fs(FileKind.project).pull("")
    return SuccessfulUserMessageOutput(response=resp)


@pull.command(name="datasets", cls=NTBLCommand)
@click.argument("path", nargs=-1)
@click.pass_obj
def datasets_pull(obj: ContextObject, path: List[str]):
    """Push dataset files to the remote store

    PATH is the path of the dataset to pull (e.g. My first dataset/data.csv, My first dataset).
    """
    path = " ".join(path)
    if "/" not in path:
        # The user is trying to push the whole dataset
        path = f"{path}/"

    with obj.planar_ally.dataset_fs().pull(path) as stream:
        process_file_update_stream(path, stream)


class DiffOutput(OutputModel):
    diff: GitDiff

    def get_human_readable_output(self) -> Iterable[Any]:
        return [Syntax(self.diff.raw, "diff", theme="ansi_light")]


@diff.command(name="project", help="Show the full file changes for project files", cls=NTBLCommand)
@click.pass_obj
def project_diff(obj: ContextObject):
    return DiffOutput(diff=obj.git.diff())
