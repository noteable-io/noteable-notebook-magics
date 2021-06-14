import os
from dataclasses import dataclass
from pathlib import PurePath
from typing import Any, Iterable

import click
from click.exceptions import Abort, Exit
from IPython.core.magic import Magics, line_cell_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments
from rich import print as rprint
from rich.syntax import Syntax
from rich.table import Table
from traitlets import Bool, Float, Unicode
from traitlets.config import Configurable

from .command import NTBLCommand, OutputModel
from .git_service import GitDiff, GitService, GitStatus, GitUser
from .planar_ally_client.api import PlanarAllyAPI
from .planar_ally_client.errors import PlanarAllyError
from .planar_ally_client.types import FileKind, RemoteStatus, UserMessage


@dataclass(frozen=True)
class ContextObject:
    planar_ally: PlanarAllyAPI
    git: GitService
    magic: "NTBLMagic"
    enable_project_push: bool


@magics_class
class NTBLMagic(Magics, Configurable):
    planar_ally_api_url = Unicode(
        "http://localhost:7000/api", config=True, help="The URL to connect to for planar-ally"
    )
    planar_ally_timeout_seconds = Float(
        60.0, config=True, help="The total timeout seconds when making a request to planar-ally"
    )
    project_dir = Unicode("project", config=True, help="The project path, relative or absolute")

    git_user_name = Unicode(
        "Noteable Kernel", config=True, help="The name of the user creating git commits"
    )
    git_user_email = Unicode(
        "engineering@noteable.io", config=True, help="The email of the user creating git commits"
    )

    enable_project_push = Bool(True, config=True, help="Allow pushing project files to S3")

    @line_cell_magic("ntbl")
    @magic_arguments()
    @argument("line", default="", nargs="*", type=str, help="Noteable magic")
    def execute(self, line="", cell=""):
        planar_ally = PlanarAllyAPI(
            self.planar_ally_api_url, total_timeout_seconds=self.planar_ally_timeout_seconds
        )
        git_service = GitService(
            self._get_full_project_path(),
            GitUser(name=self.git_user_name, email=self.git_user_email),
        )
        ctx_obj = ContextObject(
            planar_ally, git_service, magic=self, enable_project_push=self.enable_project_push
        )

        try:
            with ntbl_magic.make_context(
                info_name="%ntbl",
                args=[*line.split(), *cell.split()],
                obj=ctx_obj,
            ) as ctx:
                return ntbl_magic.invoke(ctx)
        except Exit as ex:
            if ex.exit_code != 0:
                raise ex
        except Abort:
            rprint("[red]Aborted[/red]")
        except PlanarAllyError as e:
            rprint(f"[red]{e}[/red]")

        return None

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


class ProjectPushOutput(OutputModel):
    response: UserMessage

    def get_human_readable_output(self) -> Iterable[Any]:
        # TODO: show self.message before the sync result finishes. (generator?)
        #   this sync may take awhile depending on the number of files and their size!
        return [f"[green]{self.response.message}[/green]"]


@push.command(
    name="project",
    help="Push the project file changes to the remote store asynchronously",
    cls=NTBLCommand,
)
@click.pass_obj
def project_push(obj: ContextObject):
    if not obj.enable_project_push:
        # disable project push until file reconciliation is in place
        rprint("[red]Project push is not supported yet[/red]")
        return None
    resp = obj.planar_ally.fs(FileKind.project).push("")
    return ProjectPushOutput(response=resp)


class ProjectPullOutput(OutputModel):
    response: UserMessage

    def get_human_readable_output(self) -> Iterable[Any]:
        # TODO: show self.message before the sync result finishes. (generator?)
        #   this sync may take awhile depending on the number of files and their size!
        return [f"[green]{self.response.message}[/green]"]


@pull.command(
    name="project",
    help="Pull the project file changes from the remote store asynchronously",
    cls=NTBLCommand,
)
@click.pass_obj
def project_pull(obj: ContextObject):
    resp = obj.planar_ally.fs(FileKind.project).pull("")
    return ProjectPullOutput(response=resp)


class DiffOutput(OutputModel):
    diff: GitDiff

    def get_human_readable_output(self) -> Iterable[Any]:
        return [Syntax(self.diff.raw, "diff", theme="ansi_light")]


@diff.command(name="project", help="Show the full file changes for project files", cls=NTBLCommand)
@click.pass_obj
def project_diff(obj: ContextObject):
    return DiffOutput(diff=obj.git.diff())
