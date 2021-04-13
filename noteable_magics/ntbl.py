import os
from dataclasses import dataclass
from pathlib import PurePath
from typing import Any, Iterable, Optional

import click
from click.exceptions import Abort, Exit
from IPython.core.magic import Magics, line_cell_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments
from rich import print as rprint
from rich.syntax import Syntax
from rich.table import Table
from traitlets import Int, Unicode
from traitlets.config import Configurable

from .command import NTBLCommand, OutputModel
from .git_service import GitDiff, GitService, GitStatus
from .s3_sidecar_service import (
    PullResultResponse,
    RemoteStatusResponse,
    S3SidecarService,
    SyncResultResponse,
)


@dataclass(frozen=True)
class ContextObject:
    s3_sidecar: S3SidecarService
    git: GitService
    magic: "NTBLMagic"
    enable_project_push: bool = False


@magics_class
class NTBLMagic(Magics, Configurable):
    redis_dsn = Unicode(
        "redis://:123@0.0.0.0:6379",
        config=True,
        help="The Redis DSN to connect to for publishing messages",
    )
    redis_channel_name = Unicode(
        "s3", config=True, help="The Redis channel name for publishing messages"
    )
    project_dir = Unicode("project", config=True, help="The project path, relative or absolute")
    redis_results_max_wait_time_seconds = Int(
        5, config=True, help="The maximum time to wait in seconds for redis results"
    )

    @line_cell_magic("ntbl")
    @magic_arguments()
    @argument("line", default="", nargs="*", type=str, help="Noteable magic")
    def execute(self, line="", cell=""):
        s3_sidecar_svc = S3SidecarService(
            redis_dsn=self.redis_dsn,
            channel_name=self.redis_channel_name,
            redis_results_max_wait_time_seconds=self.redis_results_max_wait_time_seconds,
        )
        git_service = GitService(self._get_full_project_path())
        ctx_obj = ContextObject(s3_sidecar_svc, git_service, self)

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
    status: RemoteStatusResponse

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
        resp = obj.s3_sidecar.request_remote_status(os.path.join(obj.magic.project_dir, ""))
        remote_status = obj.s3_sidecar.retrieve_remote_status(resp.redis_result_key)
        return ProjectRemoteStatusOutput(status=remote_status)
    return ProjectStatusOutput(status=obj.git.status())


class ProjectPushOutput(OutputModel):
    message: str = "Project files are being pushed to S3..."
    sync_result: SyncResultResponse

    def get_human_readable_output(self) -> Iterable[Any]:
        # TODO: show self.message before the sync result finishes. (generator?)
        #   this sync may take awhile depending on the number of files and their size!
        if self.sync_result.is_ok():
            result_message = f"[green]{self.sync_result.status_message}[/green]"
        else:
            result_message = f"[red]{self.sync_result.status_message}[/red]"
        return [self.message, result_message]


@push.command(
    name="project",
    help="Push the project file changes to the remote store asynchronously",
    cls=NTBLCommand,
)
@click.argument("message", type=click.STRING, required=False)
@click.pass_obj
def project_push(obj: ContextObject, message: Optional[str]):
    if not obj.enable_project_push:
        # disable project push until file reconciliation is in place
        rprint("[red]Project push is not supported yet[/red]")
        return None
    obj.git.add_and_commit_all(message)
    resp = obj.s3_sidecar.request_project_push(os.path.join(obj.magic.project_dir, ""))
    sync_result = obj.s3_sidecar.retrieve_sync_result(resp.redis_result_key)
    return ProjectPushOutput(sync_result=sync_result)


class ProjectPullOutput(OutputModel):
    message: str = "Project files are being pulled to your kernel"
    pull_result: PullResultResponse

    def get_human_readable_output(self) -> Iterable[Any]:
        # TODO: show self.message before the pull result finishes. (generator?)
        #   this pull may take awhile depending on the number of files and their size!
        if self.pull_result.is_ok():
            result_message = f"[green]{self.pull_result.status_message}[/green]"
        else:
            result_message = f"[red]{self.pull_result.status_message}[/red]"
        return [self.message, result_message]


@pull.command(
    name="project",
    help="Pull the project file changes from the remote store asynchronously",
    cls=NTBLCommand,
)
@click.pass_obj
def project_pull(obj: ContextObject):
    resp = obj.s3_sidecar.request_project_pull(os.path.join(obj.magic.project_dir, ""))
    result = obj.s3_sidecar.retrieve_pull_result(resp.redis_result_key)
    if result.is_ok():
        obj.git.add_and_commit_all("synced changes from s3")
    return ProjectPullOutput(pull_result=result)


class DiffOutput(OutputModel):
    diff: GitDiff

    def get_human_readable_output(self) -> Iterable[Any]:
        return [Syntax(self.diff.raw, "diff", theme="ansi_light")]


@diff.command(name="project", help="Show the full file changes for project files", cls=NTBLCommand)
@click.pass_obj
def project_diff(obj: ContextObject):
    return DiffOutput(diff=obj.git.diff())
