from typing import Dict

from rich import print as rprint
from rich.progress import Progress, TaskID


from ..planar_ally_client.types import (
    FileProgressEndMessage,
    FileProgressStartMessage,
    FileProgressUpdateMessage,
    StreamErrorMessage,
)


from noteable.planar_ally_client.api import DatasetOperationStream, PlanarAllyAPI


def pull_dataset_verbosely(dataset_name: str, timeout_seconds: float = 60.0) -> str:
    """Download (or re-download) an entire Noteable Dataset, a subset, or even a single file in the dataset
    from permanent storage into the notebook's kernel session's filesystem.

    Displays per-file progress using Rich as cell output. If there was an error during the download,
    DatasetOperationException will be raised.
    """

    path = _clean_dataset_name(dataset_name)

    client = PlanarAllyAPI(default_total_timeout_seconds=timeout_seconds)

    with client.dataset_fs().pull(path) as stream:
        rich_process_file_update_stream(path, stream)


def push_dataset_verbosely(dataset_name: str, timeout_seconds: float = 60.0) -> str:
    """Upload (or re-upload) an entier Noteable Dataset, a subset, or even a single file in the dataset
    from the notebook's kernel session's filesystem to permanent storage.

    Displays per-file progress using Rich as cell output. If there was an error during the upload,
    DatasetOperationException will be raised.
    """

    path = _clean_dataset_name(dataset_name)

    client = PlanarAllyAPI(default_total_timeout_seconds=timeout_seconds)

    with client.dataset_fs().push(path) as stream:
        rich_process_file_update_stream(path, stream)


def _clean_dataset_name(dataset_name: str) -> str:
    """Append trailing slash if none are found"""
    if "/" not in dataset_name:
        # The user is trying to pull the whole dataset, otherwise may want some subset.
        dataset_name = f"{dataset_name}/"

    return dataset_name


class DatasetOperationException(Exception):
    pass


def rich_process_file_update_stream(path: str, stream: DatasetOperationStream):
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
        raise DatasetOperationException(error_message)

    if complete_message:
        rprint(f"[green]{complete_message}[/green]")
    if not got_file_update_msg:
        if expect_single_file:
            rprint(f"[red]{path} not found[/red]")
        else:
            rprint(f"[red]No files found in dataset '{path.rstrip('/')}'[/red]")
