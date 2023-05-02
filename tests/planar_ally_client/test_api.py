from unittest import mock

import pytest
from httpx import Timeout

from noteable.planar_ally_client.api import DatasetFileSystemAPI, FileSystemAPI
from noteable.planar_ally_client.types import (
    FileKind,
    FileProgressUpdateContent,
    FileProgressUpdateMessage,
    RemoteStatus,
    UserMessage,
)


def test_post(api, mock_success):
    with mock.patch.object(api._client, 'request', return_value=mock_success) as mock_request:
        api.post('foo', "foo files", timeout=(0.5, 25.0))
        mock_request.assert_called_with(
            'POST', 'http://localhost:7000/api/v0/foo', timeout=(0.5, 25.0)
        )


def test_delete(api, mock_success):
    with mock.patch.object(api._client, 'request', return_value=mock_success) as mock_request:
        api.delete('foo', "foo files", timeout=(0.5, 25.0))
        mock_request.assert_called_with(
            'DELETE', 'http://localhost:7000/api/v0/foo', timeout=(0.5, 25.0)
        )


def test_get(api, mock_success):
    with mock.patch.object(api._client, 'request', return_value=mock_success) as mock_request:
        api.get('foo', "foo files", timeout=(0.5, 25.0))
        mock_request.assert_called_with(
            'GET', 'http://localhost:7000/api/v0/foo', timeout=(0.5, 25.0)
        )


def test_fs_fsapi(api):
    assert isinstance(api.fs(FileKind.project), FileSystemAPI)


def test_fs_dsapi(api):
    assert isinstance(api.dataset_fs(), DatasetFileSystemAPI)


def test_fs_pull(fs, mock_success):
    with mock.patch.object(fs._api._client, 'request', return_value=mock_success) as mock_request:
        result = fs.pull("foo/bar", timeout=(0.5, 25.0))
        mock_request.assert_called_with(
            'POST', 'http://localhost:7000/api/v0/fs/project/foo/bar/pull', timeout=(0.5, 25.0)
        )
        assert result == UserMessage(message='Success')


def test_fs_push(fs, mock_success):
    with mock.patch.object(fs._api._client, 'request', return_value=mock_success) as mock_request:
        result = fs.push("foo/bar", timeout=(0.5, 25.0))
        mock_request.assert_called_with(
            'POST', 'http://localhost:7000/api/v0/fs/project/foo/bar/push', timeout=(0.5, 25.0)
        )
        assert result == UserMessage(message='Success')


def test_fs_delete(fs, mock_success):
    with mock.patch.object(fs._api._client, 'request', return_value=mock_success) as mock_request:
        result = fs.delete("foo/bar", timeout=(0.5, 25.0))
        mock_request.assert_called_with(
            'DELETE', 'http://localhost:7000/api/v0/fs/project/foo/bar', timeout=(0.5, 25.0)
        )
        assert result == UserMessage(message='Success')


def test_fs_move(fs, mock_success):
    with mock.patch.object(fs._api._client, 'request', return_value=mock_success) as mock_request:
        result = fs.move("foo/bar", timeout=(0.5, 25.0))
        mock_request.assert_called_with(
            'POST', 'http://localhost:7000/api/v0/fs/project/foo/bar/move', timeout=(0.5, 25.0)
        )
        assert result == UserMessage(message='Success')


def test_fs_get_remote_status(fs, mock_success):
    with mock.patch.object(fs._api._client, 'request', return_value=mock_success) as mock_request:
        result = fs.get_remote_status("foo/bar", timeout=(0.5, 25.0))
        mock_request.assert_called_with(
            'GET', 'http://localhost:7000/api/v0/fs/project/foo/bar/status', timeout=(0.5, 25.0)
        )
        assert result == RemoteStatus(file_changes=[{'path': 'foo/bar', 'change_type': 'added'}])


def test_ds_pull(ds, mock_dataset_stream):
    with mock.patch.object(
        ds._api._client, 'stream', return_value=mock_dataset_stream
    ) as mock_request:
        with ds.pull("foo/bar") as result:
            mock_request.assert_called_with(
                'POST',
                'http://localhost:7000/api/v0/fs/dataset/foo/bar/pull',
                timeout=None,
            )
            assert list(result) == [
                FileProgressUpdateMessage(
                    content=FileProgressUpdateContent(file_name="foo/bar", percent_complete=1.0)
                )
            ]


def test_ds_push(ds, mock_dataset_stream):
    with mock.patch.object(
        ds._api._client, 'stream', return_value=mock_dataset_stream
    ) as mock_request:
        with ds.push("foo/bar") as result:
            mock_request.assert_called_with(
                'POST',
                'http://localhost:7000/api/v0/fs/dataset/foo/bar/push',
                timeout=None,
            )
            assert list(result) == [
                FileProgressUpdateMessage(
                    content=FileProgressUpdateContent(file_name="foo/bar", percent_complete=1.0)
                )
            ]


@pytest.mark.parametrize("ext_level", [None, "DEBUG"])
def test_change_log_level(api, mock_no_content, ext_level):
    with mock.patch.object(api._client, 'request', return_value=mock_no_content) as mock_request:
        api.change_log_level("DEBUG", ext_log_level=ext_level)
        mock_request.assert_called_with(
            'POST',
            'http://localhost:7000/instance/logs',
            json={'new_app_level': 'DEBUG', 'new_ext_level': ext_level, 'rtu_log_level': None},
            timeout=Timeout(connect=0.5, read=60.0, write=60.0, pool=60.0),
        )
