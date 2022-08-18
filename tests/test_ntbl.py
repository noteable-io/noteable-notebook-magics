import traceback
from unittest import mock

import pytest
from click.testing import CliRunner

from noteable_magics.ntbl import NTBLMagic, change_log_level, datasets_pull, datasets_push
from noteable_magics.planar_ally_client.api import DatasetOperationStream
from noteable_magics.planar_ally_client.types import (
    FileProgressUpdateContent,
    FileProgressUpdateMessage,
)
from tests.conftest import MockResponse


@pytest.fixture()
def runner():
    yield CliRunner()


@pytest.fixture()
def context():
    yield NTBLMagic()._build_ctx()


@pytest.mark.parametrize(
    'input_path,expected_path',
    [
        ('foobar', 'foobar/'),
        ('foo bar', 'foo bar/'),
        ('foo bar/baz bak', 'foo bar/baz bak'),
        ('foo/bar', 'foo/bar'),
        ('foo/bar/', 'foo/bar/'),
        ('', '/'),
    ],
)
def test_datasets_push(input_path, expected_path, runner, context):
    response = MockResponse(
        FileProgressUpdateMessage(
            content=FileProgressUpdateContent(file_name="foo/bar", percent_complete=1.0)
        ).json(),
        200,
    )

    with mock.patch(
        "noteable_magics.planar_ally_client.api.DatasetFileSystemAPI.push",
        return_value=DatasetOperationStream(response.stream(), "push files"),
    ) as push_mock:
        result = runner.invoke(datasets_push, input_path.split(' '), obj=context)
        assert result.exit_code == 0, ''.join(traceback.format_exception(*result.exc_info))
        push_mock.assert_called_with(expected_path)


@pytest.mark.parametrize(
    'input_path,expected_path',
    [
        ('foobar', 'foobar/'),
        ('foo bar', 'foo bar/'),
        ('foo bar/baz bak', 'foo bar/baz bak'),
        ('foo/bar', 'foo/bar'),
        ('foo/bar/', 'foo/bar/'),
        ('', '/'),
    ],
)
def test_datasets_pull(input_path, expected_path, runner, context):
    response = MockResponse(
        FileProgressUpdateMessage(
            content=FileProgressUpdateContent(file_name="foo/bar", percent_complete=1.0)
        ).json(),
        200,
    )

    with mock.patch(
        "noteable_magics.planar_ally_client.api.DatasetFileSystemAPI.pull",
        return_value=DatasetOperationStream(response.stream(), "pull files"),
    ) as pull_mock:
        result = runner.invoke(datasets_pull, input_path.split(' '), obj=context)
        assert result.exit_code == 0, ''.join(traceback.format_exception(*result.exc_info))
        pull_mock.assert_called_with(expected_path)


def test_change_log_level(runner, context):
    with mock.patch(
        "noteable_magics.planar_ally_client.api.PlanarAllyAPI.change_log_level", return_value=None
    ) as change_mock:
        result = runner.invoke(change_log_level, ["--app-level", "DEBUG"], obj=context)
        assert result.exit_code == 0, ''.join(traceback.format_exception(*result.exc_info))
        change_mock.assert_called_with(
            app_log_level="DEBUG", ext_log_level=None, rtu_log_level=None
        )
