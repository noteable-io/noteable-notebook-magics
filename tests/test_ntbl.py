import traceback
from unittest import mock

import pytest
from click.testing import CliRunner

from noteable_magics.ntbl import NTBLMagic, datasets_pull, datasets_push
from noteable_magics.planar_ally_client.types import UserMessage


@pytest.fixture()
def runner():
    yield CliRunner()


@pytest.fixture()
def context():
    with mock.patch('noteable_magics.ntbl.GitService'):
        yield NTBLMagic()._build_ctx()


@mock.patch(
    'noteable_magics.planar_ally_client.api.DatasetFileSystemAPI.push',
    return_value=UserMessage(message='Success'),
)
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
def test_datasets_push(push_mock, input_path, expected_path, runner, context):
    result = runner.invoke(datasets_push, input_path.split(' '), obj=context)
    assert result.exit_code == 0, ''.join(traceback.format_exception(*result.exc_info))
    push_mock.assert_called_with(expected_path, timeout=(0.5, 3600.0))


@mock.patch(
    'noteable_magics.planar_ally_client.api.DatasetFileSystemAPI.push',
    return_value=UserMessage(message='Success'),
)
@pytest.mark.parametrize(
    'input_path,expected_path,timeout',
    [
        ('foobar', 'foobar/', 125.0),
        ('foo bar', 'foo bar/', 125.0),
        ('foo bar/baz bak', 'foo bar/baz bak', 125.0),
        ('foo/bar', 'foo/bar', 125.0),
        ('foo/bar/', 'foo/bar/', 125.0),
        ('', '/', 125.0),
    ],
)
def test_datasets_push_with_timeout(push_mock, input_path, expected_path, timeout, runner, context):
    result = runner.invoke(datasets_push, ['-t', timeout] + input_path.split(' '), obj=context)
    assert result.exit_code == 0, ''.join(traceback.format_exception(*result.exc_info))
    push_mock.assert_called_with(expected_path, timeout=(0.5, timeout))


@mock.patch(
    'noteable_magics.planar_ally_client.api.DatasetFileSystemAPI.pull',
    return_value=UserMessage(message='Success'),
)
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
def test_datasets_pull(push_mock, input_path, expected_path, runner, context):
    result = runner.invoke(datasets_pull, input_path.split(' '), obj=context)
    assert result.exit_code == 0, ''.join(traceback.format_exception(*result.exc_info))
    push_mock.assert_called_with(expected_path, timeout=(0.5, 3600.0))


@mock.patch(
    'noteable_magics.planar_ally_client.api.DatasetFileSystemAPI.pull',
    return_value=UserMessage(message='Success'),
)
@pytest.mark.parametrize(
    'input_path,expected_path,timeout',
    [
        ('foobar', 'foobar/', 125.0),
        ('foo bar', 'foo bar/', 125.0),
        ('foo bar/baz bak', 'foo bar/baz bak', 125.0),
        ('foo/bar', 'foo/bar', 125.0),
        ('foo/bar/', 'foo/bar/', 125.0),
        ('', '/', 125.0),
    ],
)
def test_datasets_pull_with_timeout(push_mock, input_path, expected_path, timeout, runner, context):
    result = runner.invoke(datasets_pull, ['-t', timeout] + input_path.split(' '), obj=context)
    assert result.exit_code == 0, ''.join(traceback.format_exception(*result.exc_info))
    push_mock.assert_called_with(expected_path, timeout=(0.5, timeout))
