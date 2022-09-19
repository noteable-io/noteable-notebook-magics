import json
from contextlib import contextmanager

import pytest

from noteable_magics.logging import configure_logging
from noteable_magics.planar_ally_client.api import PlanarAllyAPI
from noteable_magics.planar_ally_client.types import (
    FileKind,
    FileProgressUpdateContent,
    FileProgressUpdateMessage,
)
from noteable_magics.sql.connection import Connection


@pytest.fixture(scope="session", autouse=True)
def _configure_logging():
    configure_logging(True, "INFO", "DEBUG")


@pytest.fixture()
def api():
    yield PlanarAllyAPI()


@pytest.fixture
def fs(api):
    return api.fs(FileKind.project)


@pytest.fixture
def ds(api):
    return api.dataset_fs()


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

    def iter_lines(self):
        if not isinstance(self.json_data, (bytes, str)):
            yield json.dumps(self.json_data) + "\n"
        else:
            yield self.json_data + "\n"

    @contextmanager
    def stream(self):
        yield self


@pytest.fixture()
def mock_success():
    yield MockResponse(
        {"message": "Success", 'file_changes': [{'path': 'foo/bar', 'change_type': 'added'}]}, 200
    )


@pytest.fixture()
def mock_no_content():
    return MockResponse(None, 204)


@pytest.fixture()
def mock_dataset_stream():
    return MockResponse(
        FileProgressUpdateMessage(
            content=FileProgressUpdateContent(file_name="foo/bar", percent_complete=1.0)
        ).json(),
        200,
    ).stream()


@pytest.fixture
def with_empty_connections():
    """Empty out the current set of sql magic Connections"""
    preexisting_connections = Connection.connections

    Connection.connections = {}

    yield

    Connection.connections = preexisting_connections


@pytest.fixture
def foo_database_connection(with_empty_connections):
    """Make an @foo SQLite connection to simulate a non-default bootstrapped datasource."""

    handle = '@foo'
    human_name = "My Shiny Connection"
    Connection.set("sqlite:///:memory:", displaycon=False, name=handle, human_name=human_name)

    yield handle, human_name


@pytest.fixture
def populated_foo_database(foo_database_connection):
    connection = Connection.connections['@foo']
    db = connection.session  # sic, a sqlalchemy.engine.base.Connection, not a Session. Sigh.
    db.execute('create table int_table(a int, b int, c int)')
    db.execute('insert into int_table (a, b, c) values (1, 2, 3), (4, 5, 6)')
