import json
from contextlib import contextmanager
from typing import Tuple

import pytest
from IPython.core.interactiveshell import InteractiveShell
from managed_service_fixtures import CockroachDetails
from sqlalchemy.orm import Session

from noteable_magics.logging import configure_logging
from noteable_magics.planar_ally_client.api import PlanarAllyAPI
from noteable_magics.planar_ally_client.types import (
    FileKind,
    FileProgressUpdateContent,
    FileProgressUpdateMessage,
)
from noteable_magics.sql.connection import Connection
from noteable_magics.sql.magic import SqlMagic

# managed_service_fixtures plugin for a live cockroachdb
pytest_plugins = 'managed_service_fixtures'


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
def with_empty_connections() -> None:
    """Empty out the current set of sql magic Connections"""
    preexisting_connections = Connection.connections

    Connection.connections = {}

    yield

    Connection.connections = preexisting_connections


@pytest.fixture
def ipython_shell() -> InteractiveShell:
    return InteractiveShell()


@pytest.fixture
def sql_magic(ipython_shell) -> SqlMagic:
    magic = SqlMagic(ipython_shell)
    # As would be done when we normally bootstrap things ...
    magic.autopandas = True

    return magic


@pytest.fixture
def ipython_namespace(ipython_shell):
    return ipython_shell.user_ns


@pytest.fixture
def mock_display(mocker):
    mock = mocker.Mock()
    mocker.patch("noteable_magics.sql.meta_commands.display", mock)
    return mock


def populate_database(connection: Connection, include_comments=False):

    # Must actually do the table building transactionally, especially adding comments, else
    # subsequently introspecting CRDB schema will block indefinitely.
    with Session(connection._engine) as db:
        db.execute(
            'create table int_table(a int primary key not null, b int not null default 12, c int not null default 42)'
        )
        db.execute('insert into int_table (a, b, c) values (1, 2, 3), (4, 5, 6)')

        db.execute(
            "create table str_table(str_id text not null default 'foonly', int_col int default 22)"
        )
        db.execute(
            "insert into str_table(str_id, int_col) values ('a', 1), ('b', 2), ('c', 3), ('d', null)"
        )

        db.execute(
            '''create table references_int_table (
            ref_id int primary key not null,
            a_id int not null references int_table(a)
        )'''
        )

        # Make a view!
        # Will only project a single row, ('a', 1, 2, 3)
        db.execute(
            '''create view str_int_view
                        as select
                            s.str_id, s.int_col,
                            i.b, i.c
                        from str_table s
                            join int_table i on (s.int_col = i.a)
                    '''
        )

        if include_comments:
            db.execute('''comment on table int_table is 'This is table comment';''')
            db.execute('''comment on column int_table.a is 'This is column comment';''')

        db.commit()


@pytest.fixture
def sqlite_database_connection() -> Tuple[str, str]:
    """Make an @sqlite SQLite connection to simulate a non-default bootstrapped datasource."""

    handle = '@sqlite'
    human_name = "My Sqlite Connection"
    Connection.set("sqlite:///:memory:", name=handle, human_name=human_name)

    return handle, human_name


@pytest.fixture
def populated_sqlite_database(sqlite_database_connection: Tuple[str, str]) -> None:
    handle, _ = sqlite_database_connection
    connection = Connection.connections[handle]
    populate_database(connection)


# For tests talking to a live cockroachdb
@pytest.fixture(scope='session')
def cockroach_database_connection(managed_cockroach: CockroachDetails) -> Tuple[str, str]:

    # CRDB uses psycopg2 driver. Install the extension that makes control-c work
    # and be able to interrupt statements.
    from noteable_magics.datasource_postprocessing import _install_psycopg2_interrupt_fix

    _install_psycopg2_interrupt_fix()

    handle = '@cockroach'
    human_name = "My Cockroach Connection"
    Connection.set(managed_cockroach.sync_dsn, name=handle, human_name=human_name)
    return handle, human_name


@pytest.fixture(scope='session')
def populated_cockroach_database(cockroach_database_connection: Tuple[str, str]) -> None:
    handle, _ = cockroach_database_connection
    connection = Connection.connections[handle]
    populate_database(connection, include_comments=True)
