import json
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple
from uuid import uuid4

import pytest
from IPython.core.interactiveshell import InteractiveShell
from managed_service_fixtures import CockroachDetails
from sqlalchemy.orm import Session

from noteable_magics.logging import configure_logging, RawLogCapture
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


@pytest.fixture
def log_capture():
    """Reset logs and enable log capture for a test.

    Returns a context manager which returns the list of logged structlog dicts"""

    @contextmanager
    def _log_capture():
        logcap = RawLogCapture()
        configure_logging(True, "INFO", "DEBUG", log_capture=logcap)
        yield logcap.entries

    yield _log_capture

    # Put back way it was as from _configure_logging auto-fixture.
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
    return mocker.patch("noteable_magics.sql.meta_commands.display")


def populate_database(connection: Connection, include_comments=False):

    # Must actually do the table building transactionally, especially adding comments, else
    # subsequently introspecting CRDB schema will block indefinitely.
    with Session(connection._engine) as db:
        db.execute(
            'create table int_table(a int primary key not null, b int not null default 12, c int not null default 42)'
        )

        db.execute('create unique index int_table_whole_row_idx on int_table(a,b,c)')
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


@dataclass
class DatasourceJSONs:
    meta_dict: Dict[str, Any]
    dsn_dict: Optional[Dict[str, str]] = None
    connect_args_dict: Optional[Dict[str, any]] = None

    @property
    def meta_json(self) -> str:
        return json.dumps(self.meta_dict)

    @property
    def dsn_json(self) -> Optional[str]:
        if self.dsn_dict:
            return json.dumps(self.dsn_dict)

    @property
    def connect_args_json(self) -> Optional[str]:
        if self.connect_args_dict:
            return json.dumps(self.connect_args_dict)

    def json_to_tmpdir(self, datasource_id: str, tmpdir: Path):
        """Save our json strings to a tmpdir so can be used to test
        bootstrap_datasource_from_files or bootstrap_datasources
        """

        json_str_and_paths = [
            (self.meta_json, tmpdir / f'{datasource_id}.meta_js'),
            (self.dsn_json, tmpdir / f'{datasource_id}.dsn_js'),
            (self.connect_args_json, tmpdir / f'{datasource_id}.ca_js'),
        ]

        for json_str, path in json_str_and_paths:
            if json_str:
                path.write_text(json_str)


@pytest.fixture
def datasource_id_factory() -> Callable[[], str]:
    def factory_datasource_id():
        return uuid4().hex

    return factory_datasource_id


@pytest.fixture
def datasource_id(datasource_id_factory) -> str:
    return datasource_id_factory()
