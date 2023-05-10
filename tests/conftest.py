import json
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple
from uuid import UUID, uuid4

import pytest
from IPython.core.interactiveshell import InteractiveShell
from managed_service_fixtures import CockroachDetails
from sqlalchemy import inspect
from sqlalchemy.orm import Session

from noteable.logging import RawLogCapture, configure_logging
from noteable.planar_ally_client.api import PlanarAllyAPI
from noteable.planar_ally_client.types import (
    FileKind,
    FileProgressUpdateContent,
    FileProgressUpdateMessage,
)
from noteable.sql.connection import Connection, bootstrap_duckdb
from noteable.sql.magic import SqlMagic
from noteable.sql.run import add_commit_blacklist_dialect

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
def with_duckdb_bootstrapped(with_empty_connections) -> None:
    # Normal magics bootstrapping will leave us with DuckDB connection populated.
    bootstrap_duckdb()

    yield


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
    return mocker.patch("noteable.sql.meta_commands.display")


KNOWN_TABLES_AND_KINDS = [
    ('int_table', 'table'),
    ('str_table', 'table'),
    ('references_int_table', 'table'),
    ('str_int_view', 'view'),
]
KNOWN_TABLES = set(tk[0] for tk in KNOWN_TABLES_AND_KINDS)

"""The table / view names in default schema that populate_database() will create. See cleanup_any_extra_tables()"""


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
            """create table str_table(
                str_id text not null default 'f'
                    constraint single_char_str_id check (length(str_id) = 1),
                int_col int default 22
                    constraint only_even_int_col_values check (int_col % 2 = 0),
                constraint never_f_10 check (not (str_id = 'f' and int_col = 10))
             )
            """
        )
        db.execute(
            "insert into str_table(str_id, int_col) values ('a', 2), ('b', 2), ('c', 4), ('d', null)"
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


def cleanup_any_extra_tables(connection: Connection):
    """Remove any tables in default schema that aren't what populate_database above creates"""

    inspector = inspect(connection._engine)

    relations = set(inspector.get_table_names())
    relations.update(inspector.get_view_names())

    unexpected_relations = relations - KNOWN_TABLES

    # CRDB needs 'cascade' to be able to drop tables referenced with FKs. SQLite does
    # not recognize, however.
    maybe_cascade = 'cascade' if 'cockroach' in str(connection._engine) else ''

    for unexpected_relation in unexpected_relations:
        try:
            with Session(connection._engine) as db:
                db.execute(f'drop table {unexpected_relation} {maybe_cascade}')
                db.commit()
        except Exception:
            # Maybe it was a view?
            with Session(connection._engine) as db:
                db.execute(f'drop view {unexpected_relation}')
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

    yield

    cleanup_any_extra_tables(connection)


# For tests talking to a live cockroachdb

# Need to have its SQL cell handle derived from a UUID for the \introspect tests,
# but also want it to be visually distinguished in the tests so we can tell is
# the cockroach db being parameterized over, esp. when listing out failed tests.
#
# In reality, all of the real and non-legacy DuckDB and/or BigQuery datasources
# bootstrapped into noteable kernels will have their 'sql handles' be based off of
# the hex of their datasource UUID primary key value. But the only portion of our
# codebase over here in kernel magic land that needs to know that these handles
# are most of the time structured in this fashion is when reporting discovered table
# structures back to Gate with the SQL cell meta command \introspect.
#
# (This is the closest spelling to 'cockroach' I'm going to bother making in hex)
#
COCKROACH_UUID = UUID('cccccccc-0000-cccc-0000-cccccccccccc')
COCKROACH_HANDLE = f"@{COCKROACH_UUID.hex}"


@pytest.fixture(scope='session')
def cockroach_database_connection(managed_cockroach: CockroachDetails) -> Tuple[str, str]:
    # CRDB uses psycopg2 driver. Install the extension that makes control-c work
    # and be able to interrupt statements.
    from noteable.datasource_postprocessing import _install_psycopg2_interrupt_fix

    _install_psycopg2_interrupt_fix()

    # CRDB will by default be in autocommit mode, so must prevent trying to double-commit.
    add_commit_blacklist_dialect('cockroachdb')

    human_name = "My Cockroach Connection"
    Connection.set(managed_cockroach.sync_dsn, name=COCKROACH_HANDLE, human_name=human_name)
    return COCKROACH_HANDLE, human_name


@pytest.fixture(scope='session')
def bad_port_number_cockroach_connection(
    managed_cockroach: CockroachDetails,
) -> Tuple[UUID, str]:
    """Broken cockroach configuration with a bad port number. Won't ever be able to connect.

    Returns the UUID + handle pair.
    """

    BAD_PORT_COCKROACH_UUID = UUID('badccccc-0000-cccc-0000-cccccccccccc')
    BAD_COCKROACH_HANDLE = f"@{BAD_PORT_COCKROACH_UUID.hex}"

    as_dict = managed_cockroach.dict()
    as_dict['sql_port'] = 999  # definitely wrong port.

    bad_cockroach_details = CockroachDetails(**as_dict)

    human_name = "Bad Port Number Cockroach"
    Connection.set(bad_cockroach_details.sync_dsn, name=BAD_COCKROACH_HANDLE, human_name=human_name)

    return (BAD_PORT_COCKROACH_UUID, BAD_COCKROACH_HANDLE)


@pytest.fixture(scope='session')
def session_populated_cockroach_database(cockroach_database_connection: Tuple[str, str]) -> None:
    handle, _ = cockroach_database_connection
    connection = Connection.connections[handle]
    populate_database(connection, include_comments=True)


@pytest.fixture
def populated_cockroach_database(
    session_populated_cockroach_database, cockroach_database_connection: Tuple[str, str]
) -> None:
    """Function-scoped version of session_populated_cockroach_database, cleans up any newly created tables
    after each function.
    """

    yield

    handle, _ = cockroach_database_connection
    connection = Connection.connections[handle]

    cleanup_any_extra_tables(connection)


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
