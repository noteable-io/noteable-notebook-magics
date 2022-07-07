""" Tests over the data loading magic, "create_or_replace_data_view" """

import pytest
from pathlib import Path

from sql.connection import Connection
from sqlalchemy import text

from noteable_magics import get_db_connection, LOCAL_DB_CONN_HANDLE, NoteableDataLoaderMagic


@pytest.fixture
def with_empty_connections():
    """Empty out the current set of sql magic Connections"""
    preexisting_connections = Connection.connections

    Connection.connections = {}

    yield

    Connection.connections = preexisting_connections


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    the_file = tmp_path / 'test.csv'
    with the_file.open('w') as outfile:
        outfile.write('a,b,c\n')
        outfile.write('1,2,3\n')
        outfile.write('4,5,6\n')

    return the_file


@pytest.mark.usefixtures("with_empty_connections")
class TestGetDbConnection:
    def test_populate_sqlite_conn_if_needed(self):
        assert len(Connection.connections) == 0

        conn = get_db_connection(LOCAL_DB_CONN_HANDLE)

        assert conn.name == LOCAL_DB_CONN_HANDLE
        assert str(conn._engine.url) == "sqlite:////tmp/ntbl.db"

        assert len(Connection.connections) == 1

        # Call it again, should return same thing.
        conn2 = get_db_connection(LOCAL_DB_CONN_HANDLE)

        assert conn2 is conn
        assert len(Connection.connections) == 1

    def test_returns_none_on_non_local_db_handle_miss(self):
        assert get_db_connection("@456567567343456567") is None


@pytest.fixture
def data_loader() -> NoteableDataLoaderMagic:
    return NoteableDataLoaderMagic()


@pytest.fixture
def alternate_datasource_handle():
    """Empty out the current set of sql magic Connections, then make an @foo SQLite connection
    to simulate a non-default bootstrapped datasource.
    """
    preexisting_connections = Connection.connections

    Connection.connections = {}

    # We likey memory-only sqlite dbs.
    handle = '@foo'
    Connection.set("sqlite:///:memory:", displaycon=False, name=handle)

    yield handle

    Connection.connections = preexisting_connections


class TestDataLoaderMagic:
    @pytest.mark.usefixtures("with_empty_connections")
    def test_can_load_into_local_connection(self, csv_file: Path, data_loader):
        """Load CSV file into a table named 'my_table' within implied @noteable connection."""
        df = data_loader.execute(f"{csv_file} my_table")

        # By default, we return the head of the loaded dataframe.
        assert df.columns.tolist() == ['a', 'b', 'c']
        assert len(df) == 2

        # Shoulda populated into @notable sqlite
        assert len(Connection.connections) == 1
        conn = Connection.connections['@noteable']
        session = conn.session
        with session.begin():
            count = session.execute(text('select count(*) from my_table')).scalar_one()
            assert count == 2  # CSV fixture populated 2 rows.

    @pytest.mark.usefixtures("with_empty_connections")
    def test_can_load_multiple_times_into_local_connection(self, csv_file: Path, data_loader):
        data_loader.execute(f"{csv_file} my_table")
        data_loader.execute(f"{csv_file} my_table2")

        # Shoulda populated into @notable sqlite
        assert len(Connection.connections) == 1
        conn = Connection.connections['@noteable']
        session = conn.session
        with session.begin():
            # rowcounts better be equal between the two tables!
            assert session.execute(
                text('select (select count(*) from my_table) = (select count(*) from my_table2)')
            ).scalar_one()

    def test_can_specify_alternate_sql_cell_handle(
        self, csv_file, data_loader, alternate_datasource_handle
    ):
        assert alternate_datasource_handle != '@noteable'
        df = data_loader.execute(
            f"{csv_file} the_table --sql-cell-handle {alternate_datasource_handle} --datasource-name 'my shiny connection'"
        )

        assert len(df) == 2

        assert len(Connection.connections) == 1
        conn = Connection.connections[alternate_datasource_handle]
        session = conn.session
        with session.begin():
            assert (
                21
                == session.execute(
                    text('select sum(a) + sum(b) + sum(c) from the_table')
                ).scalar_one()
            )

    @pytest.mark.usefixtures("with_empty_connections")
    def test_cannot_load_into_unknown_handle(self, csv_file, data_loader, capsys):
        # Grr. The ValueError raised gets translated into stdout by the ipythonery.
        assert (
            data_loader.execute(f"{csv_file} the_table --sql-cell-handle @nonexistenthandle")
            is None
        )
        # But we can still get at the message we expect.
        captured = capsys.readouterr()
        assert captured.out.startswith(
            "Could not find datasource identified by '@nonexistenthandle'"
        )

        assert len(Connection.connections) == 0
