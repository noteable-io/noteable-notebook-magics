""" Tests over the data loading magic, "create_or_replace_data_view" """

from pathlib import Path

import pytest
from sqlalchemy import text

from noteable.data_loader import NoteableDataLoaderMagic
from noteable.sql.connection import UnknownConnectionError, get_connection_registry, get_sqla_engine


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    the_file = tmp_path / 'test.csv'
    with the_file.open('w') as outfile:
        outfile.write('a,b,c\n')
        outfile.write('1,2,3\n')
        outfile.write('4,5,6\n')

    return the_file


@pytest.fixture
def data_loader() -> NoteableDataLoaderMagic:
    return NoteableDataLoaderMagic()


class TestDataLoaderMagic:
    @pytest.mark.usefixtures("with_duckdb_bootstrapped")
    def test_can_load_into_local_connection(self, csv_file: Path, data_loader):
        """Load CSV file into a table named 'my_table' within implied @noteable connection."""
        df = data_loader.execute(f"{csv_file} my_table")

        # By default, we return the head of the loaded dataframe.
        assert df.columns.tolist() == ['a', 'b', 'c']
        assert len(df) == 2

        # Shoulda populated into @notable duckdb
        conn = get_connection_registry().get('@noteable')
        sqla_connection = conn.sqla_connection
        with sqla_connection.begin():
            count = sqla_connection.execute(text('select count(*) from my_table')).scalar_one()
            assert count == 2  # CSV fixture populated 2 rows.

    @pytest.mark.usefixtures("with_duckdb_bootstrapped")
    def test_can_load_multiple_times_into_local_connection(self, csv_file: Path, data_loader):
        data_loader.execute(f"{csv_file} my_table")
        data_loader.execute(f"{csv_file} my_table2")

        # Shoulda populated into @notable duckdb
        conn = conn = get_connection_registry().get('@noteable')
        sqla_connection = conn.sqla_connection
        with sqla_connection.begin():
            # rowcounts better be equal between the two tables!
            assert sqla_connection.execute(
                text('select (select count(*) from my_table) = (select count(*) from my_table2)')
            ).scalar_one()

    def test_can_specify_alternate_connection_via_handle(
        self, csv_file, data_loader, sqlite_database_connection
    ):
        """Test specifying non-default connection via --connection @sql_cell_handle"""
        alternate_datasource_handle, human_name = sqlite_database_connection
        assert alternate_datasource_handle != '@noteable'
        df = data_loader.execute(f"{csv_file} the_table --connection {alternate_datasource_handle}")

        assert len(df) == 2

        conn = get_connection_registry().get(alternate_datasource_handle)
        sqla_connection = conn.sqla_connection
        with sqla_connection.begin():
            assert (
                21
                == sqla_connection.execute(
                    text('select sum(a) + sum(b) + sum(c) from the_table')
                ).scalar_one()
            )

    def test_can_specify_alternate_connection_via_human_name(
        self, csv_file, data_loader, sqlite_database_connection
    ):
        """Test specifying non-default connection via --connection 'Human given datasource name'"""
        _, human_name = sqlite_database_connection

        # human_name from fixture gots spaces in it, so must wrap in quotes.
        df = data_loader.execute(f"{csv_file} the_table --connection '{human_name}'")

        assert len(df) == 2

        engine = get_sqla_engine(human_name)
        sqla_connection = engine.connect()
        with sqla_connection.begin():
            assert (
                21
                == sqla_connection.execute(
                    text('select sum(a) + sum(b) + sum(c) from the_table')
                ).scalar_one()
            )

    @pytest.mark.usefixtures("with_empty_connections")
    def test_cannot_load_into_unknown_handle(self, csv_file, data_loader):
        with pytest.raises(
            UnknownConnectionError,
            match="Cannot find data connection. If you recently created this connection, please restart the kernel",
        ):
            data_loader.execute(f"{csv_file} the_table --connection @nonexistenthandle")

        assert len(get_connection_registry()) == 0
