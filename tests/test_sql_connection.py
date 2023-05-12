import pytest
import sqlalchemy.engine.base
from sqlalchemy.engine import Engine

from noteable.sql import get_sqla_connection, get_sqla_engine
from noteable.sql.connection import (
    LOCAL_DB_CONN_HANDLE,
    LOCAL_DB_CONN_NAME,
    Connection,
    bootstrap_duckdb,
    get_db_connection,
)

UNKNOWN_CONN_HANDLE = "@456567567343456567"

# For parameterization when fetching connection / connection artifacts by either handle or name.
DUCKDB_HANDLE_AND_NAME = [LOCAL_DB_CONN_HANDLE, LOCAL_DB_CONN_NAME]


class TestToplevelFunctions:
    @pytest.mark.parametrize('handle_or_name', DUCKDB_HANDLE_AND_NAME)
    @pytest.mark.usefixtures("with_empty_connections")
    def test_bootstrap_duckdb_and_get_db_connection(self, handle_or_name):
        assert len(Connection.connections) == 0

        conn = get_db_connection(handle_or_name)
        assert conn is None

        bootstrap_duckdb()
        conn = get_db_connection(handle_or_name)

        # sic, the 'name' attr is what we call the handle.
        assert conn.name == LOCAL_DB_CONN_HANDLE
        assert conn.human_name == LOCAL_DB_CONN_NAME
        assert str(conn._engine.url) == "duckdb:///:memory:"

        assert len(Connection.connections) == 1

        # Call it again, should return same thing.
        conn2 = get_db_connection(handle_or_name)

        assert conn2 is conn
        assert len(Connection.connections) == 1

    def test_get_db_connection_returns_none_on_non_local_db_handle_miss(self):
        assert get_db_connection(UNKNOWN_CONN_HANDLE) is None

    @pytest.mark.parametrize('handle_or_name', DUCKDB_HANDLE_AND_NAME)
    @pytest.mark.usefixtures('with_duckdb_bootstrapped')
    def test_get_sqla_connection_hit(self, handle_or_name: str):
        sqla_conn = get_sqla_connection(handle_or_name)
        assert isinstance(sqla_conn, sqlalchemy.engine.base.Connection)

    def test_get_sqla_connection_miss(self):
        assert get_sqla_connection(UNKNOWN_CONN_HANDLE) is None

    @pytest.mark.parametrize('handle_or_name', DUCKDB_HANDLE_AND_NAME)
    @pytest.mark.usefixtures('with_duckdb_bootstrapped')
    def test_get_sqla_engine(self, handle_or_name: str):
        engine = get_sqla_engine(handle_or_name)
        assert isinstance(engine, Engine)

    def test_get_sqla_engine_miss(self):
        assert get_sqla_connection(UNKNOWN_CONN_HANDLE) is None


class TestMiscConnectionMethods:
    @pytest.mark.usefixtures("with_empty_connections")
    def test_reset_connection_pool(self):
        bootstrap_duckdb()
        conn = get_db_connection(LOCAL_DB_CONN_HANDLE)

        sqla_conn = conn.sqla_connection

        assert isinstance(sqla_conn, sqlalchemy.engine.base.Connection)
        assert conn._sqla_connection is sqla_conn

        conn.reset_connection_pool()

        assert conn._sqla_connection is None

        # But Lo, he is risen.
        sqla_conn = conn.sqla_connection

        assert isinstance(sqla_conn, sqlalchemy.engine.base.Connection)
