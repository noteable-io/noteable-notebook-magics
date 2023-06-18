from uuid import uuid4

import pytest


from noteable.sql.connection import (
    Connection,
    ConnectionRegistry,
    get_connection_registry,
    get_db_connection,
    get_sqla_connection,
    get_sqla_engine,
    bootstrap_duckdb,
)


class TestConnection:
    def test_sql_cell_handles_must_start_with_at(self):
        with pytest.raises(ValueError, match="sql_cell_handle values must start with '@'"):
            Connection('no_leading_at', 'sdf', 'sqlite:///:memory:')

    def test_must_have_human_name(self):
        with pytest.raises(ValueError, match="Connections must have a human-assigned name"):
            Connection('@foo', '', 'sqlite:///:memory:')


class TestConnectionRegistry:
    def test_hates_null_connection(self):
        registry = get_connection_registry()
        with pytest.raises(ValueError, match='must be a Connection instance'):
            registry.register(None)

    def test_hates_double_registration(self, sqlite_database_connection):
        # The sqlite_database_connection fixture will have already registered it.
        registry = get_connection_registry()

        handle, human_name = sqlite_database_connection

        with pytest.raises(ValueError, match='is already registered'):
            registry.register(Connection(handle, human_name, 'sqlite:///:memory:'))

    def tests_hates_double_reported_bootstrapping_failures(self, sqlite_database_connection):
        registry = get_connection_registry()

        handle, human_name = sqlite_database_connection

        with pytest.raises(
            ValueError, match='s already defined, but now reporting bootstrapping failure'
        ):
            registry.add_bootstrapping_failure(
                handle, human_name, 'Whacktastical late error reporting, Batman!'
            )
