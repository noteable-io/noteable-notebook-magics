import pytest
import sqlalchemy.engine.base

from noteable.sql.connection import (
    Connection,
    UnknownConnectionError,
    get_connection_registry,
    get_noteable_connection,
    get_sqla_connection,
    get_sqla_engine,
)


class TestConnection:
    def test_sql_cell_handles_must_start_with_at(self):
        with pytest.raises(ValueError, match="sql_cell_handle values must start with '@'"):
            Connection('no_leading_at', 'sdf', 'sqlite:///:memory:')

    def test_must_have_human_name(self):
        with pytest.raises(ValueError, match="Connections must have a human-assigned name"):
            Connection('@foo', '', 'sqlite:///:memory:')


class TestConnectionRegistry:
    def test_can_find_by_either_sql_cell_handle_or_human_name(self, sqlite_database_connection):
        registry = get_connection_registry()

        handle, human_name = sqlite_database_connection

        assert registry.get(handle) == registry.get(human_name) and isinstance(
            registry.get(handle), Connection
        )

    def test_register_datasource_bootstrapper_hates_malformed_cell_handle(self):
        with pytest.raises(ValueError, match='sql_cell_handle must be provided and start with "@"'):
            get_connection_registry().register_datasource_bootstrapper('bad', 'foo', lambda: None)

    def test_register_datasource_bootstrapper_hates_bootstrapper_not_callable(self):
        with pytest.raises(
            TypeError, match='Data connection bootstrapper functions must be zero-arg callables'
        ):
            get_connection_registry().register_datasource_bootstrapper('@bad', 'foo', None)

    def test_registry_hates_if_bootstrapper_returns_non_connection(self):
        registry = get_connection_registry()

        registry.register_datasource_bootstrapper('@123', '123', lambda: None)
        with pytest.raises(TypeError, match='returned something other than a Connection instance'):
            registry.get('@123')

    def test_register_hates_null_connection(self):
        registry = get_connection_registry()
        with pytest.raises(ValueError, match='must be a Connection instance'):
            registry._register(None)

    def test_register_hates_double_registration(self, sqlite_database_connection):
        # The sqlite_database_connection fixture will have already registered it.
        registry = get_connection_registry()

        handle, human_name = sqlite_database_connection

        # Force the already queued-for-bootstrapping mapping to make it all the way ...
        registry.get(handle)

        # Will first complain about the handle collision
        with pytest.raises(ValueError, match=f'with handle {handle} is already registered'):
            registry._register(Connection(handle, human_name, 'sqlite:///:memory:'))

        with pytest.raises(ValueError, match=f'with human name {human_name} is already registered'):
            registry._register(Connection(handle + 'sdfsdf', human_name, 'sqlite:///:memory:'))


class TestGetNoteableConnection:
    def test_can_get_by_either_sql_cell_handle_or_human_name(self, sqlite_database_connection):
        handle, human_name = sqlite_database_connection

        assert get_noteable_connection(handle) == get_noteable_connection(
            human_name
        ) and isinstance(get_noteable_connection(handle), Connection)

    def test_raises_if_not_found(self):
        with pytest.raises(UnknownConnectionError):
            get_noteable_connection('unknown connection')

    # will test raising on non-SLQA supported connection when first one is implemented.


class TestGetSqlaConnection:
    def test_can_get_by_either_sql_cell_handle_or_human_name(self, sqlite_database_connection):
        handle, human_name = sqlite_database_connection

        assert get_sqla_connection(handle) == get_sqla_connection(human_name) and isinstance(
            get_sqla_connection(handle), sqlalchemy.engine.base.Connection
        )

    def test_raises_if_not_found(self):
        with pytest.raises(UnknownConnectionError):
            get_sqla_connection('unknown connection')

    # will test raising on non-SLQA supported connection when first one is implemented.


class TestGetSqlaEngine:
    def test_can_get_by_either_sql_cell_handle_or_human_name(self, sqlite_database_connection):
        handle, human_name = sqlite_database_connection

        assert get_sqla_engine(handle) == get_sqla_engine(human_name) and isinstance(
            get_sqla_engine(handle), sqlalchemy.engine.Engine
        )

    def test_raises_if_not_found(self):
        with pytest.raises(UnknownConnectionError):
            get_sqla_engine('unknown connection')

    # will test raising on non-SLQA supported connection when first one is implemented.
