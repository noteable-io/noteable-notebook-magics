from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Protocol, Type, TypeVar, runtime_checkable

import pandas as pd
import sqlalchemy
import sqlalchemy.engine.base
import structlog
from sqlalchemy.engine import Engine

__all__ = (
    'get_connection_registry',
    'get_noteable_connection',
    'get_sqla_connection',
    'get_sqla_engine',
    'ConnectionBootstrapper',
    'UnknownConnectionError',
    'SQLAlchemyUnsupportedError',
    'ResultSet',
    'Connection',
    'ConnectionRegistry',
    'connection_class',
    'get_connection_class',
)

logger = structlog.get_logger(__name__)


class UnknownConnectionError(Exception):
    """There is no noteable.sql.Connection registered for the given string key"""

    pass


class SQLAlchemyUnsupportedError(Exception):
    """The noteable.sql.Connection referenced is not implemented using SQLAlchemy"""

    pass


class ResultSet(Protocol):
    """
    Results of a query against any kind of data connection / connection type.
    """

    keys: Optional[List[str]]
    """Column names from the result, if any"""

    rows: Optional[list]
    """List of rows from the result, if any. Each row should be len(keys) long."""
    # In case of an INSERT, UPDATE, or DELETE statement.

    rowcount: Optional[int]
    """How many rows were affected by an INSERT/UPDATE/DELETE sort of statement?"""

    has_results_to_report: bool
    """Most queries will have results to report, but CREATE TABLE and other DDLs may not."""

    @property
    def is_scalar_value(self) -> bool:
        """Is result expressable as a single scalar value w/o losing any information?"""
        return self.has_results_to_report and (
            (self.rowcount is not None) or (len(self.rows) == 1 and len(self.rows[0]) == 1)
        )

    @property
    def scalar_value(self):
        """Return either the only row / column value, or the affected num of rows
        from an INSERT/DELETE/UPDATE statement as bare scalar"""

        # Should only be called if self.is_scalar_value
        if self.rowcount is not None:
            return self.rowcount
        else:
            return self.rows[0][0]

    @property
    def can_become_dataframe(self) -> bool:
        return self.has_results_to_report and self.rows is not None

    def to_dataframe(self) -> pd.DataFrame:
        "Returns a Pandas DataFrame instance built from the result set, if possible."

        # Should only be called if self.can_become_dataframe is True

        # Worst case will be a zero row but defined columns dataframe.
        return pd.DataFrame(self.rows, columns=self.keys)


@runtime_checkable
class Connection(Protocol):
    """Protocol defining all Noteable Data Connection implementations"""

    sql_cell_handle: str
    """Machine-accessible name/id, aka @35647345345345 ..."""

    human_name: str
    """Human assigned datasource name"""

    is_sqlalchemy_based: bool
    """Is this conection implemented on top of SQLAlchemy?"""

    # Lifecycle methods

    def execute(self, statement: str, bind_dict: Dict[str, Any]) -> ResultSet:
        """Execute this statement, possibly interpolating the values in bind_dict"""
        ...  # pragma: no cover

    def close(self) -> None:
        """Close any resources currently allocated to this connection"""
        ...  # pragma: no cover


@runtime_checkable
class InspectorProtocol(Protocol):
    """Protocol describing a subset of the SQLAlchemy Inspector class"""

    max_concurrency: int
    """Maximum concurrency allowed within introspection"""

    default_schema_name: Optional[str]

    def get_schema_names(self) -> List[str]:
        ...  # pragma: no cover

    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        ...  # pragma: no cover

    def get_view_names(self, schema: Optional[str] = None) -> List[str]:
        ...  # pragma: no cover

    def get_columns(self, relation_name: str, schema: Optional[str] = None) -> List[dict]:
        ...  # pragma: no cover

    def get_view_definition(self, view_name: str, schema: Optional[str] = None) -> str:
        ...  # pragma: no cover

    def get_pk_constraint(self, table_name: str, schema: Optional[str] = None) -> dict:
        ...  # pragma: no cover

    def get_foreign_keys(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        ...  # pragma: no cover

    def get_check_constraints(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        ...  # pragma: no cover

    def get_indexes(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        ...  # pragma: no cover

    def get_unique_constraints(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        ...  # pragma: no cover


@runtime_checkable
class IntrospectableConnection(Protocol):
    """Sub-Protocol of Connection describing Connection types supporting schema / table / view discovery"""

    def get_inspector(self) -> InspectorProtocol:
        """Return an object for performing introspections into this database using a SQLAlchemy-esqe API"""


class BaseConnection(Connection):
    sql_cell_handle: str
    human_name: str

    def __init__(self, sql_cell_handle: str, human_name: str):
        super().__init__()

        if not sql_cell_handle.startswith("@"):
            raise ValueError("sql_cell_handle values must start with '@'")

        if not human_name:
            raise ValueError("Connections must have a human-assigned name")

        # Common bits to make it into base class when splittin this up into SQLA subclass and Random HTTP/Python Client API subclasses.
        self.sql_cell_handle = sql_cell_handle
        self.human_name = human_name


# Dict of drivername -> Connection implementation
_drivername_to_connection_type: Dict[str, Type[Connection]] = {}


def connection_class(drivername: str):
    """Decorator to register a concrete Connection implementation to use for the given driver"""

    # Explicitly allows for overwriting any old binding so as to allow for notebook-side
    # hotpatching.

    def decorator_outer(clazz):
        _drivername_to_connection_type[drivername] = clazz

        return clazz

    return decorator_outer


def get_connection_class(drivername: str) -> Type[Connection]:
    """Return the Connection implementation class registered for this driver.

    Raises KeyError if no implementation is registered.
    """
    return _drivername_to_connection_type[drivername]


ConnectionBootstrapper: TypeVar = Callable[[], Connection]
"""Zero-arg function, that when called, will return a Connection instance to be retained in a ConnectionRegistry."""


class ConnectionRegistry:
    """A registry of Connection instances and bootstrapping functions that will create Connections on first need"""

    bootstrappers: Dict[str, ConnectionBootstrapper]
    """Dict of sql cell handle or human name -> function that, when passed this registry, will construct
       the Connection for that handle upon demand. When the connection is bootstrapped, then
       the entries will be removed from this dict, in that they won't be needed anymore.

       All external data connections are bootstrapped upon first demand within the notebook, not at notebook launch.
    """

    connections: Dict[str, Connection]
    """My registry of bootstrapped, live connections. A single Connection will be multiply-registered, by its '@{sql_cell_handle}'
       as well as its 'human name' for convenience to humans.
    """

    def __init__(self):
        self.connections = {}
        self.bootstrappers = {}

    def register_datasource_bootstrapper(
        self, sql_cell_handle: str, human_name: str, bootstrapper: ConnectionBootstrapper
    ):
        """Register a function that will, upon first need, bootstrap and construct a Connection to be retained."""

        # Kernel startup in `noteable.datasources.discover_datasources()` will call into this once for each possible
        # datasource found in vault secret filesystem, as well as for DuckDB.

        if not (sql_cell_handle and sql_cell_handle.startswith('@')):
            raise ValueError(
                f'sql_cell_handle must be provided and start with "@": {sql_cell_handle}'
            )

        if not callable(bootstrapper):
            raise TypeError(
                f'Data connection bootstrapper functions must be zero-arg callables, got {type(bootstrapper)} for {sql_cell_handle!r}'
            )

        # Explicitly allow new registration shadowing out a prior one for test suite purposes at this time.
        self.bootstrappers[sql_cell_handle] = bootstrapper
        self.bootstrappers[human_name] = bootstrapper

    def get(self, handle_or_human_name: str) -> Connection:
        """Find a connection either by cell handle or by human assigned name.

            If not constructed already, then consult the pending-to-be-bootstrapped
            dict, and if there's a bootstrapper registered, then call it to bootstrap
            the connection.

            Any exceptions raised by the bootstrapping process will be thrown by this method, even
            from repeated bootstrapping attempts within same notebook kernel, since bootstrappers won't
            be able to get their connection registered (and then the bootstrapper forgotten about)
            within this registry until the bootstrapping process completes successfully.

        Raises UnknownConnectionError if there is no Connection registered by this name and no
        registered bootstrapper.

        Raises any exception coming from the bootstrapper function if was needed to be called.
        """

        if conn := self.connections.get(handle_or_human_name):
            return conn

        # Hopefully just not bootstrapped yet? Consult the pending bootstrappers!
        if bootstrapper := self.bootstrappers.get(handle_or_human_name):
            # Any data connection bootstrapping errors will be raised right now for this cell.
            conn = bootstrapper()

            if not isinstance(conn, Connection):
                raise TypeError(
                    f'Data connection bootstrapper for {handle_or_human_name} returned something other than a Connection instance!'
                )

            # Register the connection handler from now on. Will de-register the bootstrapper.
            self._register(conn)

            return conn

        # Otherwise is just plain unknown.
        raise UnknownConnectionError(
            'Cannot find data connection. If you recently created this connection, please restart the kernel.'
        )

    def close_and_pop(self, handle_or_name: str):
        """If this handle_or_name is in self, then close it and forget about it."""

        conn = self.connections.get(handle_or_name)
        if conn:
            try:
                conn.close()
            finally:
                self.connections.pop(conn.sql_cell_handle, None)
                self.connections.pop(conn.human_name, None)

    def __len__(self):
        # Each Connection is double-registered under both sql_cell_handle and human name, so div by two.
        return len(self.connections) // 2

    def _register(self, conn: Connection):
        """Register this connection into self under both its SQL cell handle and its human assigned name"""

        if not isinstance(conn, Connection):
            raise ValueError('connection must be a Connection instance')

        if conn.sql_cell_handle in self.connections:
            raise ValueError(
                f'Datasource with handle {conn.sql_cell_handle} is already registered!'
            )

        if conn.human_name in self.connections:
            raise ValueError(f'Datasource with human name {conn.human_name} is already registered!')

        self.connections[conn.sql_cell_handle] = conn
        self.connections[conn.human_name] = conn

        # No longer need any existing bootstrapper entries for these names (bootstrapping now complete).
        self.bootstrappers.pop(conn.sql_cell_handle, None)
        self.bootstrappers.pop(conn.human_name, None)


_registry_singleton: Optional[ConnectionRegistry] = None


def get_connection_registry() -> ConnectionRegistry:
    """Return the singleton instance of `ConnectionRegistry`"""
    global _registry_singleton

    if _registry_singleton is None:
        _registry_singleton = ConnectionRegistry()

    return _registry_singleton


def get_noteable_connection(name_or_handle: str) -> Connection:
    """Return the noteable.sql.connection.Connection corresponding to the requested
        datasource a name or handle.

    Will raise UnknownConnectionError if the given handle isn't present in
    the connections dict already (created after this kernel was launched?)
    """
    return get_connection_registry().get(name_or_handle)


def get_sqla_connection(name_or_handle: str) -> sqlalchemy.engine.base.Connection:
    """Return a SQLAlchemy connection given a name or handle.

    Raises UnknownConnectionError if cannot find by this string.
    Raises SQLAlchemyUnsupportedError if the given Connection doesn't support SQLAlchemy (future expansion)
    """
    conn = get_connection_registry().get(name_or_handle)

    if not conn.is_sqlalchemy_based:
        raise SQLAlchemyUnsupportedError(
            f'Connection {name_or_handle} ({conn!r}) does not support SQLAlchemy'
        )

    return conn.sqla_connection


def get_sqla_engine(name_or_handle: str) -> Engine:
    """Return a SQLAlchemy Engine given a name or handle.

    Raises UnknownConnectionError if cannot find by this string.
    Raises SQLAlchemyUnsupportedError if the given Connection doesn't support SQLAlchemy (future expansion)
    """
    conn = get_connection_registry().get(name_or_handle)
    if not conn.is_sqlalchemy_based:
        raise SQLAlchemyUnsupportedError(
            f'Connection {name_or_handle} ({conn!r}) does not support SQLAlchemy'
        )

    return conn.sqla_engine
