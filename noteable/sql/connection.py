from __future__ import annotations

from typing import Callable, Dict, Optional, TypeVar

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
)

logger = structlog.get_logger(__name__)


class UnknownConnectionError(Exception):
    """There is no noteable.sql.Connection registered for the given string key"""

    pass


class SQLAlchemyUnsupportedError(Exception):
    """The noteable.sql.Connection referenced is not implemented using SQLAlchemy"""

    pass


class Connection:
    sql_cell_handle: str
    """Machine-accessible name/id, aka @35647345345345 ..."""
    human_name: str
    """Human assigned datasource name"""

    def __init__(
        self, sql_cell_handle: str, human_name: str, connection_url: str, **create_engine_kwargs
    ):
        """
        Construct a new 'connection', which in reality is a sqla Engine
        plus some convienent metadata.

        Common args to go into the create_engine call (and therefore need to be
        passed in within `create_engine_kwargs`) include:

          * create_engine_kwargs: SQLA will pass these down to its call to create the DBAPI-level
                            connection class when new low-level connections are
                            established.

        No SQLA-level connection is immediately established (see the `sqla_connection` property).

        'name' is what we call now the 'sql_cell_handle' -- starts with '@', followed by
        the hex of the datasource uuid (usually -- the legacy "local database" (was sqlite, now duckdb)
        and bigquery do not use the hex convention because they predate datasources)

        'human_name' is the name that the user gave the datasource ('My PostgreSQL Connection')
        (again, only for real datasource connections). There's a slight risk of name collision
        due to having the same name used between user and space scopes, but so be it.

        """
        if not sql_cell_handle.startswith("@"):
            raise ValueError("sql_cell_handle values must start with '@'")

        if not human_name:
            raise ValueError("Connections must have a human-assigned name")

        # Common bits to make it into base class when splittin this up into SQLA subclass and Random HTTP/Python Client API subclasses.
        self.sql_cell_handle = sql_cell_handle
        self.human_name = human_name

        # SLQA-centric fields hereon down, to be pushed into SQLA subclass in the future.
        self._engine = sqlalchemy.create_engine(connection_url, **create_engine_kwargs)
        self._create_engine_kwargs = create_engine_kwargs

    def close(self):
        """General-ish API method; SQLA-centric implementation"""
        if self._sqla_connection:
            self._sqla_connection.close()
        self.reset_connection_pool()

    ####
    # SLQA-centric methods / properties here down
    ####

    is_sqlalchemy_based = True

    @property
    def sqla_engine(self) -> sqlalchemy.engine.base.Engine:
        return self._engine

    @property
    def dialect(self):
        return self.sqla_engine.url.get_dialect()

    _sqla_connection: Optional[sqlalchemy.engine.base.Connection] = None

    @property
    def sqla_connection(self) -> sqlalchemy.engine.base.Connection:
        """Lazily connect to the database. Return a SQLA Connection object, or die trying."""

        if not self._sqla_connection:
            self._sqla_connection = self.sqla_engine.connect()

        return self._sqla_connection

    def reset_connection_pool(self):
        """Reset the SQLA connection pool, such as after an exception suspected to indicate
        a broken connection has been raised.
        """
        self._engine.dispose()
        self._sqla_connection = None


ConnectionBootstrapper: TypeVar = Callable[[], Connection]
"""Zero-arg function, that when called, will return a Connection instance to be retained in a ConnectionRegistry."""


class ConnectionRegistry:
    """A registry of Connection instances and bootstrapping functions that will create Connections on first need"""

    boostrappers: Dict[str, ConnectionBootstrapper]
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
        self.boostrappers = {}

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
        self.boostrappers[sql_cell_handle] = bootstrapper
        self.boostrappers[human_name] = bootstrapper

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
        if bootstrapper := self.boostrappers.get(handle_or_human_name):
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
        self.boostrappers.pop(conn.sql_cell_handle, None)
        self.boostrappers.pop(conn.human_name, None)


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
