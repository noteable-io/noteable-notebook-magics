from typing import Dict, Optional

import sqlalchemy
import sqlalchemy.engine.base
import structlog
from sqlalchemy.engine import Engine

__all__ = (
    'get_connection_registry',
    'get_noteable_connection',
    'get_sqla_connection',
    'get_sqla_engine',
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


class ConnectionRegistry:
    """A registry of Connection instances"""

    connections: Dict[str, Connection]
    """My registry of connections. A single Connection will be multiply-registered, by its '@{sql_cell_handle}' as well as its 'human name' for convenience to humans."""

    bootstrapping_failures: Dict[str, str]
    """Deferred errors from bootstrapping time. The error will be correlated to both the sql_cell_handle and the human name."""

    def __init__(self):
        self.connections = {}
        self.bootstrapping_failures = {}

    def factory_and_register(
        self, sql_cell_handle: str, human_name: str, connection_url: str, **kwargs
    ):
        """Factory a connection instance and register it.

        If we encounter an exception at factory time, then remember it as a bootstrapping failure.
        """
        conn = self.factory(sql_cell_handle, human_name, connection_url, **kwargs)

        # If still here, then all good.
        self.register(conn)

    def factory(self, sql_cell_handle: str, human_name: str, connection_url: str, **kwargs):
        """Construct the appropriate Connection subclass.

        Does not register the instance, only returns it.
        """

        # Simple for now, only knows how to make SQLA-ish Connection objs. Will generalize in later steps and figure out how
        # to have the SQLA Connection subclass declare its preference for some, and other subclasses like Jira declare preference
        # for others.
        return Connection(
            sql_cell_handle=sql_cell_handle,
            human_name=human_name,
            connection_url=connection_url,
            **kwargs,
        )

    def register(self, conn: Connection):
        """Register this connection into self under both its SQL cell handle it its human assigned name"""

        if not isinstance(conn, Connection):
            raise ValueError('connection must be a Connection instance')

        if conn.sql_cell_handle in self.connections:
            raise ValueError(
                f'Datasource with handle {conn.sql_cell_handle} is already registered!'
            )

        self.connections[conn.sql_cell_handle] = conn
        self.connections[conn.human_name] = conn

        self.bootstrapping_failures.pop(conn.sql_cell_handle, None)
        self.bootstrapping_failures.pop(conn.human_name, None)

    def add_bootstrapping_failure(self, sql_cell_handle: str, human_name: str, error_message: str):
        """Remember (short) reason why we could not bootstrap a connection by this name,
        so that we can tell the user about it if / when they try to use the connection
        in a SQL cell.
        """

        if sql_cell_handle in self.connections:
            raise ValueError(
                'Strange: this connection is already defined, but now reporting bootstrapping failure? Perhaps close_and_pop() first?'
            )

        self.bootstrapping_failures[sql_cell_handle] = error_message
        self.bootstrapping_failures[human_name] = error_message

    def get(self, handle_or_human_name: str) -> Optional[Connection]:
        """Find a connection by SQL cell handle or by human assigned name. If not present and expected,
        then perhaps call get_bootstrapping_failure() to learn of any deferred construction issues.

        Raises UnknownConnectionError if there is no Connection registered by this name.
        """

        if conn := self.connections.get(handle_or_human_name):
            return conn

        # Perhaps had bootstrapping error?
        if bootstrapping_error := self.get_bootstrapping_failure(handle_or_human_name):
            # Could use a better exception type here.
            raise UnknownConnectionError(
                f'Please check data connection configuration, correct, and restart kernel:\n{bootstrapping_error}'
            )

        # Otherwise is just plain unknown.
        raise UnknownConnectionError(
            'Cannot find data connection. If you recently created this connection, please restart the kernel.'
        )

    def get_bootstrapping_failure(self, handle_or_name: str) -> Optional[str]:
        """Return failure-to-bootstrap reason (if any) related to this
        datasource by either its sql handle / id ("@3464564") or its human
        name ("My PostgreSQL")
        """
        return self.bootstrapping_failures.get(handle_or_name)

    def close_and_pop(self, handle_or_name: str):
        """If this handle_or_name is in self, then close it and forget about it."""

        conn = self.connections.get(handle_or_name)
        if conn:
            try:
                conn.close()
            finally:
                self.connections.pop(conn.sql_cell_handle, None)
                self.connections.pop(conn.human_name, None)

        self.bootstrapping_failures.pop(handle_or_name, None)

    def __len__(self):
        # Each Connection is double-registered under both sql_cell_handle and human name, so div by two.
        return len(self.connections) // 2

    def __contains__(self, key: str):
        return key in self.connections


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
