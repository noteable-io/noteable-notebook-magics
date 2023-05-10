from typing import Dict, Optional, Union

import sqlalchemy
import sqlalchemy.engine.base
import structlog
from sqlalchemy.engine import Engine

logger = structlog.get_logger(__name__)


LOCAL_DB_CONN_HANDLE = "@noteable"
LOCAL_DB_CONN_NAME = "Local Database"
DUCKDB_LOCATION = "duckdb:///:memory:"


class UnknownConnectionError(Exception):
    pass


class Connection:
    current = None
    connections: Dict[str, 'Connection'] = {}
    bootstrapping_failures: Dict[str, str] = {}

    def __init__(self, connect_str=None, name=None, human_name=None, **create_engine_kwargs):
        """
        Construct + register a new 'connection', which in reality is a sqla Engine
        plus some convienent metadata.

        Common args to go into the create_engine call (and therefore need to be
        passed in within `create_engine_kwargs`) include:

          * connect_args: SQLA will pass these down to its call to create the DBAPI-level
                            connection class when new low-level connections are
                            established.

          * creator: Callable which itself returns the DBAPI connection. See
            https://docs-sqlalchemy.readthedocs.io/ko/latest/core/engines.html#custom-dbapi-connect-arguments

        Sets the 'current' connection to the newly init'd one.

        No session is immediately established (see the session property).

        'name' is what we call now the 'sql_cell_handle' -- starts with '@', followed by
        the hex of the datasource uuid (usually -- the legacy "local database" (was sqlite, now duckdb)
        and bigquery do not use the hex convention because they predate datasources)

        'human_name' is the name that the user gave the datasource ('My PostgreSQL Connection')
        (again, only for real datasource connections). There's a slight risk of name collision
        due to having the same name used between user and space scopes, but so be it.

        """
        if name and not name.startswith("@"):
            raise ValueError("preassigned names must start with @")

        if "creator" in create_engine_kwargs and create_engine_kwargs["creator"] is None:
            # As called from sql.magic in calling sql.connection.Connection.set,
            # will always pass kwarg 'creator', but it will most likely be None.
            # SQLA does not like it passed in as None (will still try to call it).
            # So must remove the dict. Is cause for why legacy BigQuery connection
            # fails.
            del create_engine_kwargs["creator"]

        try:
            self._engine = sqlalchemy.create_engine(connect_str, **create_engine_kwargs)
        except Exception:
            # Most likely reason to end up here: cell being asked to use a datasource that wasn't bootstrapped
            # as one of these Connections at kernel startup, and sql-magic ends up here, trying
            # to create a new Connection on the fly. But if given only something like
            # "@3453454567546" for the connect_str as from a SQL cell invocation, this obviously
            # isn't enough to create a new SQLA engine.

            logger.exception('Error creating new noteable.sql.Connection', connect_str=connect_str)

            if connect_str.startswith('@'):
                # Is indeed the above most likely reason. Cell ran something like "%sql @3244356456 select true",
                # and magic.py's call to `conn = noteable_magics.sql.connection.Connection.set(...)`
                # ended up here trying to create a new Connection and Engine because '@3244356456' didn't
                # rendezvous with an already known bootstrapped datasource from vault via the startup-time bootstrapping
                # noteable_magics.datasources.bootstrap_datasources() call. Most likely reason for that is because
                # the user created the datasource _after_ when the kernel was launched and other checks and balances
                # didn't prevent them from trying to gesture to use it in this kernel session.

                # Maybe we've a known bootstrapping problem for it?
                bootstrapping_error = self.get_bootstrapping_failure(connect_str)
                if bootstrapping_error:
                    error_msg = f'Please check data connection configuration, correct, and restart kernel:\n{bootstrapping_error}'
                else:
                    error_msg = "Cannot find data connection. If you recently created this connection, please restart the kernel."

                raise UnknownConnectionError(error_msg)
            else:
                # Hmm. Maybe something desperately wrong at inside of a bootstrapped datasource? Just re-raise.
                raise

        self.dialect = self._engine.url.get_dialect()
        self.metadata = sqlalchemy.MetaData(bind=self._engine)
        self.name = name or self.assign_name(self._engine)
        self.human_name = human_name
        self._sqla_connection = None
        self.connections[name or repr(self.metadata.bind.url)] = self

        Connection.current = self

    @property
    def engine(self) -> sqlalchemy.engine.base.Engine:
        return self._engine

    @property
    def sqla_connection(self) -> sqlalchemy.engine.base.Connection:
        """Lazily connect to the database. Return a SQLA Connection object, or die trying."""

        if not self._sqla_connection:
            self._sqla_connection = self._engine.connect()

        return self._sqla_connection

    def reset_connection_pool(self):
        """Reset the SQLA connection pool, such as after an exception suspected to indicate
        a broken connection has been raised.
        """
        self._engine.dispose()
        self._sqla_connection = None

    @classmethod
    def set(
        cls,
        descriptor: Union[str, 'Connection'],
        name: Optional[str] = None,
        **create_engine_kwargs,
    ):
        """Sets the current database connection. Will construct and cache new one on the fly if needed."""

        if descriptor:
            if isinstance(descriptor, Connection):
                cls.current = descriptor
            else:
                existing = rough_dict_get(cls.connections, descriptor)
                # http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#custom-dbapi-connect-arguments
                cls.current = existing or Connection(
                    descriptor,
                    name,
                    **create_engine_kwargs,
                )
        return cls.current

    @classmethod
    def assign_name(cls, engine):
        name = "%s@%s" % (engine.url.username or "", engine.url.database)
        return name

    @classmethod
    def connection_list(cls):
        result = []
        for key in sorted(cls.connections):
            engine_url = cls.connections[key].metadata.bind.url  # type: sqlalchemy.engine.url.URL
            if cls.connections[key] == cls.current:
                template = " * {}"
            else:
                template = "   {}"
            result.append(template.format(engine_url.__repr__()))
        return "\n".join(result)

    @classmethod
    def find(cls, name: str) -> Optional['Connection']:
        """Find a connection by SQL cell handle or by human assigned name"""
        # TODO: Capt. Obvious says to double-register the instance by both of these keys
        # to then be able to do lookups properly in this dict?
        for c in cls.connections.values():
            if c.name == name or c.human_name == name:
                return c

    @classmethod
    def get_engine(cls, name: str) -> Optional[Engine]:
        """Return the SQLAlchemy Engine given either the sql_cell_handle or
        end-user assigned name for the connection.
        """
        maybe_conn = cls.find(name)
        if maybe_conn:
            return maybe_conn.engine

    @classmethod
    def add_bootstrapping_failure(cls, name: str, human_name: Optional[str], error_message: str):
        """Remember (short) reason why we could not bootstrap a connection by this name,
        so that we can tell the user about it if / when they try to use the connection
        in a SQL cell.
        """

        cls.bootstrapping_failures[name] = error_message
        if human_name:
            cls.bootstrapping_failures[human_name] = error_message

    @classmethod
    def get_bootstrapping_failure(cls, handle_or_id: str) -> Optional[str]:
        """Return failure-to-bootstrap reason (if any) related to this
        datasource by either its sql handle / id ("@3464564") or its human
        name ("My PostgreSQL")
        """
        return rough_dict_get(cls.bootstrapping_failures, handle_or_id)

    def _close(cls, descriptor):
        if isinstance(descriptor, Connection):
            conn = descriptor
        else:
            conn = cls.connections.get(descriptor) or cls.connections.get(descriptor.lower())
        if not conn:
            raise Exception(
                "Could not close connection because it was not found amongst these: %s"
                % str(cls.connections.keys())
            )
        cls.connections.pop(conn.name, None)
        cls.connections.pop(str(conn.metadata.bind.url), None)
        conn.sqla_connection.close()

    def close(self):
        self.__class__._close(self)


def rough_dict_get(dct, sought, default=None):
    """
    Like dct.get(sought), but any key containing sought will do.

    If there is a `@` in sought, seek each piece separately.
    This lets `me@server` match `me:***@myserver/db`
    """

    sought = sought.split("@")
    for key, val in dct.items():
        if not any(s.lower() not in key.lower() for s in sought):
            return val
    return default


def get_db_connection(name_or_handle: str) -> Optional[Connection]:
    """Return the noteable.sql.connection.Connection corresponding to the requested
        datasource a name or handle.

    Will return None if the given handle isn't present in
    the connections dict already (created after this kernel was launched?)
    """
    return Connection.find(name_or_handle)


def get_sqla_connection(name_or_handle: str) -> Optional[sqlalchemy.engine.base.Connection]:
    """Return a SQLAlchemy connection given a name or handle
    Returns None if cannot find by this string.
    """
    nconn = get_db_connection(name_or_handle)
    if nconn:
        return nconn.sqla_connection


def get_sqla_engine(name_or_handle: str) -> Optional[Engine]:
    """Return a SQLAlchemy Engine given a name or handle.
    Returns None if cannot find by this string.
    """
    return Connection.get_engine(name_or_handle)


def bootstrap_duckdb():
    Connection.set(
        DUCKDB_LOCATION,
        human_name=LOCAL_DB_CONN_NAME,
        name=LOCAL_DB_CONN_HANDLE,
    )
