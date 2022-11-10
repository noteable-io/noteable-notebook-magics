import os
from typing import Dict, Optional, Union

import sqlalchemy
import sqlalchemy.engine.base
import structlog
from sqlalchemy.engine import Engine

logger = structlog.get_logger(__name__)


class UnknownConnectionError(Exception):
    pass


def rough_dict_get(dct, sought, default=None):
    """
    Like dct.get(sought), but any key containing sought will do.

    If there is a `@` in sought, seek each piece separately.
    This lets `me@server` match `me:***@myserver/db`
    """

    sought = sought.split("@")
    for (key, val) in dct.items():
        if not any(s.lower() not in key.lower() for s in sought):
            return val
    return default


class Connection(object):
    current = None
    connections: Dict[str, 'Connection'] = {}

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

            logger.exception(
                'Error creating new noteable_magics.sql.Connection', connect_str=connect_str
            )

            if connect_str.startswith('@'):
                # Is indeed the above most likely reason. Cell ran something like "%sql @3244356456 select true",
                # and magic.py's call to `conn = noteable_magics.sql.connection.Connection.set(...)`
                # ended up here trying to create a new Connection and Engine because '@3244356456' didn't
                # rendezvous with an already known bootstrapped datasource from vault via the startup-time bootstrapping
                # noteable_magics.datasources.bootstrap_datasources() call. Most likely reason for that is because
                # the user created the datasource _after_ when the kernel was launched and other checks and balances
                # didn't prevent them from trying to gesture to use it in this kernel session.
                raise UnknownConnectionError(
                    "Cannot find data connection. If you recently created this connection, please restart the kernel."
                )
            else:
                # Hmm. Maybe something desperately wrong at inside of a bootstrapped datasource? Just re-raise.
                raise

        self.dialect = self._engine.url.get_dialect()
        self.metadata = sqlalchemy.MetaData(bind=self._engine)
        self.name = name or self.assign_name(self._engine)
        self.human_name = human_name
        self._session = None
        self.connections[name or repr(self.metadata.bind.url)] = self

        Connection.current = self

    @property
    def session(self) -> sqlalchemy.engine.base.Connection:
        """Lazily connect to the database.

        Despite the name, this is a SQLA Connection, not a Session. And 'Connection'
        is highly overused term around here.
        """

        if not self._session:
            self._session = self._engine.connect()

        return self._session

    @classmethod
    def set(
        cls,
        descriptor: Union[str, 'Connection'],
        name: Optional[str] = None,
        **create_engine_kwargs
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
    def get_engine(cls, name: str) -> Optional[Engine]:
        """Return the SQLAlchemy Engine given either the sql_cell_handle or
        end-user assigned name for the connection.
        """
        for c in cls.connections.values():
            if c.name == name or c.human_name == name:
                return c._engine

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
        conn.session.close()

    def close(self):
        self.__class__._close(self)
