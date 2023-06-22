from __future__ import annotations

from typing import Any, Dict, List, Optional

import sqlalchemy
from sqlalchemy.engine import Dialect

from .connection import BaseConnection, ResultSet


class SQLAlchemyResult(ResultSet):
    """
    Results of a SQL query.
    """

    # Result of a SELECT or perhaps INSERT INTO ... RETURNING projecting a result set.
    keys: Optional[List[str]] = None
    rows: Optional[list] = None

    # In case of an INSERT, UPDATE, or DELETE statement.
    rowcount: Optional[int] = None

    has_results_to_report: bool = True

    def __init__(self, sqla_result):
        # Check for non-empty list of keys in addition to returns_rows flag.

        # NOTE: Clickhouse does funky things with INSERT/UPDATE/DELETE statements
        #       and sets returns_rows to True even though there are no results or keys.
        #       We don't want to report results in that case.
        if sqla_result.returns_rows and len(keys := list(sqla_result.keys())) > 0:
            self.keys = keys
            self.rows = sqla_result.fetchall()
        elif sqla_result.rowcount != -1:
            # Was either DDL or perhaps DML like an INSERT or UPDATE statement
            # that just talks about number or rows affected server-side.
            self.rowcount = sqla_result.rowcount
        else:
            # CREATE TABLE or somesuch DDL that ran successfully and offers
            # no constructive feedback whatsoever.
            self.has_results_to_report = False


class SQLAlchemyConnection(BaseConnection):
    is_sqlalchemy_based: bool = True

    def __init__(
        self,
        sql_cell_handle: str,
        human_name: str,
        connection_url: str,
        needs_explicit_commit: bool,
        **create_engine_kwargs,
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

        super().__init__(sql_cell_handle, human_name)

        self._engine = sqlalchemy.create_engine(connection_url, **create_engine_kwargs)

        # Legacy bit, might not be needed anymore?
        if self.dialect_name in {
            "awsathena",
            "clickhouse",
            "mssql",
        }:
            needs_explicit_commit = False

        self._needs_explicit_commit = needs_explicit_commit
        self._create_engine_kwargs = create_engine_kwargs  # Retained for test suite purposes.

    def execute(self, statement: str, bind_dict: Dict[str, Any]) -> ResultSet:
        """Execute this statement, possibly interpolating the values in bind_dict"""

        sqla_connection = self.sqla_connection

        result = sqla_connection.execute(sqlalchemy.sql.text(statement), bind_dict)

        if self._needs_explicit_commit:
            sqla_connection.execute("commit")

        return SQLAlchemyResult(result)

    def close(self):
        """Close any resources currently allocated to this connection"""
        if self._sqla_connection:
            self._sqla_connection.close()
        self.reset_connection_pool()

    @property
    def sqla_engine(self) -> sqlalchemy.engine.base.Engine:
        return self._engine

    @property
    def dialect(self) -> Dialect:
        return self.sqla_engine.url.get_dialect()

    @property
    def dialect_name(self) -> str:
        return self.dialect.name

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
