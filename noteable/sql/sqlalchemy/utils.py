from functools import wraps
from typing import Any, Callable, List, Optional
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine import CursorResult

from noteable.sql import ResultSet


import structlog

logger = structlog.get_logger(__name__)


class SQLAlchemyResult(ResultSet):
    """
    Results of a query from SQLAlchemy, converted into noteable.sql.ResultSet form.
    """

    # Result of a SELECT or perhaps INSERT INTO ... RETURNING projecting a result set.
    keys: Optional[List[str]] = None
    rows: Optional[list] = None

    # In case of an INSERT, UPDATE, or DELETE statement.
    rowcount: Optional[int] = None

    has_results_to_report: bool = True

    def __init__(self, sqla_result: CursorResult):
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


def handle_not_implemented(default: Any = None, default_factory: Callable[[], Any] = None):
    """Decorator to catch NotImplementedError, return either default constant or
    whatever  default_factory() returns."""
    assert default or default_factory, 'must provide one of default or default_factory'
    assert not (
        default and default_factory
    ), 'only provide one of either default or default_factory'

    def wrapper(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except NotImplementedError:
                if default_factory:
                    return default_factory()
                else:
                    return default

        return wrapped

    return wrapper


class WrappedInspector:
    """Wraps the underlying sqlalchemy inspector, guards against a few methods returning NotImplemented"""

    def __init__(self, underlying_inspector: Inspector):
        self.underlying_inspector = underlying_inspector

    # Direct passthrough attributes / methods
    @property
    def default_schema_name(self) -> Optional[str]:
        # BigQuery, Trino dialects may end up returning None.
        return self.underlying_inspector.default_schema_name

    @property
    def engine(self) -> Engine:
        return self.underlying_inspector.engine

    def get_schema_names(self) -> List[str]:
        return self.underlying_inspector.get_schema_names()

    def get_columns(self, relation_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_columns(relation_name, schema=schema)

    @handle_not_implemented('(unobtainable)')
    def get_view_definition(self, view_name: str, schema: Optional[str] = None) -> str:
        return self.underlying_inspector.get_view_definition(view_name, schema=schema)

    def get_pk_constraint(self, table_name: str, schema: Optional[str] = None) -> dict:
        return self.underlying_inspector.get_pk_constraint(table_name, schema=schema)

    def get_foreign_keys(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_foreign_keys(table_name, schema=schema)

    @handle_not_implemented(default_factory=list)
    def get_check_constraints(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_check_constraints(table_name, schema=schema)

    def get_indexes(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_indexes(table_name, schema=schema)

    @handle_not_implemented(default_factory=list)
    def get_unique_constraints(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_unique_constraints(table_name, schema=schema)

    # Now the value-adding filtering methods.
    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        return self.underlying_inspector.get_table_names(schema)

    def get_view_names(self, schema: Optional[str] = None) -> List[str]:
        return self.underlying_inspector.get_view_names(schema)


class BigQueryInspector(WrappedInspector):
    """Proxy sqlalchemy.engine.reflection.Inspectory implementation that removes 'schema.'
    prefixing from results of underlying get_table_names() and get_view_names().
    BigQuery dialect inspector seems to include the schema (dataset) name in those return results,
    unlike other dialects.
    """

    def get_view_definition(self, view_name: str, schema: Optional[str] = None) -> str:
        # Sigh. Have to explicitly interpolate schema back into view name, else
        # underlying driver code complains. Not even joking.
        if schema:
            view_name = f'{schema}.{view_name}'

        return self.underlying_inspector.get_view_definition(view_name, schema=schema)

    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        names = self.underlying_inspector.get_table_names(schema)
        return self._strip_schema(names, schema)

    def get_view_names(self, schema: Optional[str] = None) -> List[str]:
        names = self.underlying_inspector.get_view_names(schema)
        return self._strip_schema(names, schema)

    def _strip_schema(self, names: List[str], schema: Optional[str] = None) -> List[str]:
        if not schema:
            return names

        prefix = f'{schema}.'
        # Remove "schema." from the start of each name if starts with.
        # (name[False:] is equiv to name[0:], 'cause python bools are subclasses of ints)
        return [name[name.startswith(prefix) and len(prefix) :] for name in names]
