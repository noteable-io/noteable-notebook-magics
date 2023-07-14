from functools import lru_cache, wraps
from typing import Any, Callable, Iterable, List, Optional, Tuple

import structlog
from sqlalchemy.engine import CursorResult
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.types import TypeEngine

from noteable.sql import ResultSet
from noteable.sql.connection import InspectorProtocol

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


class WrappedInspector(InspectorProtocol):
    """Base implementation for InspectorProtocol on top of SQLAlchemy datasources.
    Wraps the underlying sqlalchemy Inspector instance, guards against a few methods returning NotImplemented
    """

    max_concurrency = 10
    schemas_to_avoid: Iterable[str]

    def __init__(self, underlying_inspector: Inspector, schemas_to_avoid=('information_schema',)):
        self.underlying_inspector = underlying_inspector
        self.schemas_to_avoid = schemas_to_avoid

    # Direct passthrough attributes / methods
    @property
    def default_schema_name(self) -> Optional[str]:
        # BigQuery, Trino dialects may end up returning None.
        return self.underlying_inspector.default_schema_name

    def get_schema_names(self) -> List[str]:
        """Returns all schemas reported by the underlying SQLA inspector, minus
        those we have been instructed to avoid, case-insensitively"""
        underlying_schemas = self.underlying_inspector.get_schema_names()

        # Ensure that the default schema is named also. Some dialect omits this
        # (can't remember which, though)
        default_schema = self.underlying_inspector.default_schema_name
        if default_schema and default_schema not in underlying_schemas:
            underlying_schemas.append(default_schema)

        # Honor avoiding schemas_to_avoid case insensitively
        return [s for s in underlying_schemas if s.lower() not in self.schemas_to_avoid]

    def get_columns(self, relation_name: str, schema: Optional[str] = None) -> List[dict]:
        """Call the underlying get_columns(), but convert the type object members
        to strings, not SQLA-centric objects."""
        columns: List[dict] = self.underlying_inspector.get_columns(relation_name, schema=schema)

        for col in columns:
            col['type'] = _determine_column_type_name(col['type'])

            # Some dialects do not return at all, but are expected to.
            if 'comment' not in col:
                col['comment'] = ''

        return columns

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

    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        return self.underlying_inspector.get_table_names(schema)

    def get_view_names(self, schema: Optional[str] = None) -> List[str]:
        return self.underlying_inspector.get_view_names(schema)


class AthenaInspector(WrappedInspector):
    def get_pk_constraint(self, table_name: str, schema: Optional[str] = None) -> Optional[dict]:
        # Athena does not support PKs, and the underlying dialect returns emtpy _list_,
        # not an empty _dict_!

        # Athena dialect returns ... an empty _list_ instead of a dict, contrary to what
        # https://docs.sqlalchemy.org/en/14/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_pk_constraint
        # specifies for the return result from inspector.get_pk_constraint().

        return None


class BigQueryInspector(WrappedInspector):
    """Proxy sqlalchemy.engine.reflection.Inspectory implementation that removes 'schema.'
    prefixing from results of underlying get_table_names() and get_view_names().
    BigQuery dialect inspector seems to include the schema (dataset) name in those return results,
    unlike other dialects.
    """

    # BQ's introspection API doesn't seem to be fully thread safe?
    max_concurrency = 1

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


class CockroachDBInspector(WrappedInspector):
    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        return self._get_tables_and_views(schema)[0]

    def get_view_names(self, schema: Optional[str] = None) -> List[str]:
        return self._get_tables_and_views(schema)[1]

    @lru_cache(maxsize=None)
    def _get_tables_and_views(self, schema: Optional[str]) -> Tuple[List[str], List[str]]:
        """Returns tuple of table names, view names. Deals with CRDB's tendency to describe
        views as both tables and views

        """
        table_names = set(self.underlying_inspector.get_table_names(schema))
        view_names = self.underlying_inspector.get_view_names(schema)

        if view_names:
            # Remove any view names from our pristeen list of table names.
            table_names.difference_update(view_names)

        return (list(table_names), view_names)


class MySQLInspector(WrappedInspector):
    def get_pk_constraint(self, table_name: str, schema: Optional[str] = None) -> Optional[dict]:
        # MySQL seems to support unnamed primary keys? So ensure all are named.

        pk_constraint = self.underlying_inspector.get_pk_constraint(table_name, schema)
        if pk_constraint and not pk_constraint.get('name'):
            pk_constraint['name'] = '(unnamed primary key)'

        return pk_constraint


class RedshiftInspector(WrappedInspector):
    def get_view_definition(self, view_name: str, schema: Optional[str] = None) -> str:
        """Redshift dialect's get_view_definition() returns text() for some strange reason. Downcast to str."""
        underlying_result = self.underlying_inspector.get_view_definition(view_name, schema=schema)
        if underlying_result is not None and isinstance(underlying_result, TextClause):
            return str(underlying_result)

        # Most degenerate
        return ''


def _determine_column_type_name(sqla_column_type_object: TypeEngine) -> str:
    """Convert the possibly db-centric TypeEngine instance to a sqla-generic type string"""
    try:
        type_name = str(sqla_column_type_object.as_generic()).lower()
    except (NotImplementedError, AssertionError):
        # ENG-5268: More esoteric types like UUID do not implement .as_generic()
        # ENG-5808: Some Databricks types are not fully implemented and fail
        # assertions within .as_generic()
        type_name = str(sqla_column_type_object).replace('()', '').lower()

    return type_name
