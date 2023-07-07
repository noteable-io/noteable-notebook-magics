import pytest

from noteable.sql.sqlalchemy import (
    AthenaInspector,
    ClickhouseConnection,
    CockroachDBConnection,
    MySQLInspector,
    PostgreSQLConnection,
    WrappedInspector,
)


class TestWrappedInspector:
    @pytest.mark.parametrize(
        'schemas_to_avoid,default_schema,underlying_schemas,,expected_schemas',
        [
            # default schema listed also in underlying schemas; expect 'information_schema' to be stripped out.
            (['information_schema'], 'public', ['public', 'information_schema'], ['public']),
            # default schema not also listed in underlying schemas
            (['information_schema'], 'public', ['information_schema'], ['public']),
            (
                ClickhouseConnection.schemas_to_avoid,
                'public',
                ['information_schema', 'system'],
                ['public'],
            ),
            (
                CockroachDBConnection.schemas_to_avoid,
                'public',
                ['public', 'information_schema', 'pg_catalog', 'crdb_internal'],
                ['public'],
            ),
            (
                PostgreSQLConnection.schemas_to_avoid,
                'public',
                ['public', 'information_schema', 'pg_catalog'],
                ['public'],
            ),
        ],
    )
    def test_get_schema_names(
        self, schemas_to_avoid, default_schema, underlying_schemas, expected_schemas, mocker
    ):
        underlying_inspector = mocker.Mock()
        underlying_inspector.get_schema_names = mocker.Mock(return_value=underlying_schemas)
        underlying_inspector.default_schema_name = default_schema

        wrapping_inspector = WrappedInspector(
            underlying_inspector, schemas_to_avoid=schemas_to_avoid
        )
        assert wrapping_inspector.get_schema_names() == expected_schemas


class TestAthenaInspector:
    def test_handles_returning_none_for_pk(self, mocker):
        underlying_inspector = mocker.Mock()
        underlying_inspector.get_pk_constraint = mocker.Mock()

        # Empty list! At worst coulda been empty dict.
        underlying_inspector.get_pk_constraint.return_value = []

        inspector = AthenaInspector(underlying_inspector)

        # Expect the unnamed constraint to get promoted to be named.
        assert inspector.get_pk_constraint('my_table') is None


class TestMySQLInspector:
    def test_handles_unnamed_pk_constraints(self, mocker):
        underlying_inspector = mocker.Mock()
        underlying_inspector.get_pk_constraint = mocker.Mock()

        underlying_return = {'constrained_columns': ['id']}
        underlying_inspector.get_pk_constraint.return_value = underlying_return

        inspector = MySQLInspector(underlying_inspector)

        # Expect the unnamed constraint to get promoted to be named.
        assert inspector.get_pk_constraint('my_table') == underlying_return | {
            'name': '(unnamed primary key)'
        }
