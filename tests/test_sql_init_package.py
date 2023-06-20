import pytest
import sqlalchemy.engine
import sqlalchemy.engine.base

# These functions are consciously exposed for use within Notebooks.
from noteable.sql import get_sqla_connection, get_sqla_engine


class TestToplevelExposedConvenienceGetters:
    @pytest.mark.parametrize(
        'convenence_function,expected_class',
        [
            (get_sqla_connection, sqlalchemy.engine.base.Connection),
            (get_sqla_engine, sqlalchemy.engine.Engine),
        ],
    )
    def test_get_sqla_connection(
        self, convenence_function, expected_class, sqlite_database_connection
    ):
        for name in sqlite_database_connection:  # both the sql cell handle and the human name ...
            assert isinstance(convenence_function(name), expected_class)
