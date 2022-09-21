""" Tests over the data loading magic, "create_or_replace_data_view" """


import pandas as pd
import pytest
from IPython.core.interactiveshell import InteractiveShell

from noteable_magics.sql.connection import Connection
from noteable_magics.sql.magic import SqlMagic


@pytest.fixture
def ipython_shell() -> InteractiveShell:
    return InteractiveShell()


@pytest.fixture
def sql_magic(ipython_shell) -> SqlMagic:
    magic = SqlMagic(ipython_shell)
    # As would be done when we normally bootstrap things ...
    magic.autopandas = True

    return magic


@pytest.fixture
def foo_database_connection(with_empty_connections):
    """Make an @foo SQLite connection to simulate a non-default bootstrapped datasource."""

    handle = '@foo'
    human_name = "My Shiny Connection"
    Connection.set("sqlite:///:memory:", displaycon=False, name=handle, human_name=human_name)

    yield handle, human_name


@pytest.fixture
def populated_foo_database(foo_database_connection):
    connection = Connection.connections['@foo']
    db = connection.session  # sic, a sqlalchemy.engine.base.Connection, not a Session. Sigh.
    db.execute('create table int_table(a int, b int, c int)')
    db.execute('insert into int_table (a, b, c) values (1, 2, 3), (4, 5, 6)')


@pytest.mark.usefixtures("populated_foo_database")
class TestSqlMagic:
    def test_basic_query(self, sql_magic, ipython_shell):
        """Test basic query behavior"""

        results = sql_magic.execute('@foo select a, b from int_table')
        assert isinstance(results, pd.DataFrame)

        # Two rows as from populated_foo_database
        assert len(results) == 2
        assert results['a'].tolist() == [1, 4]
        assert results['b'].tolist() == [2, 5]

    def test_assigment_to_variable(self, sql_magic, ipython_shell):
        """Test that when the 'varname << select ...' syntax is used, the df is returned
        as the main execute result, and varname is side-effect assigned to."""

        results = sql_magic.execute('@foo my_df << select a from int_table')
        assert isinstance(results, pd.DataFrame)

        # Two rows as from populated_foo_database
        assert len(results) == 2
        assert results['a'].tolist() == [1, 4]

        # Should have also assigned the results to global 'my_df' in the ipython shell.
        assert ipython_shell.user_ns['my_df'] is results

    def test_does_not_assign_exceptions_to_variable(self, sql_magic, ipython_shell):
        """Test that if queries raise exceptions, those do not get assigned to any
        requested variable name. ENG-4730"""

        # This will raise a ProgrammingError error from sqlalchemy, which
        # sql magic will catch and convert to a short print output, then not
        # do the assignment (nor re-raise the exception).
        results = sql_magic.execute('@foo my_df << select a from nonexistent_table')
        assert results is None

        # Should NOT have also assigned the exception to global 'my_df' in the ipython shell.
        assert 'my_df' not in ipython_shell.user_ns
