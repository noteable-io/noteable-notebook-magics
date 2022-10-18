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

    db.execute('create table str_table(str_id text, int_col int)')
    db.execute(
        "insert into str_table(str_id, int_col) values ('a', 1), ('b', 2), ('c', 3), ('d', null)"
    )


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

    def test_unknown_datasource_handle_produces_expected_exception(self, sql_magic, capsys):
        from noteable_magics.sql.connection import Connection, UnknownConnectionError

        # sql magic invocation of an unknown connection will end up calling .set() with
        # that unknown connection's handle. Should raise. (This is unit-test-y)
        # Verbiage from ENG-4264.
        expected_message = "Cannot find data connection. If you recently created this connection, please restart the kernel."

        with pytest.raises(
            UnknownConnectionError,
            match=expected_message,
        ):
            Connection.set('@45645675', False)

        # ... and when run through the magic, the magic will return None, but print the message out as
        # the cell's output. (This is more integration test-y, or at least higher-level unit-test-y.)
        assert sql_magic.execute('@45645675 select true') is None
        captured = capsys.readouterr()
        assert captured.out == f"{expected_message}\n"

        # Finally, the total number of known connections should have remained 1 (@foo).
        assert len(Connection.connections) == 1


@pytest.mark.usefixtures("populated_foo_database")
class TestJinjaTemplatesWithinSqlMagic:
    """Tests over jinjasql integration. See https://github.com/sripathikrishnan/jinjasql"""

    @pytest.mark.parametrize('a_value,expected_b_value', [(1, 2), (4, 5)])
    def test_basic_query(self, sql_magic, ipython_shell, a_value, expected_b_value):
        """Test simple template expansion"""

        # Each a value corresponds with a different b value, see
        # populated_foo_database().
        ipython_shell.user_ns['a_value'] = a_value

        ## jinjasql expansion!
        results = sql_magic.execute('@foo select b from int_table where a = {{a_value}}')
        assert isinstance(results, pd.DataFrame)

        # One row as from populated_foo_database
        assert len(results) == 1
        assert results['b'].tolist() == [expected_b_value]

    def test_in_query_template(self, sql_magic, ipython_shell):
        """Test an in clause expanded from a list. Requires '| inclause' formatter"""
        ipython_shell.user_ns['a_values'] = [1, 4]  # both known a values.
        results = sql_magic.execute(
            '@foo select b from int_table where a in {{a_values | inclause}} order by b'
        )

        assert len(results) == 2
        assert results['b'].tolist() == [2, 5]

    def test_against_string(self, sql_magic, ipython_shell):
        """Test string params"""
        ipython_shell.user_ns['str_id_val'] = 'a'

        results = sql_magic.execute(
            '@foo select int_col from str_table where str_id = {{str_id_val}}'
        )

        assert len(results) == 1
        assert results['int_col'].tolist() == [1]

    @pytest.mark.parametrize('ret_col,expected_value', [('a', 1), ('b', 2)])
    def test_sqlsafe(self, sql_magic, ipython_shell, ret_col, expected_value):
        """Test template that gets projected column name via jinja2. Requires '|sqlsafe' formatter"""
        ipython_shell.user_ns['ret_col'] = ret_col

        results = sql_magic.execute('@foo select {{ret_col | sqlsafe}} from int_table where a=1')

        assert len(results) == 1
        assert results[ret_col].tolist() == [expected_value]

    @pytest.mark.parametrize('do_filter,expected_values', [(True, [2]), (False, [2, 5])])
    def test_conditional_filter(self, sql_magic, ipython_shell, do_filter, expected_values):
        """Test jijna conditional in the template"""
        ipython_shell.user_ns['do_filter'] = do_filter

        results = sql_magic.execute(
            '@foo select b from int_table where true {%if do_filter%} and a=1 {% endif %} order by a'
        )

        assert results['b'].tolist() == expected_values
