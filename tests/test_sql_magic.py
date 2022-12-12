""" Tests over the data loading magic, "create_or_replace_data_view" """


import pandas as pd
import pytest

from noteable_magics import datasources
from tests.conftest import DatasourceJSONs


@pytest.mark.usefixtures("populated_sqlite_database")
class TestSqlMagic:
    @pytest.mark.parametrize(
        'invocation',
        [
            '@sqlite select a, b from int_table',  # as from line magic invocation
            '@sqlite\nselect a, b\nfrom int_table',  # as from cell magic / Planar Ally
        ],
    )
    def test_basic_query(self, invocation, sql_magic, ipython_shell):
        """Test basic query behavior"""

        results = sql_magic.execute(invocation)
        assert isinstance(results, pd.DataFrame)

        # Two rows as from populated_sqlite_database
        assert len(results) == 2
        assert results['a'].tolist() == [1, 4]
        assert results['b'].tolist() == [2, 5]

    @pytest.mark.parametrize(
        'invocation',
        [
            '@sqlite #scalar select 1 + 2',  # as from line magic invocation
            '@sqlite\n#scalar select 1 + 2',  # as from cell magic / Planar Ally
        ],
    )
    def test_returning_scalar_when_requested_and_single_value_resultset(
        self, invocation, sql_magic, ipython_shell
    ):
        """Should return bare scalar when result set was single row/column and asked"""
        results = sql_magic.execute(invocation)
        assert isinstance(results, int)
        assert results == 3

    @pytest.mark.parametrize(
        'invocation',
        [
            '@sqlite #scalar select 1 as a, 2 as b',  # as from line magic invocation
            '@sqlite\n#scalar select 1 as a, 2 as b',  # as from cell magic / Planar Ally
        ],
    )
    def test_dataframe_returned_if_nonscalar_result_despite_asking_for_scalar(
        self, invocation, sql_magic, ipython_shell
    ):
        """Despite asking for scalar, if result is dataframe that's what you get"""

        # Multiple columns.
        results = sql_magic.execute(invocation)
        assert isinstance(results, pd.DataFrame)
        assert len(results) == 1

        assert results['a'].tolist() == [1]
        assert results['b'].tolist() == [2]

        # Likewise multiple rows, single column.
        results = sql_magic.execute('@sqlite #scalar select a from int_table')
        assert isinstance(results, pd.DataFrame)

    @pytest.mark.parametrize(
        'invocation',
        [
            '@sqlite the_sum << #scalar select 1 + 2',  # as from line magic invocation
            '@sqlite the_sum <<\n#scalar select 1 + 2',  # as from cell magic / Planar Ally
        ],
    )
    def test_returning_scalar_when_requested_and_single_value_resultset_assigns_variable(
        self, invocation, sql_magic, ipython_shell
    ):
        """Should return + assign bare scalar when result set was single row/column and asked"""
        results = sql_magic.execute(invocation)
        assert isinstance(results, int)
        assert results == 3

        # ... and also bound to var 'the_sum'
        assert ipython_shell.user_ns['the_sum'] is results

    def test_assigment_to_variable(self, sql_magic, ipython_shell):
        """Test that when the 'varname << select ...' syntax is used, the df is returned
        as the main execute result, and varname is side-effect assigned to."""

        results = sql_magic.execute('@sqlite my_df <<\nselect a from int_table')
        assert isinstance(results, pd.DataFrame)

        # Two rows as from populated_sqlite_database
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
        results = sql_magic.execute('@sqlite my_df <<\nselect a from nonexistent_table')
        assert results is None

        # Should NOT have also assigned the exception to global 'my_df' in the ipython shell.
        assert 'my_df' not in ipython_shell.user_ns

    def test_unknown_datasource_handle_produces_expected_exception(self, sql_magic, capsys):
        from noteable_magics.sql.connection import Connection, UnknownConnectionError

        initial_connection_count = len(Connection.connections)
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
        assert captured.err == f"{expected_message}\n"

        # Finally, the total number of known connections should have remained the same.
        assert len(Connection.connections) == initial_connection_count


@pytest.mark.usefixtures("populated_sqlite_database")
class TestJinjaTemplatesWithinSqlMagic:
    """Tests over jinjasql integration. See https://github.com/sripathikrishnan/jinjasql"""

    @pytest.mark.parametrize('a_value,expected_b_value', [(1, 2), (4, 5)])
    def test_basic_query(self, sql_magic, ipython_shell, a_value, expected_b_value):
        """Test simple template expansion"""

        # Each a value corresponds with a different b value, see
        # populated_sqlite_database().
        ipython_shell.user_ns['a_value'] = a_value

        ## jinjasql expansion!
        results = sql_magic.execute(
            '@sqlite\n#scalar select b from int_table where a = {{a_value}}'
        )
        # Single row + column == scalar, as from populated_sqlite_database
        assert isinstance(results, int)
        assert results == expected_b_value

    def test_in_query_template(self, sql_magic, ipython_shell):
        """Test an in clause expanded from a list. Requires '| inclause' formatter"""
        ipython_shell.user_ns['a_values'] = [1, 4]  # both known a values.
        results = sql_magic.execute(
            '@sqlite select b from int_table where a in {{a_values | inclause}} order by b'
        )

        assert len(results) == 2
        assert results['b'].tolist() == [2, 5]

    def test_against_string(self, sql_magic, ipython_shell):
        """Test string params"""
        ipython_shell.user_ns['str_id_val'] = 'a'

        results = sql_magic.execute(
            '@sqlite\n#scalar select int_col from str_table where str_id = {{str_id_val}}'
        )

        # Scalar result.
        assert results == 1

    @pytest.mark.parametrize('ret_col,expected_value', [('a', 1), ('b', 2)])
    def test_sqlsafe(self, sql_magic, ipython_shell, ret_col, expected_value):
        """Test template that gets projected column name via jinja2. Requires '|sqlsafe' formatter"""
        ipython_shell.user_ns['ret_col'] = ret_col

        results = sql_magic.execute(
            '@sqlite\n#scalar select {{ret_col | sqlsafe}} from int_table where a=1'
        )

        # Scalar result.
        assert results == expected_value

    @pytest.mark.parametrize('do_filter,expected_values', [(True, 2), (False, [2, 5])])
    def test_conditional_filter(self, sql_magic, ipython_shell, do_filter, expected_values):
        """Test jijna conditional in the template"""
        ipython_shell.user_ns['do_filter'] = do_filter

        results = sql_magic.execute(
            '@sqlite\n#scalar select b from int_table where true {%if do_filter%} and a=1 {% endif %} order by a'
        )

        if isinstance(expected_values, int):
            # returned just a scalar
            assert results == expected_values
        else:
            # multi-rows comes wrapped in dataframe
            assert results['b'].tolist() == expected_values


def test_trying_to_use_broken_bootstrapped_connection(sql_magic, datasource_id, capsys):
    """ """

    bad_sqlite = DatasourceJSONs(
        meta_dict={
            'required_python_modules': [],
            'allow_datasource_dialect_autoinstall': False,
            'drivername': 'sqlite',
            'sqlmagic_autocommit': False,
            'name': 'Broken at bootstrapping SQLite',
        },
        dsn_dict={
            # Bad / illegal path here.
            'database': "/usr/bin/bash",
        },
    )

    # Will fail bootstrapping and cache failure message for referencing when
    # the connection is tried to be used in a SQL cell.
    datasources.bootstrap_datasource(
        datasource_id, bad_sqlite.meta_json, bad_sqlite.dsn_json, bad_sqlite.connect_args_json
    )

    # Simulate use in a SQL cell ...
    results = sql_magic.execute(f'@{datasource_id}\nselect true')

    assert results is None

    captured = capsys.readouterr()

    assert 'Please check data connection configuration' in captured.err
    assert (
        'SQLite database files should be located within either the project or in /tmp, got "/usr/bin/bash"'
        in captured.err
    )
