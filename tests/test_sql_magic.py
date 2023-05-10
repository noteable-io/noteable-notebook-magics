""" Tests over the data loading magic, "create_or_replace_data_view" """

import os
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest
import requests

from noteable import datasources
from tests.conftest import COCKROACH_HANDLE, DatasourceJSONs


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

    def test_select_no_rows_from_table_produces_zero_row_dataframe_with_expected_columns(
        self, sql_magic
    ):
        # Will match 0 rows, but should be returned as 0-row three-column'd dataframe.
        results = sql_magic.execute('@sqlite select a, b, c from int_table where a = -12')

        assert isinstance(results, pd.DataFrame)
        assert len(results) == 0
        assert results.columns.tolist() == ['a', 'b', 'c']

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
        from noteable.sql.connection import Connection, UnknownConnectionError

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


@pytest.mark.usefixtures("populated_cockroach_database", "populated_sqlite_database")
class TestDDLStatements:
    @pytest.mark.parametrize('conn_name', ['@sqlite', COCKROACH_HANDLE])
    def test_ddl_lifecycle(self, conn_name: str, sql_magic, capsys):
        table_name = f'test_table_{uuid4().hex}'

        r = sql_magic.execute(
            f'{conn_name}\ncreate table {table_name}(id int not null primary key, name text not null)'
        )
        # Will have printed 'Done.' to stdout.
        assert r is None  # No concrete results back from SQLA on a CREATE TABLE statement.

        r = sql_magic.execute(
            f"{conn_name}\ninsert into {table_name} (id, name) values (1, 'billy'), (2, 'bob')"
        )
        # Returns the count of rows affected as a scalar.
        assert r == 2
        # Will also have printed out the rowcount to stdout.

        r = sql_magic.execute(f"{conn_name}\ndelete from {table_name} where name = 'billy'")
        # Just one row affected here, and printed to stdout
        assert r == 1

        captured = capsys.readouterr()
        assert captured.out == 'Done.\n2 rows affected.\n1 row affected.\n'

    @pytest.mark.parametrize('conn_name', [COCKROACH_HANDLE])
    def test_insert_returning_returns_dataframe(self, conn_name: str, sql_magic):
        table_name = f'test_table_{uuid4().hex}'

        sql_magic.execute(
            f'{conn_name}\ncreate table {table_name}(id serial not null primary key, name text not null)'
        )

        r = sql_magic.execute(
            f"{conn_name}\ninsert into {table_name} (name) values ('billy'), ('bob') returning id"
        )

        assert isinstance(r, pd.DataFrame)
        assert r.columns.tolist() == ['id']
        assert all(isinstance(idval, int) for idval in r['id'].tolist())


@pytest.mark.usefixtures("populated_cockroach_database")
class TestJinjaTemplatesWithinSqlMagic:
    """Tests over jinjasql integration. See https://github.com/sripathikrishnan/jinjasql"""

    # 1) We test against cockroachdb and not sqlite because the underlying psycopg2 driver
    # will be more finicky about types coming out of jinja-ized queries.

    # 2) We don't use f-strings to interpolate COCKROACH_HANDLE because the subsequent
    # jinja syntax is incompatible with f-strings (both use '{}' incompatibly)

    @pytest.mark.parametrize('a_value,expected_b_value', [(1, 2), (4, 5)])
    def test_basic_query(self, sql_magic, ipython_shell, a_value, expected_b_value):
        """Test simple template expansion"""

        # Each a value corresponds with a different b value, see
        # populate_database() in tests/conftest.py.
        ipython_shell.user_ns['a_value'] = a_value

        ## jinjasql expansion!
        results = sql_magic.execute(
            COCKROACH_HANDLE + '\n#scalar select b from int_table where a = {{a_value}}'
        )
        # Single row + column == scalar, as from populated_sqlite_database
        assert isinstance(results, int)
        assert results == expected_b_value

    def test_query_dereferencing_object_attribute(self, sql_magic, ipython_shell):
        class MyClass:
            id: int

            def __init__(self, id: int):
                self.id = id

        mc = MyClass(1)

        ipython_shell.user_ns['mc'] = mc

        ## jinjasql expansion including object attribute dereference!
        results = sql_magic.execute(
            COCKROACH_HANDLE + '\n#scalar select b from int_table where a = {{mc.id}}'
        )

        assert isinstance(results, int)
        assert results == 2

    def test_insert_into(self, sql_magic, ipython_shell, capsys):
        """Create a one-off table, then test inserting into from python variables interpolated by jinja"""

        sql_magic.execute(
            COCKROACH_HANDLE + '\n create table scratch(id int not null primary key, name text)'
        )

        name_value = 'Joe'

        ipython_shell.user_ns['id_value'] = 12
        ipython_shell.user_ns['name_value'] = name_value

        # Insert into table via Jinja expansion.
        sql_magic.execute(
            COCKROACH_HANDLE
            + '\ninsert into scratch(id, name) values ({{id_value}}, {{name_value}})'
        )

        captured = capsys.readouterr()
        assert '1 row affected' in captured.out

        # Now expect the row to be present and we can grab the inserted name
        # back by interpolated id query.
        from_sql = sql_magic.execute(
            COCKROACH_HANDLE + '\n #scalar select name from scratch where id = {{id_value}}'
        )

        assert isinstance(from_sql, str)
        assert from_sql == name_value

        # Explicit cleanup -- fixture-level cleanup seems to block indefinitely
        sql_magic.execute(COCKROACH_HANDLE + '\n drop table scratch')

    def test_in_query_template(self, sql_magic, ipython_shell):
        """Test an in clause expanded from a list. Requires '| inclause' formatter"""
        ipython_shell.user_ns['a_values'] = [1, 4]  # both known a values.
        results = sql_magic.execute(
            COCKROACH_HANDLE
            + ' select b from int_table where a in {{a_values | inclause}} order by b'
        )

        assert len(results) == 2
        assert results['b'].tolist() == [2, 5]

    def test_against_string(self, sql_magic, ipython_shell):
        """Test string params"""
        ipython_shell.user_ns['str_id_val'] = 'a'

        results = sql_magic.execute(
            COCKROACH_HANDLE
            + '\n#scalar select int_col from str_table where str_id = {{str_id_val}}'
        )

        # Scalar result.
        assert results == 2

    @pytest.mark.parametrize('ret_col,expected_value', [('a', 1), ('b', 2)])
    def test_sqlsafe(self, sql_magic, ipython_shell, ret_col, expected_value):
        """Test template that gets projected column name via jinja2. Requires '|sqlsafe' formatter"""
        ipython_shell.user_ns['ret_col'] = ret_col

        results = sql_magic.execute(
            COCKROACH_HANDLE + '\n#scalar select {{ret_col | sqlsafe}} from int_table where a=1'
        )

        # Scalar result.
        assert results == expected_value

    @pytest.mark.parametrize('do_filter,expected_values', [(True, 2), (False, [2, 5])])
    def test_conditional_filter(self, sql_magic, ipython_shell, do_filter, expected_values):
        """Test jijna conditional in the template"""
        ipython_shell.user_ns['do_filter'] = do_filter

        results = sql_magic.execute(
            COCKROACH_HANDLE
            + '\n#scalar select b from int_table where true {%if do_filter%} and a=1 {% endif %} order by a'
        )

        if isinstance(expected_values, int):
            # returned just a scalar
            assert results == expected_values
        else:
            # multi-rows comes wrapped in dataframe
            assert results['b'].tolist() == expected_values


@pytest.fixture
def tests_fixture_data() -> Path:
    """Return Path pointing to tests/fixture_data/ dir"""
    return Path(__file__).parent / 'fixture_data'


class TestSQLite:
    """Integration test cases of bootstrapping through to using SQLite datasource type datasource"""

    @pytest.mark.parametrize('memory_spelling', ('', ':memory:'))
    def test_success_against_memory_only_database(self, sql_magic, datasource_id, memory_spelling):
        """Test can bootstrap and use against memory, using either empty string or :memory: spellings."""

        self.bootstrap(datasource_id, memory_spelling)

        results = sql_magic.execute(f'@{datasource_id}\n#scalar\nselect 1+2')

        assert results == 3

    def test_success_simulated_loading_database_from_figshare(
        self, sql_magic, tests_fixture_data: Path, datasource_id: str, requests_mock, log_capture
    ):
        """Test 'downloading' the database, expecting to find some species in there!"""

        # Simulate successful download from 'https://figshare.com/ndownloader/files/11188550'
        # We gots the canned file from that URL in `tests/fixture_data/portal_mammals.sqlite`.

        mammals_url = 'mock://mammals_database/'
        with open(tests_fixture_data / 'portal_mammals.sqlite', 'rb') as response_file:
            # Set up response for a GET to that URL to return the contents of our canned copy.
            requests_mock.get(mammals_url, body=response_file)

            # Bootstrap the datasource to 'download' this data file.
            with log_capture() as logs:
                self.bootstrap(datasource_id, mammals_url)

            assert logs[0]['event'] == 'Downloading sqlite database initial contents'
            assert logs[0]['database_url'] == mammals_url
            assert logs[0]['max_download_seconds'] == 10  # The default when unspecified.

        results = sql_magic.execute(f'@{datasource_id} #scalar select count(*) from species')

        # There oughta be rows in that species table!
        assert results == 54

    def test_success_simulated_loading_database_from_figshare_nondefault_max_timeout(
        self, sql_magic, tests_fixture_data: Path, datasource_id: str, requests_mock, log_capture
    ):
        """Test 'downloading' the database with nondefault max timeout."""

        mammals_url = 'mock://mammals_database/'
        with open(tests_fixture_data / 'portal_mammals.sqlite', 'rb') as response_file:
            # Set up response for a GET to that URL to return the contents of our canned copy.
            requests_mock.get(mammals_url, body=response_file)

            # Bootstrap the datasource to 'download' this data file.
            with log_capture() as logs:
                self.bootstrap(datasource_id, mammals_url, max_download_seconds=22)

            assert logs[0]['event'] == 'Downloading sqlite database initial contents'
            assert logs[0]['database_url'] == mammals_url
            assert logs[0]['max_download_seconds'] == 22  # Explicitly specified.

        results = sql_magic.execute(f'@{datasource_id} #scalar select count(*) from species')

        # There oughta be rows in that species table!
        assert results == 54

    # Works great, but not for CICD use.
    '''
    def test_succcess_actually_loading_database_from_figshare(self, sql_magic, datasource_id: str):
        """Test downloading the database, expecting to find some species in there!"""

        mammals_url = 'https://figshare.com/ndownloader/files/11188550'

        # Bootstrap the datasource to download this data file.
        self.bootstrap(datasource_id, mammals_url)

        results = sql_magic.execute(f'@{datasource_id} #scalar select count(*) from species')

        # There oughta be rows in that species table!
        assert results == 54
    '''

    @pytest.mark.parametrize(
        'exc,expected_substring',
        [
            (requests.exceptions.Timeout("Read timed out."), 'Read timed out'),
            (requests.exceptions.ConnectTimeout('Connect timed out.'), 'Connect timed out'),
            (
                requests.exceptions.HTTPError(
                    '404 Client Error: Not Found for url: mock://failed.download/'
                ),
                '404 Client Error',
            ),
        ],
    )
    def test_failing_download(
        self, sql_magic, datasource_id, capsys, requests_mock, exc, expected_substring
    ):
        failing_url = 'mock://failed.download/'

        # Set up to simulate exception coming up while making requests.get() call to try to
        # download the seed database.
        requests_mock.get(
            failing_url,
            exc=exc,
        )

        self.bootstrap(datasource_id, failing_url)

        results = sql_magic.execute(f'@{datasource_id} #scalar select count(*) from species')

        assert results is None

        captured = capsys.readouterr()

        assert 'Please check data connection configuration' in captured.err
        assert expected_substring in captured.err

    @pytest.mark.parametrize('bad_path', ['/usr/bin/bash', 'relative_project_file.sqlite'])
    def test_fail_bad_pathname(self, sql_magic, datasource_id, bad_path, capsys):
        """Test providing local database pathname, but in disallowed place."""

        # Not a legal local file -- isn't in a tmp-y place.
        self.bootstrap(datasource_id, bad_path)

        # Simulate use in a SQL cell ...
        results = sql_magic.execute(f'@{datasource_id}\nselect true')

        assert results is None

        captured = capsys.readouterr()

        assert 'Please check data connection configuration' in captured.err
        assert (
            f'SQLite database files should be located within /tmp, got "{bad_path}"' in captured.err
        )

    def bootstrap(
        self, datasource_id: str, database_path_or_url: str, max_download_seconds: int = None
    ):
        jsons = DatasourceJSONs(
            meta_dict={
                'required_python_modules': [],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'sqlite',
                'sqlmagic_autocommit': False,
                'name': f'Test Suite SQLite Datasource {datasource_id}',
            },
            dsn_dict={
                'database': database_path_or_url,
            },
        )

        if max_download_seconds:
            jsons.connect_args_dict = {'max_download_seconds': max_download_seconds}

        datasources.bootstrap_datasource(
            datasource_id, jsons.meta_json, jsons.dsn_json, jsons.connect_args_json
        )


class TestAmazonAthena:
    """These tests will only work if the following env vars are set (see the fixture below). Otherwise the
    fixture will skip the test (i.e. CICD)

    But James has locally set all these according to his AWS account + test Athena setup, and it actually works.

    This is technically an integration test against external services needing secrets, whose services cost money,
    so won't run in CICD.
    """

    @pytest.fixture
    def athena_datasource_handle(self, datasource_id):
        """Bootstrap AWS Athena account / database that should have the AWS example 'cloudfront_logs' table configured
        from environment variables. Returns the datasource id / handle.

        If any of these env vars are not set (Github CICD), then the test(s) will be skipped.
        """
        conn_info = (
            os.environ.get('AWSATHENA_KEY_ID'),
            os.environ.get('AWSATHENA_KEY_SECRET'),
            os.environ.get('AWSATHENA_REGION'),
            os.environ.get('AWSATHENA_DATABASE'),
            os.environ.get('AWSATHENA_S3_STAGING_DIR'),
        )

        if not all(conn_info):
            pytest.skip(
                'Required environment variables not set to run AWS Athena connectivity tests'
            )

        # As if these came from Gate / Vault.
        jsons = DatasourceJSONs(
            meta_dict={
                'required_python_modules': ["PyAthena[SQLAlchemy]"],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'awsathena+rest',
                'sqlmagic_autocommit': False,
                'name': 'My AWS Athena',
            },
            dsn_dict={
                'host': conn_info[2],
                'username': conn_info[0],
                'password': conn_info[1],
                'database': conn_info[3],
            },
            connect_args_dict={'s3_staging_dir': conn_info[4]},
        )

        # Should get postprocessed properly by postprocess_awsathena(), otherwise test
        # will definitely fail.
        datasources.bootstrap_datasource(
            datasource_id, jsons.meta_json, jsons.dsn_json, jsons.connect_args_json
        )

        return datasource_id

    def test_select(self, athena_datasource_handle, sql_magic):
        # Run 2x to ensure that we won't have autocommit issues.

        for _ in range(2):
            results = sql_magic.execute(
                f'@{athena_datasource_handle} #scalar select count(*) from cloudfront_logs'
            )

            assert results == 4996
