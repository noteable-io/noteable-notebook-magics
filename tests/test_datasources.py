""" Tests over datasource bootstrapping """

import json
from pathlib import Path
from typing import Callable, List
from uuid import uuid4

import pkg_resources
import pytest
import structlog
from structlog.testing import LogCapture

from noteable_magics import datasource_postprocessing, datasources
from noteable_magics.logging import configure_logging
from noteable_magics.sql.connection import Connection
from noteable_magics.sql.run import _COMMIT_BLACKLIST_DIALECTS
from tests.conftest import DatasourceJSONs


@pytest.fixture
def log_output() -> LogCapture:
    configure_logging(True, 'INFO', 'DEBUG')
    capturer = LogCapture()
    structlog.configure(processors=[capturer])
    return capturer


@pytest.fixture
def not_installed_packages() -> List[str]:
    """Yield a few not currently installed packages, uninstall them as needed
    upon cleanup.
    """

    # Not even kidding there's a left-pad for python.
    pkgnames = ['orjson', 'left-pad']

    for pkgname in pkgnames:
        if datasources.is_package_installed(pkgname):
            datasources.run_pip(['uninstall', '-y', pkgname])

    # Get pkg_resources to forget it existed too.
    pkg_resources.working_set.by_key.clear()

    yield pkgnames


@pytest.fixture
def not_installed_package(not_installed_packages: List[str]) -> str:
    """Yield a package name that is definitely not currently installed, then uninstall
    it upon cleanup if needed.
    """
    yield not_installed_packages[0]


@pytest.fixture
def datasource_id_factory() -> Callable[[], str]:
    def factory_datasource_id():
        return uuid4().hex

    return factory_datasource_id


@pytest.fixture
def datasource_id(datasource_id_factory) -> str:
    return datasource_id_factory()


class SampleData:
    """Test case fodder"""

    samples = {
        'simple-cockroachdb': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['sqlalchemy-cockroachdb', 'psycopg2-binary'],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'cockroachdb',
                'sqlmagic_autocommit': False,
                'name': 'My CRDB',
            },
            dsn_dict={
                'username': 'scott',
                'password': 'tiger',
                'host': 'localhost',
                'port': 26257,
                'database': 'defaultdb',
            },
        ),
        'simple-postgres': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['psycopg2-binary'],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'postgresql',
                'sqlmagic_autocommit': True,
                'name': 'My PostgreSQL',
            },
            dsn_dict={
                'username': 'scott',
                'password': 'tiger',
                'host': 'localhost',
                'port': 5432,
                'database': 'postgres',
            },
        ),
        'postgres-require-ssl': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['psycopg2-binary'],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'postgresql',
                'sqlmagic_autocommit': True,
                'name': 'My PostgreSQL SSL',
            },
            dsn_dict={
                'username': 'scott',
                'password': 'tiger',
                'host': 'localhost',
                'port': 5432,
                'database': 'postgres',
            },
            connect_args_dict={'sslmode': 'require'},
        ),
        'redshift': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['sqlalchemy-redshift', 'redshift_connector'],
                # Packages installed already in noteable-notebook-magics
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'redshift+redshift_connector',
                'sqlmagic_autocommit': True,
                'name': 'My RedShift',
            },
            dsn_dict={
                'username': 'scott',
                'password': 'tiger',
                'host': 'localhost',
                'port': 5439,
                'database': 'postgres',
            },
        ),
        'trino': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['trino[sqlalchemy]'],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'trino',
                'sqlmagic_autocommit': False,  # This one is special!
                # And explicitly no name assigned, 'legacy'.
            },
            dsn_dict={
                'username': 'ssm-user',
                'host': 'vpce-049c85e5d9fef046c-ygwkxhb9.vpce-svc-04e6878855b5a4174.us-west-2.vpce.amazonaws.com',
                'port': 18889,
                'database': 'hive',
            },
        ),
        'databricks': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['sqlalchemy-databricks'],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'databricks+connector',
                'sqlmagic_autocommit': False,  # This one is special!
                'name': 'My Databricks',
            },
            dsn_dict={
                'username': 'token',
                'host': 'dbc-1bab80fc-a74b.cloud.databricks.com","password":"dapie372d57cefdc078d8ce3936fcb0e22ee',
                'password': 'foonlybar',
            },
            connect_args_dict={
                "http_path": "sql/protocolv1/o/2414094324684936/0125-220758-m9pfb4c7"
            },
        ),
        ##
        # Looking for BigQuery?
        # Bigquery tested down in test_bigquery_particulars
        ##
        'snowflake-required': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['snowflake-sqlalchemy'],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'snowflake',
                'sqlmagic_autocommit': True,
                'name': 'My Snowflake',
            },
            dsn_dict={
                'username': 'brittle-snowflake',
                'password': 'sdfsdf',
                'host': 'sdfsfetr.us-east-1',
            },
            connect_args_dict={
                'warehouse': 'xxxxxxxxlarge',
            },
        ),
        'snowflake-with-database': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['snowflake-sqlalchemy'],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'snowflake',
                'sqlmagic_autocommit': True,
                'name': 'My Snowflake with database',
            },
            dsn_dict={
                'username': 'brittle-snowflake',
                'password': 'sdfsdf',
                'host': 'sdfsfetr.us-east-1',
                'database': 'mydb',
            },
            connect_args_dict={
                'warehouse': 'xxxxxxxxlarge',
            },
        ),
        'snowflake-with-database-and-schema': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['snowflake-sqlalchemy'],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'snowflake',
                'sqlmagic_autocommit': True,
                'name': 'Snowflake with database and schema',
            },
            dsn_dict={
                'username': 'brittle-snowflake',
                'password': 'sdfsdf',
                'host': 'sdfsfetr.us-east-1',
                'database': 'mydb',
                'schema': 'my_schema',
            },
            connect_args_dict={
                'warehouse': 'xxxxxxxxlarge',
            },
        ),
        'snowflake-with-empty-db-and-schema': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['snowflake-sqlalchemy'],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'snowflake',
                'sqlmagic_autocommit': True,
                'name': 'Snowflake with empty db and schema',
            },
            dsn_dict={
                'username': 'brittle-snowflake',
                'password': 'sdfsdf',
                'host': 'sdfsfetr.us-east-1',
                'database': '',
                'schema': '',
            },
            connect_args_dict={
                'warehouse': 'xxxxxxxxlarge',
            },
        ),
        'explicit-memory-sqlite': DatasourceJSONs(
            meta_dict={
                'required_python_modules': [],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'sqlite',
                'sqlmagic_autocommit': False,
                'name': 'Memory SQLite',
            },
            dsn_dict={
                'database': ':memory:',
            },
        ),
        'implicit-memory-sqlite': DatasourceJSONs(
            meta_dict={
                'required_python_modules': [],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'sqlite',
                'sqlmagic_autocommit': False,
                'name': 'Memory SQLite',
            },
            dsn_dict={
                # Empty database file also ends up with memory-based database.
                'database': '',
            },
        ),
        'file-sqlite': DatasourceJSONs(
            meta_dict={
                'required_python_modules': [],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'sqlite',
                'sqlmagic_autocommit': False,
                'name': 'Memory SQLite',
            },
            dsn_dict={
                'database': 'local_project_file.db',
            },
        ),
    }

    @classmethod
    def get_sample(cls, name: str) -> DatasourceJSONs:
        return cls.samples[name]

    @classmethod
    def all_sample_names(cls) -> List[str]:
        # Sorted so that if tests are run in parallel test discovery is stable.
        return sorted(cls.samples.keys())

    @classmethod
    def all_samples(cls) -> List[DatasourceJSONs]:
        return list(cls.samples.values())


class TestBootstrapDatasources:
    def test_bootstrap_datasources(self, datasource_id_factory, tmp_path: Path):
        """Test that we bootstrap all of our samples from json files properly."""
        id_and_samples = [(datasource_id_factory(), sample) for sample in SampleData.all_samples()]

        # Scribble them all out into tmpdir as if on kernel launch
        for ds_id, sample in id_and_samples:
            sample.json_to_tmpdir(ds_id, tmp_path)

        # Ensure ipython-sql's little mind is clear and will be focused
        # on just this task.
        Connection.connections.clear()

        datasources.bootstrap_datasources(tmp_path)

        # Should now have len(id_and_samples) connections in there!
        assert len(Connection.connections) == len(id_and_samples)

        # (Let test TestBootstrapDatasource focus on the finer-grained details)


class TestBootstrapDatasource:
    @pytest.mark.parametrize('sample_name', SampleData.all_sample_names())
    def test_success(self, sample_name, datasource_id):

        # Clear out connections at the onset, else the get_engine() portion
        # gets confused over human-name conflicts when we've bootstrapped
        # the same sample data repeatedly, namely through having first
        # run TestBootstrapDatasources.test_bootstrap_datasources().
        Connection.connections = {}

        case_data = SampleData.get_sample(sample_name)

        datasources.bootstrap_datasource(
            datasource_id, case_data.meta_json, case_data.dsn_json, case_data.connect_args_json
        )

        # Check over the created 'Connection' instance.

        # Alas, in Connection parlance, 'name' == 'sql_cell_handle', and 'human_name'
        # is the human-assigned name for the datasource. Sigh.
        expected_sql_cell_handle_name = f'@{datasource_id}'
        the_conn = Connection.connections[expected_sql_cell_handle_name]
        assert the_conn.name == expected_sql_cell_handle_name
        # Might be None in the meta_json.
        expected_human_name = case_data.meta_dict.get('name')
        assert the_conn.human_name == expected_human_name

        # Test Connection.get_engine() while here,
        assert the_conn._engine is Connection.get_engine(expected_sql_cell_handle_name)
        if expected_human_name:
            # Can only work this way also if the datasource name was present in meta-json.
            assert the_conn._engine is Connection.get_engine(expected_human_name)

        # Ensure the required packages are installed -- excercies either the 'auto-installation'
        # code useful when trying out new datasource types in integration, or having been
        # already installed because is listed as a dependency here in noteable-notebook-magics requirements.
        expected_packages = case_data.meta_dict['required_python_modules']
        pkg_to_installed = {
            pkg_name: datasources.is_package_installed(pkg_name) for pkg_name in expected_packages
        }

        assert all(
            pkg_to_installed.values()
        ), f'Not all packages smell installed! {pkg_to_installed}'

        # If case_data.meta_dict['sqlmagic_autocommit'] is False, then expect to see the dialect portion of
        # drivername mentioned in ipython-sql's _COMMIT_BLACKLIST_DIALECTS set.
        dialect = case_data.meta_dict['drivername'].split('+')[0]
        assert (dialect in _COMMIT_BLACKLIST_DIALECTS) == (
            not case_data.meta_dict['sqlmagic_autocommit']
        )

    def test_broken_postgres_is_silent_noop(self, datasource_id, log_output):
        case_data = DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['psycopg2-binary'],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'postgresql',
                'sqlmagic_autocommit': True,
                'name': 'My PostgreSQL',
            },
            dsn_dict={
                'username': 'scott',
                'password': 'tiger',
                'host': 'https://bogus.org',  # is a URL fragment, not a host name!
                'port': 5432,
                'database': 'postgres',
            },
        )

        initial_len = len(Connection.connections)

        # Trying to bootstrap this one will fail somewhat silently -- will log exception, but
        # ultimately not having added new entry into Connection.connections.
        datasources.bootstrap_datasource(
            datasource_id, case_data.meta_json, case_data.dsn_json, case_data.connect_args_json
        )

        assert len(Connection.connections) == initial_len

        assert len(log_output.entries) == 2

        e1 = log_output.entries[0]
        assert e1['event'] == 'Error creating new noteable_magics.sql.Connection'
        assert e1['connect_str'] == 'postgresql://scott:tiger@[https://bogus.org]:5432/postgres'

        e2 = log_output.entries[1]
        assert e2['event'] == 'Unable to bootstrap datasource'
        assert e2['datasource_id'] == datasource_id

    def test_bigquery_particulars(self, datasource_id, log_output):
        """Ensure that we convert connect_args['credential_file_contents'] to
        become its own file, and (indirectly) that we promote all elements in
        connect_args to be toplevel create_engine_kwargs
        """

        # Not a general sample in the main list because with these credential_file_contents,
        # the call to create_engine ultimately fails because isn't a real google cred file.
        # That's fine though, because the nature of the exception is such that we know we
        # have it's attention properly.
        case_data = DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['sqlalchemy-bigquery'],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'bigquery',
                'sqlmagic_autocommit': True,
            },
            connect_args_dict={
                # b64 encoding of '{"foo": "bar"}'
                'credential_file_contents': 'eyJmb28iOiAiYmFyIn0='
            },
        )

        # Expect the ultimate call to sqlalchemy.create_engine() to fail softly, because
        # we're not really feeding it a legit google credentials file at this time
        # (the 'credentials_file_contents' in the sample data is really just {"foo": "bar"}).
        #
        # Had postprocess_bigquery() not done the promotion from connect_args -> create_engine_kwargs, would
        # die a very different death, complaining about cannot find any credentials anywhere
        # since not passed in and the google magic env var isn't set.

        initial_len = len(Connection.connections)

        datasources.bootstrap_datasource(
            datasource_id, case_data.meta_json, case_data.dsn_json, case_data.connect_args_json
        )

        # No successful side effect.
        assert len(Connection.connections) == initial_len

        assert len(log_output.entries) == 2

        e1 = log_output.entries[0]
        assert e1['event'] == 'Error creating new noteable_magics.sql.Connection'
        assert e1['connect_str'] == 'bigquery://'

        e2 = log_output.entries[1]
        assert e2['event'] == 'Unable to bootstrap datasource'
        assert e2['datasource_id'] == datasource_id

        # But we do expect the postprocessor to have run, and to have created this
        # file properly....

        # /tmp/{datasource_id}_bigquery_credentials.json should now exist and
        # contain '{"foo": "bar"}' due to consiracy in
        # datasource_postprocessing.postprocess_bigquery
        with open(f'/tmp/{datasource_id}_bigquery_credentials.json') as inf:
            from_json = json.load(inf)
            assert from_json == {'foo': 'bar'}

    def test_postprocess_postgresql(self, datasource_id):
        pg_details = SampleData.get_sample('simple-postgres')

        datasources.bootstrap_datasource(
            datasource_id, pg_details.meta_json, pg_details.dsn_json, pg_details.connect_args_json
        )

        # At least look for signs of the side-effect. Can't test it actually does the
        # right thing here w/o actually making a postgresql connection, firing off a query,
        # and then having another thread or whatnot deliver SIGINT while waiting for query
        # results. Or just go clicktest it in integration.

        import psycopg2.extensions
        import psycopg2.extras

        assert psycopg2.extensions.get_wait_callback() is psycopg2.extras.wait_select

    @pytest.mark.parametrize('bad_pathname', ['/dev/foo.db', './../jailbreak.db'])
    def test_had_bad_sqlite_database_files(self, datasource_id, bad_pathname: str):
        """If configured with neither a path within project nor exactly ':memory:', then
        bootstrapping should fail (currently silently w/o creating the datasource)
        """

        human_name = 'My Bad SQLite'
        bad_sqlite = DatasourceJSONs(
            meta_dict={
                'required_python_modules': [],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'sqlite',
                'sqlmagic_autocommit': False,
                'name': human_name,
            },
            dsn_dict={
                'database': bad_pathname,
            },
        )

        initial_count = len(Connection.connections)

        datasources.bootstrap_datasource(
            datasource_id, bad_sqlite.meta_json, bad_sqlite.dsn_json, bad_sqlite.connect_args_json
        )

        assert len(Connection.connections) == initial_count

        # And should have had a bootstrapping failure against both the datasource_id
        # and the human name.
        assert 'SQLite database files should be located' in Connection.get_bootstrapping_failure(
            human_name
        )
        assert 'SQLite database files should be located' in Connection.get_bootstrapping_failure(
            datasource_id
        )

        # There are test(s) over in test_sql_magic.py that prove that when such a broken datasource is
        # attempted to be used, this get_bootstrapping_failure() message will show back up, surfacing
        # the real problem to the user.


class TestEnsureRequirements:
    def test_already_installed(self, datasource_id):
        requirements = ['pip']
        assert all(datasources.is_package_installed(r) for r in requirements)

        datasources.ensure_requirements(datasource_id, requirements, False)

    def test_installs_when_allowed(self, datasource_id, not_installed_packages):
        datasources.ensure_requirements(datasource_id, not_installed_packages, True)

        # They oughta be installed now!
        assert all(datasources.is_package_installed(r) for r in not_installed_packages)

    def test_raises_when_disallowed_but_needs_to_install(
        self, datasource_id, not_installed_packages
    ):
        with pytest.raises(Exception, match='requires package .* but is not already installed'):
            datasources.ensure_requirements(datasource_id, not_installed_packages, False)


class TestInstallPackage:
    def test_install(self, not_installed_package):

        pkgname = not_installed_package

        # Better not be installed right now!
        assert not datasources.is_package_installed(pkgname)

        # Install it.
        datasources.install_package(pkgname)

        # Should smell installed now.
        assert datasources.is_package_installed(pkgname)


class TestIsPackageInstalled:
    def test_no(self):
        assert not datasources.is_package_installed('foonlybar')

    def test_yes(self):
        assert datasources.is_package_installed('pip')


@pytest.mark.parametrize(
    'test_dict,expected',
    [
        (
            # Should erode database and schema away from a snowflake-esque
            # dsn dict
            {
                'host': 'sdfsfetr.us-east-1',
                'database': '',
                'schema': '',
            },
            {'host': 'sdfsfetr.us-east-1'},
        ),
        (
            # Hypothetical nested connect_args dict
            {
                'foo': 'bar',
                'blammo': {
                    'blat': 'blarg',
                    'blummo': '',  # should disappear
                    'quux': {
                        'foo': ''
                    },  # whole subdict should disappear since sole member disappears
                },
            },
            {'foo': 'bar', 'blammo': {'blat': 'blarg'}},
        ),
    ],
)
def test_pre_process_dict(test_dict, expected):
    """Prove that pre_process_dict strips out empty string values / empty sub-dicts properly"""
    datasources.pre_process_dict(test_dict)

    assert test_dict == expected


@pytest.mark.parametrize(
    'dsn_dict,expected',
    [
        # Both should be merged, schema popped.
        ({'database': 'foo', 'schema': 'bar'}, {'database': 'foo/bar'}),
        # database only is fine.
        ({'database': 'foo'}, {'database': 'foo'}),
        # Neither are fine also.
        ({'host': 'sdfsdf'}, {'host': 'sdfsdf'}),
    ],
)
def test_postprocess_snowflake(dsn_dict, expected):
    """Prove handling of database, schema -> 'database/schema'"""

    datasource_postprocessing.postprocess_snowflake(None, dsn_dict, {})
    assert dsn_dict == expected
