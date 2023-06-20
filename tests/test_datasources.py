""" Tests over datasource bootstrapping """

import json
import os
from pathlib import Path
from typing import Callable, List, Tuple, Union
from unittest.mock import patch
from uuid import uuid4

import certifi
import pkg_resources
import pytest
import structlog
from structlog.testing import LogCapture

from noteable import datasource_postprocessing, datasources
from noteable.logging import configure_logging
from noteable.sql.connection import get_connection_registry, get_sqla_engine
from noteable.sql.run import _COMMIT_BLACKLIST_DIALECTS
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
                'required_python_modules': ['sqlalchemy-cockroachdb', 'psycopg2'],
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
                'required_python_modules': ['psycopg2'],
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
                'required_python_modules': ['psycopg2'],
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
                'name': 'My Trino',
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
                'name': 'Explicit Memory SQLite',
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
                'name': 'Implicit Memory SQLite',
            },
            dsn_dict={
                # Empty database file also ends up with memory-based database.
                'database': '',
            },
        ),
        'memory-sqlite-also-with-max_download_seconds': DatasourceJSONs(
            meta_dict={
                'required_python_modules': [],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'sqlite',
                'sqlmagic_autocommit': False,
                'name': 'Memory SQLite with max_download',
            },
            dsn_dict={
                # Empty database file also ends up with memory-based database.
                'database': '',
            },
            connect_args_dict={
                'max_download_seconds': '22',
            },
            expect_identical_connect_args=False,
        ),
        'awsathena': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ["PyAthena[SQLAlchemy]"],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'awsathena+rest',
                'sqlmagic_autocommit': False,
                'name': 'My AWS Athena',
            },
            dsn_dict={
                'host': 'us-west-1',
                'username': 'MYKEYID',
                'password': 'MyKeyValueHoHoHo',
                'database': 'default_database',
            },
            connect_args_dict={'s3_staging_dir': 's3://myamazonawsbucket/results/'},
        ),
        # Originally, we used pymysql for SingleStore, mysql, mariadb.
        # Older generation datasource data in Vault will still request to use this driver
        # and python module.
        'singlestore-with-pymysql': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ["pymysql"],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'mysql+pymysql',
                'sqlmagic_autocommit': False,
                'name': 'Old Singlestore',
            },
            dsn_dict={
                'host': 'us-west-1',
                'port': 3306,
                'username': 'myuser',
                'password': 'MyKeyValueHoHoHo',
                'database': 'default_database',
            },
        ),
        # But now we prefer the explicit singlestore dialect.
        'singlestore-with-singlestore-dialect': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ["sqlalchemy-singlestoredb"],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'singlestoredb',
                'sqlmagic_autocommit': False,
                'name': 'New Singlestore',
            },
            dsn_dict={
                'host': 'us-west-1',
                'port': 3306,
                'username': 'myuser',
                'password': 'MyKeyValueHoHoHo',
                'database': 'default_database',
            },
        ),
        # Similarly from pymysql over to mysqlclient driver for mysql/mariadb
        'mariadb-with-pymysql': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ["pymysql"],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'mysql+pymysql',
                'sqlmagic_autocommit': False,
                'name': 'Old Mariadb',
            },
            dsn_dict={
                'host': 'us-west-1',
                'port': 3306,
                'username': 'myuser',
                'password': 'MyKeyValueHoHoHo',
                'database': 'default_database',
            },
        ),
        # But now we prefer the C-based driver
        'mariadb-with-singlestore-dialect': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ["mysqlclient"],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'mysql+mysqldb',
                'sqlmagic_autocommit': False,
                'name': 'New Mariadb',
            },
            dsn_dict={
                'host': 'us-west-1',
                'port': 3306,
                'username': 'myuser',
                'password': 'MyKeyValueHoHoHo',
                'database': 'default_database',
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
        # Ensure no dupe names, otherwise hilarity.
        assert len(set(v.meta_dict['name'] for v in cls.samples.values())) == len(cls.samples)

        return list(cls.samples.values())


class TestBootstrapDatasources:
    @pytest.mark.usefixtures("with_empty_connections")
    def test_bootstrap_datasources(self, datasource_id_factory, tmp_path: Path):
        """Test that we bootstrap all of our samples from json files properly."""
        id_and_samples = [(datasource_id_factory(), sample) for sample in SampleData.all_samples()]

        # Scribble them all out into tmpdir as if on kernel launch
        for ds_id, sample in id_and_samples:
            sample.json_to_tmpdir(ds_id, tmp_path)

        datasources.bootstrap_datasources(tmp_path)

        # Should now have len(id_and_samples) connections in there!
        registry = get_connection_registry()

        # Each id and human name should have been registered.
        for ds_id, sample in id_and_samples:
            assert f'@{ds_id}' in registry
            assert sample.meta_dict['name'] in registry

            # Ensure that the Connection actually got bootstrapped with
            # the connect_args, otherwise bootstrap_datasource() messed up badly.
            if sample.connect_args_dict and sample.expect_identical_connect_args:
                con = registry.get(f'@{ds_id}')
                assert con._create_engine_kwargs == {'connect_args': sample.connect_args_dict}

        # (Let test TestBootstrapDatasource focus on the finer-grained details)


@pytest.mark.usefixtures("with_empty_connections")
class TestBootstrapDatasource:
    @pytest.mark.parametrize('sample_name', SampleData.all_sample_names())
    def test_success(self, sample_name, datasource_id):
        # Clear out connections at the onset, else the get_engine() portion
        # gets confused over human-name conflicts when we've bootstrapped
        # the same sample data repeatedly, namely through having first
        # run TestBootstrapDatasources.test_bootstrap_datasources().

        case_data = SampleData.get_sample(sample_name)

        registry = get_connection_registry()

        datasources.bootstrap_datasource(
            registry,
            datasource_id,
            case_data.meta_json,
            case_data.dsn_json,
            case_data.connect_args_json,
        )

        # Check over the created 'Connection' instance.

        # Alas, in Connection parlance, 'name' == 'sql_cell_handle', and 'human_name'
        # is the human-assigned name for the datasource. Sigh.
        expected_sql_cell_handle_name = f'@{datasource_id}'
        the_conn = registry.get(expected_sql_cell_handle_name)
        assert the_conn.sql_cell_handle == expected_sql_cell_handle_name
        # Might be None in the meta_json.
        expected_human_name = case_data.meta_dict.get('name')
        assert the_conn.human_name == expected_human_name

        # Test get_sqla_engine() while here,
        assert the_conn._engine is get_sqla_engine(expected_sql_cell_handle_name)
        if expected_human_name:
            # Can only work this way also if the datasource name was present in meta-json.
            assert the_conn._engine is get_sqla_engine(expected_human_name)

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
                'required_python_modules': ['psycopg2'],
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

        registry = get_connection_registry()
        initial_len = len(registry)

        # Trying to bootstrap this one will fail somewhat silently -- will log exception, but
        # ultimately not having added new entry into Connection.connections.
        datasources.bootstrap_datasource(
            registry,
            datasource_id,
            case_data.meta_json,
            case_data.dsn_json,
            case_data.connect_args_json,
        )

        assert len(registry) == initial_len

        assert len(log_output.entries) == 1

        e1 = log_output.entries[0]
        assert e1['event'] == 'Unable to bootstrap datasource'
        assert e1['human_name'] == 'My PostgreSQL'

    def test_postgres_via_psycopg2_binary_is_ok(self, datasource_id):
        """Nowadays we have "psycopg2" source package installed, and newer-generation
        datasources from Gate will be spelled asking for 'psycopg2', but older generation
        datasources from Gate will say 'psycopg2-binary'.

        A mapping down deep in ensuring our packages are installed should treat these
        two equally.
        """
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
                'host': 'localhost',
                'port': 5432,
                'database': 'postgres',
            },
        )

        registry = get_connection_registry()

        datasources.bootstrap_datasource(
            registry,
            datasource_id,
            case_data.meta_json,
            case_data.dsn_json,
            case_data.connect_args_json,
        )

        assert len(registry) == 1

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

        registry = get_connection_registry()

        datasources.bootstrap_datasource(
            registry,
            datasource_id,
            case_data.meta_json,
            case_data.dsn_json,
            case_data.connect_args_json,
        )

        # No successful side effect.

        assert len(registry) == 0

        assert len(log_output.entries) == 1

        e1 = log_output.entries[0]
        assert e1['event'] == 'Unable to bootstrap datasource'

        # But we do expect the postprocessor to have run, and to have created this
        # file properly....

        # /tmp/{datasource_id}_bigquery_credentials.json should now exist and
        # contain '{"foo": "bar"}' due to conspiracy in
        # datasource_postprocessing.postprocess_bigquery
        with open(f'/tmp/{datasource_id}_bigquery_credentials.json') as inf:
            from_json = json.load(inf)
            assert from_json == {'foo': 'bar'}

    def test_postprocess_postgresql(self, datasource_id):
        pg_details = SampleData.get_sample('simple-postgres')

        datasources.bootstrap_datasource(
            get_connection_registry(),
            datasource_id,
            pg_details.meta_json,
            pg_details.dsn_json,
            pg_details.connect_args_json,
        )

        # At least look for signs of the side-effect. Can't test it actually does the
        # right thing here w/o actually making a postgresql connection, firing off a query,
        # and then having another thread or whatnot deliver SIGINT while waiting for query
        # results. Or just go clicktest it in integration.

        import psycopg2.extensions
        import psycopg2.extras

        assert psycopg2.extensions.get_wait_callback() is psycopg2.extras.wait_select


@pytest.mark.usefixtures("with_empty_connections")
class TestDatabricks:
    """Test ENG-5517 Very Special Behavior for databricks side-effects if databricks-connect script is
    in PATH and we have the optional 'cluster_id' datapoint in connect args.
    """

    @pytest.fixture()
    def tmp_home(self, tmpdir: Path) -> Path:
        """Replace $HOME to be a new directory of $TMPDIR, yielding the new Path."""
        existing_home = os.environ['HOME']

        new_home = tmpdir / 'home'

        new_home.mkdir()

        os.environ['HOME'] = str(new_home)

        try:
            yield new_home
        finally:
            os.environ['HOME'] = existing_home

    @pytest.fixture()
    def databricks_connect_in_path(self, tmpdir: Path) -> Tuple[Path, Path]:
        """Get a mock-ish executable 'databricks-connect' into an element in the path
        so that which('databricks-connect') will find something (see databricks post
        processor)

        Yields the new executable's path, plus where it will scribble its own output.
        """

        # Make a new subdir of tmpdir, add it to the path, create executable
        # shell script databricks-connect

        bindir = tmpdir / 'scratch-bin'
        bindir.mkdir()

        orig_path = os.environ['PATH']

        os.environ['PATH'] = f"{orig_path}:{bindir}"

        scriptpath = bindir / 'databricks-connect'
        script_output_path = tmpdir / 'connect-inputs.txt'

        # Now make a 'databricks-connect' executable that echos all its stdin to tmpdir/connect-inputs.txt.txt.
        with open(scriptpath, 'w') as outfile:
            outfile.write(f'#!/bin/sh\ncat > {script_output_path}\nexit 0\n')

        scriptpath.chmod(0o755)

        try:
            yield scriptpath, script_output_path

        finally:
            # Undo $PATH change
            os.environ['PATH'] = orig_path

    @pytest.fixture()
    def jsons_for_extra_behavior(self) -> Tuple[DatasourceJSONs, dict]:
        """Return a DatasourceJSONs describing databricks that will tickle postprocess_databricks()
        into doing its extra behavior. Also returns dict of some of the fields within that JSON."""

        hostname = 'dbc-1bab80fc-a74b.cloud.databricks.com'
        password = 'dapie372d57cefdc078d8ce3936fcb0e22ee'
        port = 54321
        org_id = 65475674534576
        cluster_id = '0122-044839-vx2fk606'

        case_data = DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['sqlalchemy-databricks'],
                'allow_datasource_dialect_autoinstall': False,
                'drivername': 'databricks+connector',
                'sqlmagic_autocommit': False,  # This one is special!
                'name': 'Databricks With Extras',
            },
            dsn_dict={
                'username': 'token',
                'host': hostname,
                'password': password,
            },
            connect_args_dict={
                "http_path": "sql/protocolv1/o/2414094324684936/0125-220758-m9pfb4c7",
                "cluster_id": cluster_id,
                "org_id": org_id,
                "port": port,
            },
        )

        return (
            case_data,
            {
                'hostname': hostname,
                'password': password,
                'port': port,
                'org_id': org_id,
                'cluster_id': cluster_id,
            },
        )

    def test_postprocess_databricks_pops_correctly(self, datasource_id, jsons_for_extra_behavior):
        """Ensure that postprocess_databricks side effect pops from the correct dict (connect_args,
        not the containing create_engine_kwargs dict), even w/o databricks-connect
        being found in the $PATH.
        """

        keys_expected_to_be_removed = ['cluster_id', 'org_id', 'port']
        jsons_obj, specific_fields = jsons_for_extra_behavior
        connect_args = jsons_obj.connect_args_dict

        # All initially there...
        assert all(key in connect_args for key in keys_expected_to_be_removed)

        create_engine_kwargs = {'connect_args': connect_args}

        datasource_postprocessing.postprocess_databricks(
            datasource_id,
            jsons_obj.dsn_dict,
            create_engine_kwargs,
        )

        # Should have removed all the keys as side effect of the call.
        # (Had bug where they were popped from wrong dict originally.)
        assert not any(key in connect_args for key in keys_expected_to_be_removed)

    def test_errors_from_databricks_connect_are_surfaced(
        datasource_id, databricks_connect_in_path, tmp_home, jsons_for_extra_behavior
    ):
        """Prove that if databricks-connect script exits nonzero, a ValueError is raised
        and the script's stderr will be within the error message."""

        # Respell the databricks-connect script to always error out, expect that in a ValueError
        # when calling postprocess_databricks

        script_path, _ = databricks_connect_in_path

        expected_error_message = 'oh noes!'

        # Respell the script to bomb out with message to stderr.
        with script_path.open('w') as of:
            of.write('#!/bin/sh\n')
            of.write(f'echo "{expected_error_message}" 1>&2\n')
            of.write('exit 1\n')

        jsons_obj, specific_fields = jsons_for_extra_behavior
        create_engine_kwargs = {'connect_args': jsons_obj.connect_args_dict}

        with pytest.raises(ValueError, match=expected_error_message):
            datasource_postprocessing.postprocess_databricks(
                datasource_id,
                jsons_obj.dsn_dict,
                create_engine_kwargs,
            )

    @pytest.fixture()
    def short_script_timeout(self):
        """Respell datasource_postprocessing.DATABRICKS_CONNECT_SCRIPT_TIMEOUT to 1 (second)"""
        original_value = datasource_postprocessing.DATABRICKS_CONNECT_SCRIPT_TIMEOUT

        datasource_postprocessing.DATABRICKS_CONNECT_SCRIPT_TIMEOUT = 1

        try:
            yield datasource_postprocessing.DATABRICKS_CONNECT_SCRIPT_TIMEOUT
        finally:
            datasource_postprocessing.DATABRICKS_CONNECT_SCRIPT_TIMEOUT = original_value

    def test_databricks_connect_taking_too_long(
        datasource_id, databricks_connect_in_path, short_script_timeout, jsons_for_extra_behavior
    ):
        """Prove that if databricks-connect takes longer than allowed to run, that ValueError will
        be raised with an appropriate message.
        """

        # Respell the databricks-connect script to take longer than short_script_timeout seconds,
        # expect that in a ValueError when calling postprocess_databricks.

        script_path, _ = databricks_connect_in_path

        # Respell the script to take longer than new timeout, but to (try to) exit cleanly
        with script_path.open('w') as of:
            of.write('#!/bin/sh\n')
            of.write(f'sleep {short_script_timeout+1}\n')
            of.write('exit 0\n')

        jsons_obj, specific_fields = jsons_for_extra_behavior
        create_engine_kwargs = {'connect_args': jsons_obj.connect_args_dict}

        with pytest.raises(ValueError, match='databricks-connect took longer than'):
            datasource_postprocessing.postprocess_databricks(
                datasource_id,
                jsons_obj.dsn_dict,
                create_engine_kwargs,
            )

    def test_extra_behavior(
        self, datasource_id, databricks_connect_in_path, tmp_home, jsons_for_extra_behavior
    ):
        """Test creating databricks with extra keys to cause postprocess_databricks() to do its magic"""

        # Make a preexisting tmp_home/.databricks-connect, expect it to get unlinked
        # (see lines in postprocess_databricks)
        dotconnect = tmp_home / '.databricks-connect'
        with dotconnect.open('w') as of:
            of.write('exists')

        assert dotconnect.exists()

        case_data, case_dict = jsons_for_extra_behavior

        assert 'cluster_id' in case_data.connect_args_dict

        registry = get_connection_registry()

        datasources.bootstrap_datasource(
            registry,
            datasource_id,
            case_data.meta_json,
            case_data.dsn_json,
            case_data.connect_args_json,
        )

        assert len(registry) == 1

        # Preexisting file should have been unlinked. The real databricks-connect
        # script would have recreated it, but the mock version we create in fixture
        # databricks_connect_in_path will create a different file.
        assert not dotconnect.exists()

        # databricks_connect_in_path second member is the path where the fake script output was placed
        _, script_output = databricks_connect_in_path
        assert script_output.exists()

        # Expect to find things in it. See ENG-5517.
        # We can only test that we ran this mock script and the known result
        # of our mock script. What the real one does ... ?
        contents = script_output.read().split('\n')
        assert len(contents) == 6
        assert contents[0] == 'y'
        assert contents[1] == f"https://{case_dict['hostname']}/"
        assert contents[2] == case_dict['password']
        assert contents[3] == case_dict['cluster_id']
        assert contents[4] == str(case_dict['org_id'])
        assert contents[5] == str(case_dict['port'])

    def test_skip_extra_behavior_if_no_databricks_connect(
        self, datasource_id, tmp_home, jsons_for_extra_behavior
    ):
        # Let's create a $HOME/.databricks-connect file. It should remain untouched
        # since we're not also using fixture databricks_connect_in_path putting the
        # script in our path.

        dotconnect = tmp_home / '.databricks-connect'
        with dotconnect.open('w') as of:
            of.write('preexists')

        assert dotconnect.exists()

        case_data, case_dict = jsons_for_extra_behavior

        registry = get_connection_registry()

        # Should not fail, but won't have done any extra behavior.
        datasources.bootstrap_datasource(
            registry,
            datasource_id,
            case_data.meta_json,
            case_data.dsn_json,
            case_data.connect_args_json,
        )

        assert len(registry) == 1

        # Left unchanged
        assert dotconnect.exists()
        assert 'preexists' in dotconnect.read()

    def test_skip_extra_behavior_if_no_cluster_id(
        self, datasource_id, tmp_home, databricks_connect_in_path
    ):
        # Let's create a $HOME/.databricks-connect file. It should remain untouched
        # since we're not also using fixture databricks_connect_in_path putting the
        # script in our path.

        dotconnect = tmp_home / '.databricks-connect'
        with dotconnect.open('w') as of:
            of.write('preexists')

        assert dotconnect.exists()

        case_data = SampleData.get_sample('databricks')
        assert 'cluster_id' not in case_data.connect_args_dict

        # Should not have triggered extra behavior -- cluster_id wasn't present (but
        # we do have a databricks-connect script in the PATH).

        registry = get_connection_registry()

        datasources.bootstrap_datasource(
            registry,
            datasource_id,
            case_data.meta_json,
            case_data.dsn_json,
            case_data.connect_args_json,
        )

        assert len(registry) == 1

        # But won't have breathed on dotconnect file.
        # Left unchanged
        assert dotconnect.exists()
        assert 'preexists' in dotconnect.read()


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


class TestSQLite:
    @pytest.mark.parametrize(
        'sample',
        (
            DatasourceJSONs(
                meta_dict={
                    'required_python_modules': [],
                    'allow_datasource_dialect_autoinstall': False,
                    'drivername': 'sqlite',
                    'sqlmagic_autocommit': False,
                    'name': 'Download file sqlite',
                },
                dsn_dict={
                    'database': 'mock://source.of.my.db.com/foo.sqlite',
                },
                connect_args_dict={
                    'max_download_seconds': '22',
                },
            ),
            'memory-sqlite-also-with-max_download_seconds',
        ),
    )
    def test_postprocess_sqlite_pops_max_download_seconds_correctly(
        self, sample: Union[str, DatasourceJSONs], datasource_id, requests_mock, tests_fixture_data
    ):
        if isinstance(sample, str):
            jsons = SampleData.get_sample(sample)
        else:
            jsons = sample

        create_engine_kwargs = {'connect_args': dict(jsons.connect_args_dict)}

        assert 'max_download_seconds' in create_engine_kwargs['connect_args']

        with open(tests_fixture_data / 'portal_mammals.sqlite', 'rb') as response_file:
            if jsons.dsn_dict['database'].startswith('mock'):
                # Set up response for a GET to that URL to return the contents of our canned copy.
                requests_mock.get(jsons.dsn_dict['database'], body=response_file)

            datasource_postprocessing.postprocess_sqlite(
                datasource_id, dsn_dict=jsons.dsn_dict, create_engine_kwargs=create_engine_kwargs
            )

        # Should be popped out from connect_args regardless of if was mem db or real download db.
        assert 'max_download_seconds' not in create_engine_kwargs['connect_args']

    def test_actually_connecting_to_sqlite_with_download_seconds(self, datasource_id):
        jsons = SampleData.get_sample('memory-sqlite-also-with-max_download_seconds')

        datasources.bootstrap_datasource(
            get_connection_registry(),
            datasource_id,
            jsons.meta_json,
            jsons.dsn_json,
            jsons.connect_args_json,
        )

        engine = get_sqla_engine(jsons.meta_dict['name'])
        # Should not barf when trying to connect, https://community.noteable.io/c/issues-and-bugs/cant-connect-to-sqlite-database
        engine.execute('select 1')

    @pytest.mark.usefixtures("with_empty_connections")
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

        datasources.bootstrap_datasource(
            get_connection_registry(),
            datasource_id,
            bad_sqlite.meta_json,
            bad_sqlite.dsn_json,
            bad_sqlite.connect_args_json,
        )

        registry = get_connection_registry()
        assert len(registry) == 0

        # And should have had a bootstrapping failure against both the datasource_id
        # and the human name.
        assert 'SQLite database files should be located' in registry.get_bootstrapping_failure(
            human_name
        )
        assert 'SQLite database files should be located' in registry.get_bootstrapping_failure(
            f'@{datasource_id}'
        )

        # There are test(s) over in test_sql_magic.py that prove that when such a broken datasource is
        # attempted to be used, this get_bootstrapping_failure() message will show back up, surfacing
        # the real problem to the user.


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


@pytest.mark.parametrize(
    'input_dicts,expected_dicts',
    [
        (
            # Should expand initial host value of AWS region to whole hostname; quote_plus affects username, password, s2_staging_dir
            (
                # input DSN dict
                {'host': 'us-west-1', 'username': 'ADFGD:/', 'password': 'MMHq:/'},
                # input connect args dict
                {'s3_staging_dir': 's3://myamazonbucket/results/'},
            ),
            (
                # Resulting DSN dict
                {
                    'host': 'athena.us-west-1.amazonaws.com',
                    'username': 'ADFGD%3A%2F',
                    'password': 'MMHq%3A%2F',
                },
                # resulting connect args dict
                {'s3_staging_dir': 's3%3A%2F%2Fmyamazonbucket%2Fresults%2F'},
            ),
        ),
    ],
)
def test_postprocess_awsathena(input_dicts, expected_dicts):
    dsn_dict, create_engine_dict = input_dicts
    datasource_postprocessing.postprocess_awsathena(None, dsn_dict, create_engine_dict)
    assert dsn_dict == expected_dicts[0]
    assert create_engine_dict == expected_dicts[1]


@pytest.mark.parametrize(
    "input_create_engine_dict,expected_query_params",
    [
        (
            # Input create engine dict
            {'connect_args': {'secure_connection': 'Yes, use HTTPS'}},
            # Expected connect_args dict
            {'protocol': 'https', 'verify': 'False'},
        ),
        (
            {'connect_args': {'secure_connection': 'Yes, use HTTPS and verify server certificate'}},
            {'protocol': 'https', 'verify': '/path/to/certifi/cert.pem'},
        ),
        (
            {'connect_args': {'secure_connection': 'No, use HTTP'}},
            {'protocol': 'http', 'verify': 'False'},
        ),
    ],
)
def test_postprocess_clickhouse(input_create_engine_dict, expected_query_params):
    with patch.object(certifi, 'where', return_value='/path/to/certifi/cert.pem'):
        dsn_dict = {}
        datasource_postprocessing.postprocess_clickhouse(None, dsn_dict, input_create_engine_dict)
        assert dsn_dict['query'] == expected_query_params


def test_postprocess_clickhouse_raises_on_bad_secure_connection():
    with pytest.raises(ValueError):
        datasource_postprocessing.postprocess_clickhouse(
            None, None, {'connect_args': {'secure_connection': 'foobar'}}
        )


@pytest.mark.parametrize(
    "input_create_engine_dict,expected_connect_args_dict",
    [
        (
            # Input create engine dict
            {'connect_args': {'verify': True}},
            # Expected connect_args dict
            {
                "Driver": "ODBC Driver 18 for SQL Server",
                "Authentication": "SqlPassword",
                "Encrypt": "yes",
                # If asked to verify the server certificate, we should not trust it.
                "TrustServerCertificate": "no",
            },
        ),
        (
            # Input create engine dict
            {'connect_args': {'verify': False}},
            # Expected connect_args dict
            {
                "Driver": "ODBC Driver 18 for SQL Server",
                "Authentication": "SqlPassword",
                "Encrypt": "yes",
                # If asked to not verify the server certificate, we should trust it.
                "TrustServerCertificate": "yes",
            },
        ),
    ],
)
def test_postprocess_mssql_pyodbc(input_create_engine_dict, expected_connect_args_dict):
    datasource_postprocessing.postprocess_mssql_pyodbc(None, None, input_create_engine_dict)
    assert input_create_engine_dict['connect_args'] == expected_connect_args_dict
