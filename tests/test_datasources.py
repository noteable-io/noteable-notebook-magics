""" Tests over datasource bootstrapping """

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

import pkg_resources
import pytest
from sql.connection import Connection
from sql.run import _COMMIT_BLACKLIST_DIALECTS

from noteable_magics import datasources


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


@dataclass
class DatasourceJSONs:
    meta_dict: Dict[str, Any]
    dsn_dict: Optional[Dict[str, str]] = None
    connect_args_dict: Optional[Dict[str, any]] = None

    @property
    def meta_json(self) -> str:
        return json.dumps(self.meta_dict)

    @property
    def dsn_json(self) -> Optional[str]:
        if self.dsn_dict:
            return json.dumps(self.dsn_dict)

    @property
    def connect_args_json(self) -> Optional[str]:
        if self.connect_args_dict:
            return json.dumps(self.connect_args_dict)

    def json_to_tmpdir(self, datasource_id: str, tmpdir: Path):
        """Save our json strings to a tmpdir so can be used to test
        bootstrap_datasource_from_files or bootstrap_datasources
        """

        json_str_and_paths = [
            (self.meta_json, tmpdir / f'{datasource_id}.meta_js'),
            (self.dsn_json, tmpdir / f'{datasource_id}.dsn_js'),
            (self.connect_args_json, tmpdir / f'{datasource_id}.ca_js'),
        ]

        for json_str, path in json_str_and_paths:
            if json_str:
                path.write_text(json_str)


class SampleData:
    """Test case fodder"""

    samples = {
        'simple-postgres': DatasourceJSONs(
            meta_dict={
                'required_python_modules': ['psycopg2-binary'],
                'allow_datasource_dialect_autoinstall': True,
                'drivername': 'postgresql',
                'sqlmagic_autocommit': True,
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
                'allow_datasource_dialect_autoinstall': True,
                'drivername': 'postgresql',
                'sqlmagic_autocommit': True,
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
                'allow_datasource_dialect_autoinstall': True,
                'drivername': 'redshift+redshift_connector',
                'sqlmagic_autocommit': True,
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
                'allow_datasource_dialect_autoinstall': True,
                'drivername': 'trino',
                'sqlmagic_autocommit': False,  # This one is special!
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
                'allow_datasource_dialect_autoinstall': True,
                'drivername': 'databricks+connector',
                'sqlmagic_autocommit': False,  # This one is special!
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
        case_data = SampleData.get_sample(sample_name)

        datasources.bootstrap_datasource(
            datasource_id, case_data.meta_json, case_data.dsn_json, case_data.connect_args_json
        )

        # Check over the created 'Connection' instance.
        expected_name = f'@{datasource_id}'
        the_conn = Connection.connections[expected_name]
        assert the_conn.name == expected_name

        # Ensure the required packages are installed.
        assert all(
            datasources.is_package_installed(pkg_name)
            for pkg_name in case_data.meta_dict['required_python_modules']
        )

        # If case_data.meta_dict['sqlmagic_autocommit'] is False, then expect to see the dialect portion of
        # drivername mentioned in ipython-sql's _COMMIT_BLACKLIST_DIALECTS set.
        dialect = case_data.meta_dict['drivername'].split('+')[0]
        assert (dialect in _COMMIT_BLACKLIST_DIALECTS) == (
            not case_data.meta_dict['sqlmagic_autocommit']
        )

    def test_bigquery_particulars(self, datasource_id):
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
                'allow_datasource_dialect_autoinstall': True,
                'drivername': 'bigquery',
                'sqlmagic_autocommit': True,
            },
            connect_args_dict={
                # b64 encoding of '{"foo": "bar"}'
                'credential_file_contents': 'eyJmb28iOiAiYmFyIn0='
            },
        )

        # Expect the ultimate call to sqlalchemy.create_engine() to fail, because
        # we're not really feeding it a legit google credentials file at this time
        # (the 'credentials_file_contents' in the sample data is really just {"foo": "bar"}).
        #
        # Had postprocess_bigquery() not done the promotion from connect_args -> create_engine_kwargs, would
        # die a very different death, complaining about cannot find any credentials anywhere
        # since not passed in and the google magic env var isn't set.
        with pytest.raises(ValueError, match='Service account info was not in the expected format'):
            datasources.bootstrap_datasource(
                datasource_id, case_data.meta_json, case_data.dsn_json, case_data.connect_args_json
            )

        # But we do expect the postprocessor to have run, and to have created this
        # file properly....

        # /tmp/{datasource_id}_bigquery_credentials.json should now exist and
        # contain '{"foo": "bar"}' due to consiracy in
        # datasource_postprocessing.postprocess_bigquery
        with open(f'/tmp/{datasource_id}_bigquery_credentials.json') as inf:
            from_json = json.load(inf)
            assert from_json == {'foo': 'bar'}


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
