""" Tests over datasource bootstrapping """

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

import pytest

from noteable_magics import datasources
from sql.run import _COMMIT_BLACKLIST_DIALECTS


@pytest.fixture
def not_installed_packages() -> list[str]:
    """Yield a few not currently installed packages, uninstall them as needed
    upon cleanup.
    """

    # Not even kidding there's a left-pad for python.
    pkgnames = ['orjson', 'left-pad']

    for pkgname in pkgnames:
        if datasources.is_package_installed(pkgname):
            datasources.run_pip(['uninstall', '-y', pkgname])

    yield pkgnames

    for pkgname in pkgnames:
        if datasources.is_package_installed(pkgname):
            datasources.run_pip(['uninstall', '-y', pkgname])


@pytest.fixture
def not_installed_package(not_installed_packages: list[str]) -> str:
    """Yield a package name that is definitely not currently installed, then uninstall
    it upon cleanup if needed.
    """
    yield not_installed_packages[0]


@pytest.fixture
def datasource_id() -> str:
    return uuid4().hex


@dataclass
class DatasourceJSONs:
    meta_dict: dict[str, Any]
    dsn_dict: Optional[dict[str, str]]
    connect_args_dict: Optional[dict[str, any]]

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


class SampleData:
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
            connect_args_dict=None,
        )
    }

    @classmethod
    def get_sample(cls, name: str) -> DatasourceJSONs:
        return cls.samples[name]

    @classmethod
    def all_sample_names(cls) -> list[str]:
        # Sorted so that if tests are run in parallel test discovery is stable.
        return sorted(cls.samples.keys())


class TestBootstrapDatasource:
    @pytest.mark.parametrize('sample_name', SampleData.all_sample_names())
    def test_success(self, sample_name, datasource_id, mocker):
        case_data = SampleData.get_sample(sample_name)

        # ipython-sql ends up trying to eagerly connect to the datasource; not just creating the engine.
        # XXX perhaps we want to adjust to delay connecting until first use?
        patched_connect = mocker.patch('sqlalchemy.engine.base.Engine.connect')
        datasources.bootstrap_datasource(
            datasource_id, case_data.meta_json, case_data.dsn_json, case_data.connect_args_json
        )

        patched_connect.assert_called_once()

        assert all(
            datasources.is_package_installed(pkg_name)
            for pkg_name in case_data.meta_dict['required_python_modules']
        )

        # If case_data.meta_dict['sqlmagic_autocommit'] is False, then expect to see the drivername
        # mentioned in ipython-sql's _COMMIT_BLACKLIST_DIALECTS set.
        drivername = case_data.meta_dict['drivername']
        assert (drivername in _COMMIT_BLACKLIST_DIALECTS) == (
            not case_data.meta_dict['sqlmagic_autocommit']
        )


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
