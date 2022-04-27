"""External datasource / database connection management bridging Noteable and ipython-sql"""

import json
from pathlib import Path
from typing import Union

import pkg_resources
# ipython-sql thinks mighty highly of isself with this package name.
import sql.connection
from pip._internal.cli.main import main as pipmain
from sql.run import add_commit_blacklist_dialect
from sqlalchemy.engine import URL

DEFAULT_SECRETS_DIR = Path('/vault/secrets')


def bootstrap_datasources(secrets_dir: Union[Path, str] = DEFAULT_SECRETS_DIR):
    """Digest all of the datasource files Vault injector has created for us and
    inject into ipython-sql as their Connection objects.

    """

    if isinstance(secrets_dir, str):
        secrets_dir = Path(secrets_dir)

    # Look for *.meta.json files.
    for ds_meta_json_path in secrets_dir.glob('*.meta.json'):
        # Derive filenames for the expected related files

        bootstrap_datasource_from_files(ds_meta_json_path)


def bootstrap_datasource_from_files(ds_meta_json_path: Path):
    """Bootstrap a single datasource from files given reference to the meta json file

    Assumes the other two files are peers in the directory and named accordingly
    """
    # '/foo/bar/345345345345.meta.json' -> '345345345345'
    basename = ds_meta_json_path.name[: ds_meta_json_path.name.index('.')]

    dsn_json_path = ds_meta_json_path.parent / basename + '.dsn.json'
    if not dsn_json_path.exists():
        raise Exception(f'Could not find {dsn_json_path}')

    connect_args_json_path = ds_meta_json_path.parent / basename + '.ca.json'
    if not connect_args_json_path.exists():
        raise Exception(f'Could not find {connect_args_json_path}')

    # Load 'em all as JSON strings, pass down to next step.
    meta_json = _read_path(ds_meta_json_path)
    dsn_json = _read_path(dsn_json_path)
    connect_args_json = _read_path(connect_args_json_path)

    bootstrap_datasource(basename, meta_json, dsn_json, connect_args_json)


def bootstrap_datasource(datasource_id: str, meta_json: str, dsn_json: str, connect_args_json: str):
    """Bootstrap this single datasource from its three json definition JSON sections"""
    metadata = json.loads(meta_json)

    # Ensure the required driver packages are installed already, or, if allowed,
    # install them on the fly.
    ensure_requirements(
        datasource_id,
        metadata['required_python_modules'],
        metadata['allow_datasource_dialect_autoinstall'],
    )

    # Prepare connection URL string.
    dsn_dict = json.loads(dsn_json)
    # 'drivername' comes in via metadata, because reasons.
    dsn_dict['drivername'] = metadata['drivername']
    url_obj = URL.create(**dsn_dict)
    connection_url = str(url_obj)

    # Do we need to tell sql-magic to not try to emit a COMMIT after each statement
    # according to the needs of this driver?
    if not metadata['sqlmagic_autocommit']:
        add_commit_blacklist_dialect(metadata['drivername'])

    connect_args = json.loads(connect_args_json)

    name = f'@{datasource_id}'

    # Teach ipython-sql about it!
    sql.connection.Connection.set(
        connection_url, displaycon=False, connect_args=connect_args, name=name
    )


##
# Interior utilities here on out
##


def ensure_requirements(datasource_id, requirements: list[str], allowed_to_install: bool):
    """Ensure he required driver packages are installed already, or, if allowed,
    install them on the fly.
    """
    for pkg in requirements:
        if not is_package_installed(pkg):
            if not allowed_to_install:
                raise Exception(
                    f'Datasource {datasource_id!r} requires package {pkg!r}, but is not already installed in the kernel image'
                )

            # we're allowed to install!
            install_package(pkg)


def is_package_installed(pkg_name: str) -> bool:
    """Checks the currently activated python environment to see if `pkg_name` is installed"""

    try:
        pkg_resources.get_distribution(pkg_name)
        return True
    except pkg_resources.DistributionNotFound:
        return False
    except pkg_resources.VersionConflict:
        return False


def install_package(pkg_name: str) -> None:
    """Install `pkg_name` using pip"""

    pipmain(["install", pkg_name])


def _read_path(path: Path):
    with open(path) as f:
        return f.read()
