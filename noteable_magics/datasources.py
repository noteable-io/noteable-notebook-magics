"""External datasource / database connection management bridging Noteable and ipython-sql"""
import json
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Union, Callable

import pkg_resources

# ipython-sql thinks mighty highly of isself with this package name.
import sql.connection
from sql.run import add_commit_blacklist_dialect
from sqlalchemy.engine import URL

DEFAULT_SECRETS_DIR = Path('/vault/secrets')

from noteable_magics.datasource_postprocessing import post_processor_by_drivername


def bootstrap_datasources(secrets_dir: Union[Path, str] = DEFAULT_SECRETS_DIR):
    """Digest all of the datasource files Vault injector has created for us and
    inject into ipython-sql as their Connection objects.

    """

    if isinstance(secrets_dir, str):
        secrets_dir = Path(secrets_dir)

    # Look for *.meta.json files.
    for ds_meta_json_path in secrets_dir.glob('*.meta_js'):
        # Derive filenames for the expected related files

        bootstrap_datasource_from_files(ds_meta_json_path)


def bootstrap_datasource_from_files(ds_meta_json_path: Path):
    """Bootstrap a single datasource from files given reference to the meta json file

    Assumes the other two files are peers in the directory and named accordingly
    """
    # '/foo/bar/345345345345.meta_js' -> '345345345345'
    basename = ds_meta_json_path.stem

    # Always present.
    meta_json = ds_meta_json_path.read_text()

    # The other two end up being optionally present.
    dsn_json_path = ds_meta_json_path.parent / (basename + '.dsn_js')
    if dsn_json_path.exists():
        dsn_json = dsn_json_path.read_text()
    else:
        dsn_json = None

    connect_args_json_path = ds_meta_json_path.parent / (basename + '.ca_js')
    if connect_args_json_path.exists():
        connect_args_json = connect_args_json_path.read_text()
    else:
        connect_args_json = None

    bootstrap_datasource(basename, meta_json, dsn_json, connect_args_json)


def bootstrap_datasource(
    datasource_id: str, meta_json: str, dsn_json: Optional[str], connect_args_json: Optional[str]
):
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

    # Yes, bigquery connections may end up with nothing in dsn_json.
    dsn_dict = json.loads(dsn_json) if dsn_json else {}
    # 'drivername' comes in via metadata, because reasons.
    drivername = metadata['drivername']
    dsn_dict['drivername'] = drivername
    url_obj = URL.create(**dsn_dict)
    connection_url = str(url_obj)

    # Do we need to tell sql-magic to not try to emit a COMMIT after each statement
    # according to the needs of this driver?
    if not metadata['sqlmagic_autocommit']:
        # A sqlalchemy drivername may be comprised of 'dialect+drivername', such as
        # 'databricks+connector'.
        # If so, then we must only pass along the LHS of the '+'.
        dialect = metadata['drivername'].split('+')[0]
        add_commit_blacklist_dialect(dialect)

    connect_args = json.loads(connect_args_json) if connect_args_json else {}

    create_engine_kwargs = {'connect_args': connect_args}

    # Per-drivername customization needs?
    if drivername in post_processor_by_drivername:
        post_processor: Callable[[str, dict], None] = post_processor_by_drivername[drivername]
        post_processor(datasource_id, create_engine_kwargs)

    # Teach ipython-sql about it!
    sql.connection.Connection.set(
        connection_url, name=f'@{datasource_id}', displaycon=False, **create_engine_kwargs
    )


##
# Interior utilities here on out
##


def ensure_requirements(datasource_id: str, requirements: List[str], allowed_to_install: bool):
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

    return False


def install_package(pkg_name: str) -> None:
    """Install `pkg_name` using pip"""

    run_pip(["install", pkg_name])


def run_pip(pip_args: List[str]):
    subprocess.check_call([sys.executable, "-m", "pip"] + pip_args)
