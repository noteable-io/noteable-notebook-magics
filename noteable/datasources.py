"""External datasource / database connection management"""
import json
import subprocess
import sys
from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pkg_resources
import structlog

# Import all our known concrete Connection implementations.
import noteable.sql.sqlalchemy  # noqa
# ipython-sql thinks mighty highly of isself with this package name.
from noteable.sql.connection import (
    Connection,
    ConnectionRegistry,
    get_connection_class,
    get_connection_registry,
)

DEFAULT_SECRETS_DIR = Path('/vault/secrets')


logger = structlog.get_logger(__name__)


def discover_datasources(secrets_dir: Union[Path, str] = DEFAULT_SECRETS_DIR):
    """Discover all of the possible datasource configurations that the Vault injector has
    left for us. Register a callback with the connection registry on how to configure
    each one if / when needed when the first time a cell referencing their cell handle
    is used.

    Also register callback for the implicit local memory DuckDB global datasource.
    """

    connection_registry: ConnectionRegistry = get_connection_registry()

    if isinstance(secrets_dir, str):
        secrets_dir = Path(secrets_dir)

    # Look for *.meta.json files.
    for ds_meta_json_path in secrets_dir.glob('*.meta_js'):
        # Derive filenames for the expected related files
        queue_bootstrap_datasource_from_files(connection_registry, ds_meta_json_path)

    # Also inform the registry on how to bootstrap the omnipresent mighty DuckDB if/when needed.
    # (is external function 'cause test suite uses it also.)
    queue_bootstrap_duckdb(connection_registry)


def queue_bootstrap_datasource_from_files(
    connection_registry: ConnectionRegistry, ds_meta_json_path: Path
):
    """Register bootstraper for a single datasource from files given reference to the meta json file

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

    # Must look into metadata to at least get the human name before registering the rest of bootstrapping.
    metadata = json.loads(meta_json)
    datasource_id = basename

    bootstrapper = partial(
        bootstrap_datasource,
        datasource_id=datasource_id,
        metadata=metadata,
        dsn_json=dsn_json,
        connect_args_json=connect_args_json,
    )

    sql_cell_handle = f'@{basename}'
    human_name = metadata['name']

    # The registry will call the bootstrapper function if/when this datasource is needed.
    connection_registry.register_datasource_bootstrapper(sql_cell_handle, human_name, bootstrapper)


def bootstrap_datasource(
    datasource_id: str,
    metadata: dict,
    dsn_json: Optional[str],
    connect_args_json: Optional[str],
) -> Connection:
    """Bootstrap this single datasource from its three json definition JSON sections"""

    if not isinstance(metadata, dict):
        # This param changed type recently, catch it early.
        raise ValueError(
            f'bootstrap_datasource() expects `metadata` to be passed in as a dict, got {type(metadata)}'
        )

    # Yes, bigquery connections may end up with nothing in dsn_json.
    dsn_dict = json.loads(dsn_json) if dsn_json else {}

    connect_args = json.loads(connect_args_json) if connect_args_json else {}

    create_engine_kwargs = {'connect_args': connect_args}

    # 'drivername', and is really of form dialect(+drivername), comes in via metadata, because reasons.
    drivername = metadata['drivername']
    dsn_dict['drivername'] = drivername

    # Generally pre-process the end-user-editable dicts brought to us by vault
    # by in-place eroding away any KV pair where the value is the empty string.
    pre_process_dict(dsn_dict)
    pre_process_dict(connect_args)

    # Late lookup the Connection subclass implementation registered for this drivername.
    # Will raise KeyError if none are registered.
    connection_class = get_connection_class(drivername)

    if hasattr(connection_class, 'preprocess_configuration'):
        connection_class.preprocess_configuration(datasource_id, dsn_dict, create_engine_kwargs)

    # Ensure the required driver packages are installed already, or, if allowed,
    # install them on the fly.
    ensure_requirements(
        datasource_id,
        metadata['required_python_modules'],
        metadata['allow_datasource_dialect_autoinstall'],
    )

    # Individual Connection classes don't need to be bothered with these.
    del metadata['required_python_modules']
    del metadata['allow_datasource_dialect_autoinstall']

    # Construct + return Connection subclass instance.
    return connection_class(f'@{datasource_id}', metadata, dsn_dict, create_engine_kwargs)


##
# Interior utilities here on out
##


_old_to_new_package_name = {
    # Older-generation PostgreSQL and CRDB gate-side datasources will claim to require psycopg2-binary,
    # but nowadays we install / use psycopg2 source package. They both provide the same underlying
    # importable package, 'psycopg2'.
    'psycopg2-binary': 'psycopg2'
}


def ensure_requirements(datasource_id: str, requirements: List[str], allowed_to_install: bool):
    """Ensure he required driver packages are installed already, or, if allowed,
    install them on the fly.
    """
    for pkg in requirements:
        # Perhaps swap out package name?
        pkg = _old_to_new_package_name.get(pkg, pkg)
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

    run_pip(["install", pkg_name], timeout=120)


def run_pip(pip_args: List[str], timeout=60):
    subprocess.check_call([sys.executable, "-m", "pip"] + pip_args, timeout=timeout)


def pre_process_dict(the_dict: Dict[str, Any]) -> None:
    """Pre-process the given dict by removing any KV pair where V is empty string.

    We do this because when Geas POSTs datasource, optional
    fields may well be left blank, or when PATCHing the most we
    can 'unset' an optional field is to overwrite a prior value
    with a blank. But down here when we're about to pass down into
    create_engine(), we need to finally honor the intent of those blanks
    as 'unset'.
    """
    for k, v in list(the_dict.items()):
        if v == '':
            del the_dict[k]
        if isinstance(v, dict):
            # connect_args dicts may not be flat. But they do end, eventually,
            # otherwise they'd not be JSON-able to make it this far.
            pre_process_dict(v)
            # That could have possibly removed *everything* from that dict. If
            # so, then remove it from our dict also.
            if len(v) == 0:
                del the_dict[k]


LOCAL_DB_CONN_HANDLE = "@noteable"
LOCAL_DB_CONN_NAME = "Local Database"


def local_duckdb_bootstrapper() -> Connection:
    """Return the noteable.sql.connection.Connection to use for local memory DuckDB."""
    return noteable.sql.sqlalchemy.DuckDBConnection(
        LOCAL_DB_CONN_HANDLE,
        {'name': LOCAL_DB_CONN_NAME},
        {'drivername': 'duckdb', 'database': ':memory:'},
    )


def queue_bootstrap_duckdb(registry: ConnectionRegistry):
    registry.register_datasource_bootstrapper(
        sql_cell_handle=LOCAL_DB_CONN_HANDLE,
        human_name=LOCAL_DB_CONN_NAME,
        bootstrapper=local_duckdb_bootstrapper,
    )
