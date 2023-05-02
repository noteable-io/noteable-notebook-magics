"""External datasource / database connection management bridging Noteable and ipython-sql"""
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import pkg_resources
import structlog
from sqlalchemy.engine import URL

# ipython-sql thinks mighty highly of isself with this package name.
import noteable.sql.connection
from noteable.sql.run import add_commit_blacklist_dialect

DEFAULT_SECRETS_DIR = Path('/vault/secrets')

from noteable.datasource_postprocessing import post_processor_by_drivername

logger = structlog.get_logger(__name__)


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

    # human-given name for the datasource is more likely than not present in the metadata
    # ('old' datasources in integration, staging, app.noteable.world may lack)
    human_name = metadata.get('name')

    # Do any per-drivername post-processing of and dsn_dict and create_engine_kwargs
    # before we make use of any of their contents. Post-processors may end up rejecting this
    # configuration, so catch and handle just like a failure when calling Connection.set().
    if drivername in post_processor_by_drivername:
        post_processor: Callable[[str, dict, dict], None] = post_processor_by_drivername[drivername]
        try:
            post_processor(datasource_id, dsn_dict, create_engine_kwargs)
        except Exception as e:
            logger.exception(
                'Unable to bootstrap datasource',
                datasource_id=datasource_id,
                datasource_name=human_name,
            )

            # Remember the failure so can be shown if / when human tries to use the connection.
            noteable.sql.connection.Connection.add_bootstrapping_failure(
                datasource_id, human_name, str(e)
            )

            # And bail early.
            return

    # Ensure the required driver packages are installed already, or, if allowed,
    # install them on the fly.
    ensure_requirements(
        datasource_id,
        metadata['required_python_modules'],
        metadata['allow_datasource_dialect_autoinstall'],
    )

    # Prepare connection URL string.
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

    # Teach ipython-sql about the connection!
    try:
        noteable.sql.connection.Connection.set(
            connection_url,
            name=f'@{datasource_id}',
            human_name=human_name,
            **create_engine_kwargs,
        )
    except Exception as e:
        # Eat any exceptions coming up from trying to describe the connection down into SQLAlchemy.
        # Bad data entered about the datasource that SQLA hates?
        #
        # If we don't eat this, then it will ultimately break us before we make the call to register
        # the SQL Magics entirely, and will get errors like '%%sql magic unknown', which is far
        # worse than attempts to use a broken datasource being met with it being unknown, but other
        # datasources working fine.
        logger.exception(
            'Unable to bootstrap datasource',
            datasource_id=datasource_id,
            datasource_name=human_name,
        )

        # Remember the failure so can be shown if / when human tries to use the connection.
        noteable.sql.connection.Connection.add_bootstrapping_failure(
            datasource_id, human_name, str(e)
        )


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
