import os
import shutil
from base64 import b64decode
from pathlib import Path
from subprocess import PIPE, Popen, TimeoutExpired
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Dict
from urllib.parse import quote_plus, urlparse

import structlog

logger = structlog.get_logger(__name__)

import requests

# Dict of drivername -> post-processor function that accepts (datasource_id, create_engine_kwargs
# dict) pair and is expected to mutate create_engine_kwargs as needed.
post_processor_by_drivername: Dict[str, Callable[[str, dict, dict], None]] = {}


def register_postprocessor(drivername: str):
    """Decorator to register a create_engine_kwargs post-processor"""

    def decorator_outer(func):
        assert drivername not in post_processor_by_drivername, f'{drivername} already registered!'

        post_processor_by_drivername[drivername] = func

        return func

    return decorator_outer


##
# create_engine_kwargs post-processors needed to do driver-centric post-processing of create_engine_kwargs
# All these should take create_engine_kwargs dict as input and modify it in place, returning None.
##


@register_postprocessor('postgresql')
def postprocess_postgresql(
    datasource_id: str, dsn_dict: Dict[str, str], create_engine_kwargs: Dict[str, Any]
) -> None:
    """Install fix for ENG-4327 (cannot interrupt kernels doing SQL queries)
    for PostgreSQL.

    psycopg2 is ultimately a wrapper around libpq, which when performing a
    query, ends up blocking delivery of KeyboardInterrupt aka SIGINT.

    However, registering a `wait_callback`, will cause psycopg2 to use
    libpq's nonblocking query interface, which in conjunction with
    `wait_select` will allow KeyboardInterrupt to, well, interrupt long-running
    queries.

    https://github.com/psycopg/psycopg2/blob/master/lib/extras.py#L749-L774
    (as of Aug 2022)

    This was discovered from expecting that other people have complained about this
    issue, and lo and behold, https://github.com/psycopg/psycopg2/issues/333, with bottom
    line:

        For people finding this from the Internet, on recent versions of the library, use this:
            psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)

    Thanks, internet stranger!
    """

    # We don't do anything with the datasorce / dicts. Just need to install this
    # extra behavior into the driver as side-effect so that interrupting the
    # kernel does what we expect.

    _install_psycopg2_interrupt_fix()


@register_postprocessor('cockroachdb')
def postprocess_cockroachdb(
    datasource_id: str, dsn_dict: Dict[str, str], create_engine_kwargs: Dict[str, Any]
) -> None:
    """Install fix for ENG-4327 for Cockroachdb.

    CRDB uses psycopg2 as driver, so it needs the fix also.
    """

    _install_psycopg2_interrupt_fix()


_installed_psycopg2_interrupt_fix = False


def _install_psycopg2_interrupt_fix():
    global _installed_psycopg2_interrupt_fix

    if not _installed_psycopg2_interrupt_fix:
        import psycopg2.extensions
        import psycopg2.extras

        psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)

        _installed_psycopg2_interrupt_fix = True


@register_postprocessor('bigquery')
def postprocess_bigquery(
    datasource_id: str, dsn_dict: Dict[str, str], create_engine_kwargs: Dict[str, Any]
) -> None:
    """
    Set up create_engine_kwargs for BigQuery.

        * 1) Because our Vault secret is tuned to 'connect_args', which are a sub-unit
            of create_engine_kwargs (and in fact was designed *before* James realized
            that BigQuery tuning params go at the `create_engine()` kwarg level, and not
            bundled inside subordinate dict passed in as the single `connect_args`),
            we need to take what we find inside `create_engine_kwargs['connect_args']`
            and promote them all to be at the toplevel of create_engine_kwargs.

            (The whole create_engine_kwargs concept into sql.connection.Connection.set
             was created exactly to solve this BQ issue. It used to only accept
             connect_args, 'cause apparently the open source ipython-sql maintainer
             had not yet waged war against sqlalchemy-bigquery)

        * 2) We need to interpret/demangle `['credential_file_contents']`:
                * Will come to us as b64 encoded json.
                * We need to save it out as a deencoded file, then
                * set new `['credentials_path']` entry naming its pathname, per
                https://github.com/googleapis/python-bigquery-sqlalchemy#authentication

    """

    # 1)...
    bq_args = create_engine_kwargs.pop('connect_args')
    create_engine_kwargs.update(bq_args)

    # 2)...
    # (conditionalized for time being until Gate / the datasource-type jsonschema
    #  change happens to start sending us this key. We might have a stale BQ datasource
    #  in integration already and let's not break kernel startup in that space in mean time)

    if 'credential_file_contents' in create_engine_kwargs:
        # 2.1. Pop out and un-b64 it.
        encoded_contents = create_engine_kwargs.pop('credential_file_contents')
        contents: bytes = b64decode(encoded_contents)

        # 2.2. Write out to a file based on datasource_id (user could have multiple BQ datasources!)
        path = Path('/tmp') / f'{datasource_id}_bigquery_credentials.json'
        with path.open('wb') as outfile:
            outfile.write(contents)

        # 2.3. Record pathname as new key in create_engine_kwargs. Yay, BQ connections
        # might work now!
        create_engine_kwargs['credentials_path'] = path.as_posix()


@register_postprocessor('snowflake')
def postprocess_snowflake(
    datasource_id: str, dsn_dict: Dict[str, str], create_engine_kwargs: Dict[str, Any]
) -> None:
    """Format database + possible schema from dsn_dict into new value for database.

    Snowflake dsn_dict may end up with a 'schema' member in dsn_dict. If present, we should pop
    it out and reformat database to be 'f{database}/{schema}', in that ipython-sql will
    end up using good old base class sqlalchemy.engine.URL to digest this dict.

    https://github.com/snowflakedb/snowflake-sqlalchemy#connection-parameters

    """

    if 'schema' in dsn_dict:
        schema = dsn_dict.pop('schema')
        db = dsn_dict['database']

        dsn_dict['database'] = f'{db}/{schema}'


@register_postprocessor('sqlite')
def postprocess_sqlite(
    datasource_id: str, dsn_dict: Dict[str, str], create_engine_kwargs: Dict[str, Any]
) -> None:
    """Expect to have either a URL provided which we should download and place into TMPDIR as the database file,
    or no such thing implying to run in memory mode.
    """

    if 'database' in dsn_dict:
        cur_path = dsn_dict['database']
        if cur_path == '' or cur_path == ':memory:':
            # Empty path is alias for :memory:, and is fine.
            return

        # Hint as to how long to allow downloading database file could come
        # in the create_engine_args. It is meant for us here, not actually for
        # passing along through to sqlalchemy.create_engine().

        # Pop this out of there (and default it) regardless of if the database name implies to download.
        connect_args = create_engine_kwargs.get('connect_args', {})
        max_download_seconds = int(connect_args.pop('max_download_seconds', 10))

        # If it smells like a URL, we should download, stash it into a tmpfile,
        # and respell dsn_dict['database'] to point to that file. Any exceptions in here
        # will spoil the datasource.
        parsed = urlparse(dsn_dict['database'])

        # (Sigh, 'mock' as scheme due to cannot cleanly pytest requests-mock http or https urls
        #  for reasons I trust from the requests-mocks docs)
        if parsed.scheme in ('http', 'https', 'ftp', 'mock'):
            logger.info(
                'Downloading sqlite database initial contents',
                datasource_id=datasource_id,
                database_url=dsn_dict['database'],
                max_download_seconds=max_download_seconds,
            )

            resp = requests.get(dsn_dict['database'], stream=True, timeout=max_download_seconds)

            resp.raise_for_status()

            # Save to a durable tmpfile
            with NamedTemporaryFile(delete=False) as outf:
                for chunk in resp.iter_content(chunk_size=2048):
                    outf.write(chunk)

            # Point to the resulting file.
            dsn_dict['database'] = cur_path = outf.name

        # The database file should resolve to somewhere /tmp-y (for now)
        # (Why not use Path.is_relative_to, you ask? 'Cause of ancient python 3.8, that's why.)
        allowed_parents = ['/tmp']
        if os.environ.get('TMPDIR'):
            # And also TMPDIR, which might not be in /tmp.
            #
            # On OSX, /var is symlink to /private/var, so to get test suite passing
            # need to canonicalize the path so the .startswith() test will work.
            # (on OSX at least under pytest, the NamedTemporaryFile above will be
            # something like /var/tmp/... , which is really /private/var/tmp/...)
            allowed_parents.append(str(Path(os.environ.get('TMPDIR')).resolve()))

        requested = str(Path(cur_path).resolve())

        if not any(requested.startswith(allowed_parent) for allowed_parent in allowed_parents):
            raise ValueError(
                f'SQLite database files should be located within /tmp, got "{cur_path}"'
            )


@register_postprocessor('awsathena')
def postprocess_awsathena(
    datasource_id: str, dsn_dict: Dict[str, str], create_engine_kwargs: Dict[str, Any]
) -> None:
    """Postprocess awsathena details:

        1. Host will be just the region name. Expand to -> athena.{region_name}.amazonaws.com
        2. Username + password will be AWS access key id + secret value. Needs to be quote_plus protected.
        3. Likewise the 's3_staging_dir' present in create_engine_kwargs.

    See https://github.com/laughingman7743/PyAthena/
    """

    # 1. Flesh out host
    dsn_dict['host'] = f"athena.{dsn_dict['host']}.amazonaws.com"

    # 2. quote_plus username / password
    dsn_dict['username'] = quote_plus(dsn_dict['username'])
    dsn_dict['password'] = quote_plus(dsn_dict['password'])

    # 3. quote_plus s3_staging_dir
    create_engine_kwargs['s3_staging_dir'] = quote_plus(create_engine_kwargs['s3_staging_dir'])


DATABRICKS_CONNECT_SCRIPT_TIMEOUT = 10  # seconds


@register_postprocessor('databricks+connector')
def postprocess_databricks(
    datasource_id: str, dsn_dict: Dict[str, str], create_engine_kwargs: Dict[str, Any]
) -> None:
    """ENG-5517: If cluser_id is present, and `databricks-connect` is in the path, then
    set up and run it.

    Also be sure to purge cluster_id, org_id, port from connect_args portion of create_engine_kwargs,
    in that these fields were added for only going into this side effect."""

    cluster_id_key = 'cluster_id'
    connect_file_opt_keys = [cluster_id_key, 'org_id', 'port']

    # Collect data to drive databricks-connect if we've got a cluster_id and script is in $PATH.
    connect_args = create_engine_kwargs['connect_args']
    # Only wanted for getting connect_args. Any additional dereferencing is a bug.
    del create_engine_kwargs

    if cluster_id_key in connect_args and shutil.which('databricks-connect'):
        # host, token (actually, our password field) come from dsn_dict.
        # (and what databricks-connect wants as 'host' is actually a https:// URL. Sigh.)
        args = {
            'host': f'https://{dsn_dict["host"]}/',
            'token': dsn_dict['password'],
        }
        for key in connect_file_opt_keys:
            if key in connect_args:
                args[key] = connect_args[key]

        connect_file_path = Path(os.environ['HOME']) / '.databricks-connect'

        # rm -f any preexisting file.
        if connect_file_path.exists():
            connect_file_path.unlink()

        p = Popen(['databricks-connect', 'configure'], stdout=PIPE, stdin=PIPE, stderr=PIPE)
        try:
            _stdout, stderr = p.communicate(
                # Indention fugly so as to not prefix each input with whitespace.
                # And oh, be sure to have a newline betwen each input into the 'interactive' script.
                input=f"""y
{args['host']}
{args['token']}
{args[cluster_id_key]}
{args['org_id']}
{args['port']}""".encode(),
                timeout=DATABRICKS_CONNECT_SCRIPT_TIMEOUT,
            )
        except TimeoutExpired:
            raise ValueError(
                f'databricks-connect took longer than {DATABRICKS_CONNECT_SCRIPT_TIMEOUT} seconds to complete.'
            )

        if p.returncode != 0:
            # Failed to exectute the script. Raise an exception.
            raise ValueError(
                "Failed to execute databricks-connect configure script: " + stderr.decode()
            )

    # Always be sure to purge these only-for-databricks-connect file args from connect_args,
    # even if not all were present.
    for key in connect_file_opt_keys:
        connect_args.pop(key, '')


@register_postprocessor('clickhouse+http')
def postprocess_clickhouse(
    datasource_id: str, dsn_dict: Dict[str, str], create_engine_kwargs: Dict[str, Any]
) -> None:
    connect_args = create_engine_kwargs["connect_args"]

    # These are the enum options from the JSON schema for the dropdown titled "Secure Connection (HTTPS)"
    # Convert them to the values that the clickhouse driver expects.
    verify = connect_args["verify"]
    if verify == "Yes, use HTTPS":
        connect_args.update({"protocol": "https", "verify": False})
    elif verify == "Yes, use HTTPS and verify server certificate":
        connect_args.update({"protocol": "https", "verify": True})
    elif verify == "No, use HTTP":
        connect_args.update({"protocol": "http", "verify": False})
