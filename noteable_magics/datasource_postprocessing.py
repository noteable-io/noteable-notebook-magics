from base64 import b64decode
from pathlib import Path
from typing import Any, Callable, Dict

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
    """Reject if named a path outside of either /tmp or the project itself. Do allow :memory:, however!"""

    if 'database' in dsn_dict:
        cur_path = dsn_dict['database']
        if cur_path == '' or cur_path == ':memory:':
            # Empty path is alias for :memory:, and is fine.
            return

        # Otherwise the database file should resolve to somewhere within the
        # current working directory (the project) or /tmp.

        allowed_parents = [str(Path.cwd()), '/tmp']
        requested = str(Path(cur_path).resolve())

        if not any(requested.startswith(allowed_parent) for allowed_parent in allowed_parents):
            raise ValueError(
                f'SQLite database files should be located within either the project or in /tmp, got "{cur_path}"'
            )
