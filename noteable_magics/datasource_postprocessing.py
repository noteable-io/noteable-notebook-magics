import functools
from base64 import b64decode
from pathlib import Path
from typing import Any, Callable, Dict

# Dict of drivername -> post-processor function that accepts (datasource_id, create_engine_kwargs
# dict) pair and is expected to mutate create_engine_kwargs as needed.
post_processor_by_drivername: Dict[str, Callable[[str, dict, Path], None]] = {}


def register_postprocessor(drivername: str):
    """Decorator to register a create_engine_kwargs post-processor"""

    def decorator_outer(func):
        @functools.wraps(func)
        def decorator_inner(*args, **kwargs):
            func(*args, **kwargs)

        assert drivername not in post_processor_by_drivername, f'{drivername} already registered!'

        post_processor_by_drivername[drivername] = func

        return decorator_inner

    return decorator_outer


##
# create_engine_kwargs post-processors needed to do driver-centric post-processing of create_engine_kwargs
# All these should take create_engine_kwargs dict as input and modify it in place, returning None.
##


@register_postprocessor('bigquery')
def postprocess_bigquery(datasource_id: str, create_engine_kwargs: Dict[str, Any]) -> None:
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
        path = _determine_writeable_path(f'{datasource_id}_bigquery_credentials.json')
        with path.open('wb') as outfile:
            outfile.write(contents)

        # 2.3. Record pathname as new key in create_engine_kwargs. Yay, BQ connections
        # might work now!
        create_engine_kwargs['credentials_path'] = path.as_posix()


def _determine_writeable_path(filename: str) -> Path:
    """Determine a writeable pathname to store a file by this name.

    When being run inside noteable kernels, /vault/secrets will exist and be
    writeable. Otherwise err on /tmp.
    """
    for possible_parent in [Path('/vault/secrets'), Path('/tmp')]:
        if possible_parent.is_dir():
            fully_qualified_attempt = possible_parent / filename
            try:
                with fully_qualified_attempt.open('w'):
                    pass
                return fully_qualified_attempt
            except Exception:
                continue

    raise ValueError('Cannot determine a suitable place where I can write files!')
