from __future__ import annotations

import os
import shutil
from base64 import b64decode
from pathlib import Path
from subprocess import PIPE, Popen, TimeoutExpired
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus, urlparse

import certifi
import requests
import sqlalchemy
import structlog
from sqlalchemy.engine import URL, CursorResult, Dialect

from noteable.sql.connection import BaseConnection, ResultSet, connection_class

logger = structlog.get_logger(__name__)


class SQLAlchemyResult(ResultSet):
    """
    Results of a query from SQLAlchemy.
    """

    # Result of a SELECT or perhaps INSERT INTO ... RETURNING projecting a result set.
    keys: Optional[List[str]] = None
    rows: Optional[list] = None

    # In case of an INSERT, UPDATE, or DELETE statement.
    rowcount: Optional[int] = None

    has_results_to_report: bool = True

    def __init__(self, sqla_result: CursorResult):
        # Check for non-empty list of keys in addition to returns_rows flag.

        # NOTE: Clickhouse does funky things with INSERT/UPDATE/DELETE statements
        #       and sets returns_rows to True even though there are no results or keys.
        #       We don't want to report results in that case.
        if sqla_result.returns_rows and len(keys := list(sqla_result.keys())) > 0:
            self.keys = keys
            self.rows = sqla_result.fetchall()
        elif sqla_result.rowcount != -1:
            # Was either DDL or perhaps DML like an INSERT or UPDATE statement
            # that just talks about number or rows affected server-side.
            self.rowcount = sqla_result.rowcount
        else:
            # CREATE TABLE or somesuch DDL that ran successfully and offers
            # no constructive feedback whatsoever.
            self.has_results_to_report = False


class SQLAlchemyConnection(BaseConnection):
    """Base class for all SQLAlchemy-based Connection implementations. Each type _must_ make
    and register a subclass, at very least to define value for cls.needs_explicit_commit"""

    needs_explicit_commit: bool
    """Will there be an implicit transaction open which demands commit()ing between execute() calls?"""

    is_sqlalchemy_based: bool = True
    """Is this connection type implemented on top of SQLAlchemy?"""

    def __init__(
        self,
        cell_handle: str,
        metadata: Dict[str, Any],
        dsn_dict: Dict[str, Any],
        create_engine_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        Construct a new 'connection', which in reality is a sqla Engine
        plus some convienent metadata.

        Common args to go into the create_engine call (and therefore need to be
        passed in within `create_engine_kwargs`) include:

          * create_engine_kwargs: SQLA will pass these down to its call to create the DBAPI-level
                            connection class when new low-level connections are
                            established.

        No SQLA-level connection is immediately established (see the `sqla_connection` property).

        'name' is what we call now the 'sql_cell_handle' -- starts with '@', followed by
        the hex of the datasource uuid (usually -- the legacy "local database" (was sqlite, now duckdb)
        and bigquery do not use the hex convention because they predate datasources)

        'human_name' is the name that the user gave the datasource ('My PostgreSQL Connection')
        (again, only for real datasource connections). There's a slight risk of name collision
        due to having the same name used between user and space scopes, but so be it.

        """

        if not create_engine_kwargs:
            create_engine_kwargs = {}

        human_name = metadata['name']

        super().__init__(cell_handle, human_name)

        connection_url = str(URL.create(**dsn_dict))

        self._engine = sqlalchemy.create_engine(connection_url, **create_engine_kwargs)

        self._create_engine_kwargs = create_engine_kwargs  # Retained for test suite purposes.

    def execute(self, statement: str, bind_dict: Dict[str, Any]) -> ResultSet:
        """Execute this statement, possibly interpolating the values in bind_dict"""

        sqla_connection = self.sqla_connection

        result = sqla_connection.execute(sqlalchemy.sql.text(statement), bind_dict)

        if self.needs_explicit_commit:
            sqla_connection.execute("commit")

        return SQLAlchemyResult(result)

    def close(self):
        """Close any resources currently allocated to this connection"""
        if self._sqla_connection:
            self._sqla_connection.close()
        self.reset_connection_pool()

    @property
    def sqla_engine(self) -> sqlalchemy.engine.base.Engine:
        return self._engine

    @property
    def dialect(self) -> Dialect:
        return self.sqla_engine.url.get_dialect()

    @property
    def dialect_name(self) -> str:
        return self.dialect.name

    _sqla_connection: Optional[sqlalchemy.engine.base.Connection] = None

    @property
    def sqla_connection(self) -> sqlalchemy.engine.base.Connection:
        """Lazily connect to the database. Return a SQLA Connection object, or die trying."""

        if not self._sqla_connection:
            self._sqla_connection = self.sqla_engine.connect()

        return self._sqla_connection

    def reset_connection_pool(self):
        """Reset the SQLA connection pool, such as after an exception suspected to indicate
        a broken connection has been raised.
        """
        self._engine.dispose()
        self._sqla_connection = None

    @classmethod
    def preprocess_configuration(
        cls, datasource_id: str, dsn_dict: Dict[str, Any], create_engine_kwargs: Dict[str, Any]
    ) -> None:
        """Classmethod to allow the Connection subclass to do any late-stage conversion or
        processing based on information present in these dicts prior to construction time.

        Should modify the dicts in place
        """

        pass


###
# Now come all of the SQLAlchemy-based implementations that needed to override some behavior.
# Please keep in alphabetical order.
###


@connection_class('awsathena+rest')
class AwsAthenaConnection(SQLAlchemyConnection):
    needs_explicit_commit: bool = False

    @classmethod
    def preprocess_configuration(
        cls, datasource_id: str, dsn_dict: Dict[str, Any], create_engine_kwargs: Dict[str, Any]
    ) -> None:
        """Postprocess awsathena details:

            1. Host will be just the region name. Expand to -> athena.{region_name}.amazonaws.com
            2. Username + password will be AWS access key id + secret value. Needs to be quote_plus protected.

        See https://github.com/laughingman7743/PyAthena/
        """

        # 1. Flesh out host
        dsn_dict['host'] = f"athena.{dsn_dict['host']}.amazonaws.com"

        # 2. quote_plus username / password
        dsn_dict['username'] = quote_plus(dsn_dict['username'])
        dsn_dict['password'] = quote_plus(dsn_dict['password'])


@connection_class('bigquery')
class BigQueryConnection(SQLAlchemyConnection):
    needs_explicit_commit: bool = False

    @classmethod
    def preprocess_configuration(
        cls, datasource_id: str, dsn_dict: Dict[str, Any], create_engine_kwargs: Dict[str, Any]
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

        # XXX todo: create_engine_kwargs can take the b64-encoded value. Find out that
        # kwarg name and switch to using that instead of scribbling in tmpfile.

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


@connection_class('clickhouse+http')
class ClickhouseConnection(SQLAlchemyConnection):
    needs_explicit_commit: bool = False

    @classmethod
    def preprocess_configuration(
        cls, datasource_id: str, dsn_dict: Dict[str, Any], create_engine_kwargs: Dict[str, Any]
    ) -> None:
        connect_args = create_engine_kwargs["connect_args"]

        # These are the enum options from the JSON schema for the dropdown titled "Secure Connection (HTTPS)"
        # Convert them to the values that the clickhouse driver expects.
        secure_connection = connect_args.pop("secure_connection")
        if secure_connection == "Yes, use HTTPS":
            query = {"protocol": "https", "verify": "False"}
        elif secure_connection == "Yes, use HTTPS and verify server certificate":
            query = {"protocol": "https", "verify": certifi.where()}
        elif secure_connection == "No, use HTTP":
            query = {"protocol": "http", "verify": "False"}
        else:
            raise ValueError(
                f"Unexpected value for secure_connection: {secure_connection}. "
                "Expected one of: "
                '"Yes, use HTTPS", '
                '"Yes, use HTTPS and verify server certificate", '
                '"No, use HTTP"'
            )

        # https://clickhouse-sqlalchemy.readthedocs.io/en/latest/connection.html#http
        # The `protocol` and `verify` options need to be passed as
        # query parameters to the URL and not as connect_args to create_engine.
        # It simply doesn't work if they are passed as connect_args.
        # Why? That is left as an exercise for the reader.
        dsn_dict["query"] = query


@connection_class('databricks+connector')
class DatabricksConnection(SQLAlchemyConnection):
    needs_explicit_commit: bool = False

    DATABRICKS_CONNECT_SCRIPT_TIMEOUT = 10

    @classmethod
    def preprocess_configuration(
        cls, datasource_id: str, dsn_dict: Dict[str, Any], create_engine_kwargs: Dict[str, Any]
    ) -> None:
        """ENG-5517: If cluser_id is present, and `databricks-connect` is in the path, then
        set up and run it.

        Also be sure to purge cluster_id, org_id, port from connect_args portion of create_engine_kwargs,
        in that these fields were added for only going into this side effect.
        """

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
                    # Indention ugly so as to not prefix each input with whitespace.
                    # And oh, be sure to have a newline betwen each input into the 'interactive' script.
                    input=f"""y
{args['host']}
{args['token']}
{args[cluster_id_key]}
{args['org_id']}
{args['port']}""".encode(),
                    timeout=cls.DATABRICKS_CONNECT_SCRIPT_TIMEOUT,
                )
            except TimeoutExpired:
                raise ValueError(
                    f'databricks-connect took longer than {cls.DATABRICKS_CONNECT_SCRIPT_TIMEOUT} seconds to complete.'
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


@connection_class('duckdb')
class DuckDBConnection(SQLAlchemyConnection):
    needs_explicit_commit = False


@connection_class('mysql+pymysql')
@connection_class('mysql+mysqldb')
@connection_class('singlestoredb')
class MySQLFamilyConnection(SQLAlchemyConnection):
    """Base class for all SQLAlchemy-based Connection implementations"""

    needs_explicit_commit: bool = False


@connection_class('mssql+pyodbc')
class MsSqlConnection(SQLAlchemyConnection):
    needs_explicit_commit: bool = False

    @classmethod
    def preprocess_configuration(
        cls, datasource_id: str, dsn_dict: Dict[str, Any], create_engine_kwargs: Dict[str, Any]
    ) -> None:
        connect_args = create_engine_kwargs["connect_args"]

        # If the user has asked to verify the server certificate, then we should not trust it
        # (i.e. set TrustServerCertificate=no), and vice versa.
        # It's a bit counterintuitive, but that's how it works.
        # https://learn.microsoft.com/en-us/sql/relational-databases/native-client-odbc-api/sqlsetconnectattr?view=sql-server-ver16#sql_copt_ss_trust_server_certificate
        if connect_args.pop("verify"):
            connect_args["TrustServerCertificate"] = "no"
        else:
            connect_args["TrustServerCertificate"] = "yes"

        # Static options that are always set.
        connect_args.update(
            {
                # This is the driver package installed in the polymorph base image - msodbcsql18
                "Driver": "ODBC Driver 18 for SQL Server",
                # https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver16#authentication---sql_copt_ss_authentication
                # SQL Server authentication with username and password.
                "Authentication": "SqlPassword",
                # Default for ODBC Driver 18+
                "Encrypt": "yes",
            }
        )


@connection_class('cockroachdb')
@connection_class('postgresql')
class PostgreSQLConnection(SQLAlchemyConnection):
    needs_explicit_commit = False
    _installed_psycopg2_interrupt_fix: bool = False

    @classmethod
    def preprocess_configuration(
        cls, datasource_id: str, dsn_dict: Dict[str, Any], create_engine_kwargs: Dict[str, Any]
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

        cls._install_psycopg2_interrupt_fix()

    @classmethod
    def _install_psycopg2_interrupt_fix(cls):
        if not cls._installed_psycopg2_interrupt_fix:
            import psycopg2.extensions
            import psycopg2.extras

            psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)

            cls._installed_psycopg2_interrupt_fix = True


@connection_class('redshift+redshift_connector')
class Redshift(SQLAlchemyConnection):
    needs_explicit_commit: bool = True


@connection_class('snowflake')
class SnowflakeConnection(SQLAlchemyConnection):
    needs_explicit_commit: bool = True

    @classmethod
    def preprocess_configuration(
        cls, datasource_id: str, dsn_dict: Dict[str, Any], create_engine_kwargs: Dict[str, Any]
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


@connection_class('sqlite')
class SQLiteConnection(SQLAlchemyConnection):
    needs_explicit_commit = False

    @classmethod
    def preprocess_configuration(
        cls, datasource_id: str, dsn_dict: Dict[str, Any], create_engine_kwargs: Dict[str, Any]
    ) -> None:
        """Expect to have either a URL provided which we should download and place into TMPDIR as the database file,
        or no such thing implying to run in memory mode.
        """

        # Pop this out of there (and default it) regardless of if the database name implies to download.
        connect_args = create_engine_kwargs.get('connect_args', {})

        # Hint as to how long to allow downloading database file could come
        # in the create_engine_args. It is meant for us here, not actually for
        # passing along through to sqlalchemy.create_engine().
        max_download_seconds = int(connect_args.pop('max_download_seconds', 10))

        if 'database' in dsn_dict:
            cur_path = dsn_dict['database']
            if cur_path == '' or cur_path == ':memory:':
                # Empty path is alias for :memory:, and is fine.
                return

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


@connection_class('trino')
class TrinoConnection(SQLAlchemyConnection):
    """Trino connection type"""

    needs_explicit_commit: bool = False
