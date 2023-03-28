from __future__ import annotations

import pathlib
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from uuid import UUID

import requests
from IPython.core.interactiveshell import InteractiveShell
from IPython.display import HTML, display
from pandas import DataFrame
from sqlalchemy import inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import NoSuchTableError

from noteable_magics.sql.connection import Connection
from noteable_magics.sql.gate_messaging_types import (
    CheckConstraintModel,
    ColumnModel,
    ForeignKeysModel,
    IndexModel,
    RelationKind,
    RelationStructureDescription,
    UniqueConstraintModel,
)

__all__ = ['MetaCommandException', 'run_meta_command']


class MetaCommandException(Exception):
    """General exception when evaluating SQL cell meta commands"""

    pass


class MetaCommandInvocationException(MetaCommandException):
    r"""Invoked a specific meta command incorrectly.

    Will trigger sql-magic print() to suggest '\help \<subcommand>'
    """

    invoked_with: Optional[str] = None

    def __init__(self, *args, invoked_with, **kwargs):
        self.invoked_with = invoked_with
        super().__init__(*args, **kwargs)


class MetaCommand:
    r"""Base class for family of metadata commands to do operations like schema-introspection, etc.
    within SQL cells.

    The docstring of each concrete subclass will be 'published' as a part of the documentation
    exposed by "\help" in a SQL cell, so keep 'em end-user friendly.

    (See HelpCommand grabbing .__doc__ from each of the concrete subclasses)
    """

    # One-line description of what this one does.
    description: str

    # List of strings that will invoke this subclass.
    # Primary human-readable and memorable invocation string should come first, then any shorthand
    # aliases. See how global _registry is populated towards bottom of
    invokers: List[str]

    # Does this command accept additional arguments?
    accepts_args: bool

    # What variable name to assign the 'primary' output to, if any
    assign_to_varname: Optional[str]

    # Should this class's command be documented by \help?
    include_in_help = True

    def __init__(self, shell: InteractiveShell, conn: Connection, assign_to_varname: Optional[str]):
        self.shell = shell
        self.conn = conn
        self.assign_to_varname = assign_to_varname

    def do_run(self, invoked_as: str, args: List[str]):
        """Call down into subclass's `run()`, call `display()` if needed,
        and assign to variable.
        """

        df, need_display_call = self.run(invoked_as, args)

        if need_display_call:
            display(df)

        # Make the assignment(s) into the user's namespace.
        # The subclass's run() will have already called display()
        # on this and possibly other dataframes already, but this
        # returned one is the 'primary' return result from the meta
        # command.

        if self.assign_to_varname:
            self.shell.user_ns[self.assign_to_varname] = df

        # This ... may well get overwritten by the ultimate result of the magic
        # returning None, which gets handled later than this.
        self.shell.user_ns['_'] = df

    def run(self, invoked_as: str, args: List[str]) -> Tuple[DataFrame, bool]:
        """Implement the meta command.

        Subclass should return a pair of the 'primary' dataframe, and bool for if display() needs
        to be called with it or not.
        """
        raise NotImplementedError

    def get_inspector(self) -> SchemaStrippingInspector:
        engine = self.conn._engine
        underlying_inspector = inspect(engine)

        # BigQuery dialect inspector at least curiously includes schema name + '.'
        # in the relation name portion of the results of get_table_names(), get_view_names(),
        # which then breaks code in our SingleRelationCommand (describe structure of single table/view)
        # So, wrap with a proxy Inspector implementation which ensures that those two methods
        # will not return relation names of the form 'schema.relation'.

        # (Why do this unconditionally, and not just for when engine.name == 'bigquery'?
        #  Because we want our test suite to cover all methods of this implementation, but
        #  we don't explicitly test against BigQuery directly in the test suite. Non-BigQuery
        #  dialects will return table and view name lists w/o the schema prefix prepended, so
        #  will just cost us an iota more CPU in exchange for greater confidence)

        return SchemaStrippingInspector(underlying_inspector)


class SchemasCommand(MetaCommand):
    """List all the schemas within the database."""

    """\nIf invoked with trailing '+', will also include the count of tables and views within each schema."""

    description = "List schemas within database."
    invokers = ['\\schemas', '\\schemas+', '\\dn', '\\dn+']
    accepts_args = False

    def run(self, invoked_as: str, args: List[str]) -> Tuple[DataFrame, bool]:
        insp = self.get_inspector()

        default_schema = insp.default_schema_name
        all_schemas = sorted(insp.get_schema_names())

        # Want to have default schema always come first regardless of alpha order
        # (alas, some dialects like Trino, BigQuery do not know of a distinguished 'default' schema, so be cautious)
        # (default_schema / insp.default_schema_name will be None)
        if default_schema in all_schemas:
            all_schemas.remove(default_schema)
            all_schemas.insert(0, default_schema)

        data = {'Schema': all_schemas, 'Default': [sn == default_schema for sn in all_schemas]}

        if invoked_as.endswith('+'):
            # Want extra info -- namely, the table + view counts.
            table_counts = []
            view_counts = []

            for sn in all_schemas:
                table_names = set(insp.get_table_names(sn))
                view_names = insp.get_view_names(sn)

                # Some dialects (lookin' at you, CRDB) return view names as both view
                # names and table names. Sigh. We'd like to only return the counts of
                # the definite tables, though, so ...
                if view_names:
                    # Remove any view names from our pristeen list of table names.
                    table_names.difference_update(view_names)

                table_counts.append(len(table_names))
                view_counts.append(len(view_names))

            data['Table Count'] = table_counts
            if any(view_counts):
                # Only optionally project a 'View Count' dataframe column if there are any views.
                data['View Count'] = view_counts

        return DataFrame(data=data), True


class RelationsCommand(MetaCommand):
    """List all the relations (tables and views) within one or more schemas of the database."""

    description = "List names of tables and/or views within database."
    invokers = [r'\list', r'\dr']
    accepts_args = True

    def run(self, invoked_as: str, args: List[str]) -> Tuple[DataFrame, bool]:
        if len(args) > 1:
            raise MetaCommandException(f'Usage: {invoked_as} [[schema pattern].[table pattern]]')

        if not args:
            # All relations in the default schema.
            args = ['*']

        return relation_names(self.get_inspector(), args[0])


class TablesCommand(MetaCommand):
    """List all the tables (not views) within one or more schemas of a database."""

    description = "List names of tables within database."
    invokers = [r'\tables', r'\dt']
    accepts_args = True

    def run(self, invoked_as: str, args: List[str]) -> Tuple[DataFrame, bool]:
        if len(args) > 1:
            raise MetaCommandException(f'Usage: {invoked_as} [[schema pattern].[table pattern]]')

        if not args:
            # All tables in the default schema.
            args = ['*']

        return relation_names(self.get_inspector(), args[0], include_views=False)


class ViewsCommand(MetaCommand):
    """List all the views (not tables) within one or more schemas of the database."""

    description = "List names of views within database."
    invokers = [r'\views', r'\dv']
    accepts_args = True

    def run(self, invoked_as: str, args: List[str]) -> Tuple[DataFrame, bool]:
        if len(args) > 1:
            raise MetaCommandException(f'Usage: {invoked_as} [[schema pattern].[view pattern]]')

        if not args:
            # All views in the default schema.
            args = ['*']

        return relation_names(self.get_inspector(), args[0], include_tables=False)


def relation_names(
    inspector: SchemaStrippingInspector,
    argument: str,
    include_tables=True,
    include_views=True,
) -> Tuple[DataFrame, bool]:
    """Determine relation names (or perhaps only specifically either views or tables) in one or more
    schemas.

    `argument` is expected to be either:
        A) a dot-separated schema (glob) and relation name glob, at which schemas matching the
            glob will be considered, and then relations matching the glob are matched and returned.
            To match against all schemas, use '*' on the left hand of the dot.
        B) not containing a dot, which then implies 'only match relations from the default schema'

     Examples:
        'foo' -> Find all relations in default schema starting with 'foo'.
        'monkeypox.foo' -> Find all relations in 'monkeypox' schema starting with foo.
        'monkeypox.foo*' -> Find all relations in 'monkeypox' schema starting with foo.
        '*mon*.foo' -> Find all relations starting with 'foo' in schemas that have 'mon' as a subset of schema name.
        '*foo' -> Find all relations in default schema whose name ends with 'foo.'
        'foo??' -> Find all relations in default schema whose names start with 'foo' and only has two following letters in name.
    """
    schema_name_glob, relation_name_glob = parse_schema_and_relation_glob(inspector, argument)

    schema_name_filter = convert_relation_glob_to_regex(schema_name_glob)
    relation_name_filter = convert_relation_glob_to_regex(relation_name_glob, imply_prefix=True)

    schemas = sorted(s for s in inspector.get_schema_names() if schema_name_filter.match(s))

    # Parallel lists of schema, relation names for output
    output_schemas: List[str] = []
    output_relations: List[str] = []
    # One more possible output column if we're returning both tables and views
    if include_tables and include_views:
        relation_types: List[str] = []

    for schema in schemas:
        # Some dialects return views as tables (and then also as views), so distict-ify via a set.
        relations = set()
        if include_tables:
            relations.update(inspector.get_table_names(schema))

        view_names = inspector.get_view_names(schema)
        if include_views:
            relations.update(view_names)
        else:
            # Because some dialects may have already included view names in get_table_names(), need
            # to explicitly remove the definite view names. Thanks, guys.
            relations.difference_update(view_names)

        # Filter, sort, append schema, relname and possibly the kind onto respective lists.
        for relname in sorted(r for r in relations if relation_name_filter.match(r)):
            output_schemas.append(schema)
            output_relations.append(relname)
            if include_tables and include_views:
                # And also if is a view or a table, since we're returning both.
                relation_types.append('view' if relname in view_names else 'table')

    if include_tables and include_views:
        relation_colname = 'Relation'
    elif include_tables:
        relation_colname = 'Table'
    else:
        relation_colname = 'View'

    data = {
        'Schema': output_schemas,
        relation_colname: output_relations,
    }
    if include_tables and include_views:
        # Only need to project this column if possibly displaying more than one kind of relation
        data['Kind'] = relation_types

    # Return dataframe and need to have display() called on it.
    # (Not applying a title to the dataframe at this time because all of the possibilities
    #  are currently daunting -- 'Views in Schema "public" Matching "v_*"' and whatnot.)
    return (DataFrame(data), True)


def parse_schema_and_relation_glob(
    inspector: SchemaStrippingInspector, schema_and_possible_relation: str
) -> Tuple[str, str]:
    """Return tuple of schema name glob, table glob given a single string
    like '*', 'public.*', 'foo???', ...

    Input string will either be of general form <schema glob>.<relation glob>, which we separate apart,
    or if without a period, will imply 'use the default schema only'.

    Expects to be driven with at least a single character string.
    """

    if schema_and_possible_relation == '.':
        # Degenerate value. Treat like they meant wildcard
        schema_and_possible_relation = '*'

    if '.' in schema_and_possible_relation and schema_and_possible_relation:
        # Only break on the leftmost dot. User might have typed more than one.
        dot_loc = schema_and_possible_relation.index('.')
        schema = schema_and_possible_relation[:dot_loc]
        table_pat = schema_and_possible_relation[dot_loc + 1 :]  # skip the dot.
    else:
        # Schema is implied to be default
        schema = inspector.default_schema_name
        if not schema:
            # Some dialects don't declare the default schema name. Sigh. Go with 1st one returned?
            schema = inspector.get_schema_names()[0]
        table_pat = schema_and_possible_relation

    return (schema, table_pat)


# Only expect simple chars in schema/table names.
ALLOWED_CHARS_RE = re.compile(r'[a-zA-Z0-9_ ]')


def convert_relation_glob_to_regex(glob: str, imply_prefix=False) -> re.Pattern:
    """Convert a simple glob like 'foo*' or 'foo_??' from glob spelling to a regex, pessimistically.
    Only allow letters, numbers, underscore, and spaces to pass through from end-user string.

    If no glob chars are found (*, ?), then we interpret this as a prefix match
    """
    buf = []
    found_glob_char = False
    for char in glob:
        if ALLOWED_CHARS_RE.match(char):
            buf.append(char)
        elif char == '*':
            # Glob spelling '*' -> regex spelling '.*'
            buf.append('.*')
            found_glob_char = True
        elif char == '?':
            # Glob spelling '?' -> regex spelling '.'
            buf.append('.')
            found_glob_char = True

    if not found_glob_char and imply_prefix:
        # Implied prefix matching only.
        buf.append('.*')

    return re.compile(''.join(buf))


class SingleRelationCommand(MetaCommand):
    """Describe a single relation."""

    description = "Show the structure of a single relation."
    invokers = [r'\describe', r'\d']
    accepts_args = True

    def run(self, invoked_as: str, args: List[str]) -> Tuple[DataFrame, bool]:
        if len(args) > 1:
            raise MetaCommandException(f'Usage: {invoked_as} [[schema].[relation_name]]')

        if len(args) == 0:
            # Kick over to showing all relations in the default schema, like PG does.
            alt_cmd = RelationsCommand(self.shell, self.conn, self.assign_to_varname)
            return alt_cmd.run('\\list', ['*'])

        schema, relation_name = self._split_schema_table(args[0])

        inspector = self.get_inspector()

        try:
            # In some dialects (BigQuery), this will raise NoSuchTableError if
            # the table doesn't exist. Yay, sane.

            # On some dialects (sigh, CockroachDB, what are you doing??),
            # this call may succeed returning empty list even if
            # the named relation does not exist. But the call to get_pk_constraint()
            # down below will then raise NoSuchTableError.
            column_dicts = inspector.get_columns(relation_name, schema=schema)
        except NoSuchTableError:
            self._raise_from_no_such_table(schema, relation_name)

        # 'Pivot' the dicts from get_columns()
        names = []
        types = []
        nullables = []
        defaults = []
        comments = []

        for col in column_dicts:
            names.append(col['name'])

            # Convert the possibly db-centric TypeEngine instance to a sqla-generic type string
            try:
                type_name = str(col['type'].as_generic()).lower()
            except NotImplementedError:
                # ENG-5268: More esoteric types like UUID do not implement .as_generic()
                type_name = str(col['type']).replace('()', '').lower()

            types.append(type_name)
            nullables.append(col['nullable'])
            defaults.append(col['default'])
            if 'comment' in col:
                # Dialect may not return this attribute at all. If supported by dialect,
                # but not present on this column, should be the empty string.
                comments.append(col['comment'])

        # Assemble dataframe out of data, conditionally skipping columns if inappropriate
        # or zero-value.

        data = {'Column': names, 'Type': types, 'Nullable': nullables}

        # Only include either of these if not all None / null.
        if any(defaults):
            data['Default'] = defaults

        if any(comments):
            data['Comment'] = comments

        displayable_rname = displayable_relation_name(schema, relation_name)

        if is_view := relation_name in inspector.get_view_names(schema):
            rtype = 'View'
        else:
            rtype = 'Table'

        main_relation_df = set_dataframe_metadata(
            DataFrame(data=data), title=f'{rtype} "{displayable_rname}" Structure'
        )

        # Keep a list of things to call display() on. Only do so at the very
        # end if we don't hit any exceptions.

        displayables = [main_relation_df]

        if is_view:
            view_definition = inspector.get_view_definition(relation_name, schema)

            # Would not surprise me if some dialects do not implement / return anything, so be cautious.
            if view_definition:
                display_view_name = displayable_relation_name(schema, relation_name)  # noqa: F841
                html_buf = []
                html_buf.append('<br />')
                html_buf.append('<h2>View Definition</h2>')
                html_buf.append('<br />')
                html_buf.append(f'<pre>{view_definition}</pre>')

                displayables.append(HTML('\n'.join(html_buf)))
        else:
            # Is a table. Let's go get indices, foreign keys, other table constraints.
            # If meaningful dataframe returned for any of these, transform to
            # HTML for presentation (DEX only expects at most a single DF display()ed per cell) and display it.
            for secondary_function in (
                index_dataframe,
                foreignkeys_dataframe,
                constraints_dataframe,
            ):
                secondary_df = secondary_function(inspector, relation_name, schema)
                if len(secondary_df):
                    displayables.append(secondary_dataframe_to_html(secondary_df))

        # Make it this far? Display what we should display.
        for displayable in displayables:
            display(displayable)

        return main_relation_df, False

    @staticmethod
    def _split_schema_table(schema_table: str) -> Tuple[Optional[str], str]:
        """Split 'foo.bar' into (foo, bar). Split 'foobar' into (None, foobar)"""
        if '.' in schema_table:
            dotpos = schema_table.index('.')
            schema = schema_table[:dotpos]
            table = schema_table[dotpos + 1 :]
        else:
            schema = None
            table = schema_table

        return (schema, table)


class IntrospectAndStoreDatabaseCommand(MetaCommand):
    """Introspect entire database, store each discovered table or view
    into Gate for front-end visualization and navigation."""

    description = "Introspect entire database and message Gate with the discovered results"
    invokers = [r'\introspect']
    accepts_args = False

    include_in_help = False

    # How many threads to use to introspect individual tables or views.
    MAX_INTROSPECTION_THREADS = 10

    # Schemas to never introspect into. Will need to augment / research for each
    # new datasource type supported.
    AVOID_SCHEMAS = set(('information_schema', 'pg_catalog', 'crdb_internal'))

    def run(self, invoked_as: str, args: List[str]) -> None:
        """Drive introspecting whole database, POSTing results back to Gate for storage.

        Not really intended for end-user interactive use. But being wired into an undocumented
        invocable command let us beta test things before E2E headless introspection was completed.
        """

        try:
            ds_id: UUID = self.get_datasource_id()
        except ValueError:
            # Tried to introspect a datasource whose kernel-side handle in the connections dict
            # wasn't coercable back into a UUID. This could be the case for the pre-datasource
            # legacy connections, '@noteable' for DuckDB or the legacy BigQuery connection.
            # Cannot continue, in that there's no place to store the results gate-side, since there's
            # no corresponding datasources row to relate to.

            # This will not happen in modern in any headless introspections -- both Gate and Geas
            # protect against headlessly introspecting the legacy datasources.

            print('Cannot introspect into this resource.', file=sys.stderr)
            return (None, False)

        # RelationStructureMessager handles both:
        #
        #   * Uploading batches of successfully discovered relations
        #   * Catching any exception and reporting it back to gate. Will suppress the exception.
        #
        with RelationStructureMessager(ds_id) as messenger:
            inspector = self.get_inspector()

            # This and delta() just for development timing figures. Could become yet another
            # timer context manager implementation.
            start = time.monotonic()

            def delta() -> float:
                """Record new timing section, kindof like a stopwatch lap timer.
                Returns the prior 'lap' time.
                """
                nonlocal start

                now = time.monotonic()
                ret = now - start
                start = now

                return ret

            relations_and_kinds = self.all_table_and_views(inspector)
            print(f'Discovered {len(relations_and_kinds)} relations in {delta()}')

            # Introspect each relation concurrently.
            # TODO: Take minimum concurrency as a param?
            with ThreadPoolExecutor(max_workers=self.MAX_INTROSPECTION_THREADS) as executor:
                future_to_relation = {
                    executor.submit(
                        self.fully_introspect, inspector, schema_name, relation_name, kind
                    ): (schema_name, relation_name, kind)
                    for (schema_name, relation_name, kind) in relations_and_kinds
                }

                for future in as_completed(future_to_relation):
                    schema_name, relation_name, kind = future_to_relation[future]
                    messenger.queue_for_delivery(future.result())

            table_introspection_delta = delta()
            print(
                f'Done introspecting and messaging gate in {table_introspection_delta}, amortized {table_introspection_delta / len(relations_and_kinds)}s per relation'
            )

        # run() contract: return what to bind to the SQL cell variable name, and if display() needs
        # to be called on it. Nothing and nope!
        return (None, False)

    ###
    # All of the rest of the methods end up assisting run(), directly or indirectly
    ###

    def all_table_and_views(self, inspector) -> List[Tuple[str, str, str]]:
        """Returns list of (schema name, relation name, table-or-view) tuples"""

        results = []

        default_schema = inspector.default_schema_name
        all_schemas = set(inspector.get_schema_names())
        all_schemas.difference_update(self.AVOID_SCHEMAS)
        if default_schema and default_schema not in all_schemas:
            all_schemas.add(default_schema)

        for schema_name in sorted(all_schemas):
            table_names = set(inspector.get_table_names(schema_name))
            view_names = inspector.get_view_names(schema_name)

            # Some dialects (lookin' at you, cockroach) return view names as both view
            # names and table names. Sigh. We'd like to only return the counts of
            # the definite tables, though, so ...
            if view_names:
                # Remove any view names from our pristeen list of table names.
                table_names.difference_update(view_names)

            for table_name in table_names:
                results.append((schema_name, table_name, 'table'))

            for view_name in view_names:
                results.append((schema_name, view_name, 'view'))

        return results

    def fully_introspect(
        self, inspector: SchemaStrippingInspector, schema_name: str, relation_name: str, kind: str
    ) -> RelationStructureDescription:
        """Drive introspecting into this single relation, making all the necessary Introspector API
        calls to learn all of the relation's sub-structures.

        Returns a RelationStructureDescription pydantic model, suitable to POST back to Gate with.
        """

        columns = self.introspect_columns(inspector, schema_name, relation_name)

        # Always introspect indexes, even if a view, because materialized views
        # can have indexes.
        indexes = self.introspect_indexes(inspector, schema_name, relation_name)

        # Likewise unique constraints? Those _might_ be definable on materialized views?
        unique_constraints = self.introspect_unique_constraints(
            inspector, schema_name, relation_name
        )

        if kind == 'view':
            view_definition = inspector.get_view_definition(relation_name, schema_name)
            primary_key_name = None
            primary_key_columns = []
            check_constraints = []
            foreign_keys = []
        else:
            view_definition = None
            primary_key_name, primary_key_columns = self.introspect_primary_key(
                inspector, relation_name, schema_name
            )
            check_constraints = self.introspect_check_constraints(
                inspector, schema_name, relation_name
            )

            foreign_keys = self.introspect_foreign_keys(inspector, schema_name, relation_name)

        print(f'Introspected {kind} {schema_name}.{relation_name}')

        return RelationStructureDescription(
            schema_name=schema_name,
            relation_name=relation_name,
            kind=RelationKind(kind),
            view_definition=view_definition,
            primary_key_name=primary_key_name,
            primary_key_columns=primary_key_columns,
            columns=columns,
            indexes=indexes,
            unique_constraints=unique_constraints,
            check_constraints=check_constraints,
            foreign_keys=foreign_keys,
        )

    def introspect_foreign_keys(
        self, inspector: SchemaStrippingInspector, schema_name: str, relation_name: str
    ) -> List[ForeignKeysModel]:
        """Introspect all foreign keys for a table, describing the results as a List[ForeignKeysModel]"""

        fkeys: List[ForeignKeysModel] = []

        fkey_dicts = inspector.get_foreign_keys(relation_name, schema_name)

        # Convert from SQLA foreign key dicts to our ForeignKeysModel. But beware,
        # Snowflake driver reports FKs with None for the target table's schema
        # (at least sometimes?) so in case then err on the referencing table's schema, because
        # ForeignKeysModel shared between us and Gate hate None for referred_schema.

        for fkey in sorted(fkey_dicts, key=lambda d: d['name']):
            fkeys.append(
                ForeignKeysModel(
                    name=fkey['name'],
                    referenced_schema=(
                        fkey['referred_schema']
                        if fkey['referred_schema'] is not None
                        else schema_name
                    ),
                    referenced_relation=fkey['referred_table'],
                    columns=fkey['constrained_columns'],
                    referenced_columns=fkey['referred_columns'],
                )
            )

        return fkeys

    def introspect_check_constraints(
        self, inspector, schema_name, relation_name
    ) -> List[CheckConstraintModel]:
        """Introspect all check constraints for a table, describing the results as a List[CheckConstraintModel]"""

        constraints: List[CheckConstraintModel] = []

        constraint_dicts = inspector.get_check_constraints(relation_name, schema_name)

        for constraint_dict in sorted(constraint_dicts, key=lambda d: d['name']):
            constraints.append(
                CheckConstraintModel(
                    name=constraint_dict['name'], expression=constraint_dict['sqltext']
                )
            )

        return constraints

    def introspect_unique_constraints(
        self, inspector, schema_name, relation_name
    ) -> List[UniqueConstraintModel]:
        """Introspect all unique constraints for a table, describing the results as a List[UniqueConstraintModel]"""

        constraints: List[UniqueConstraintModel] = []

        constraint_dicts = inspector.get_unique_constraints(relation_name, schema_name)

        for constraint_dict in sorted(constraint_dicts, key=lambda d: d['name']):
            constraints.append(
                UniqueConstraintModel(
                    name=constraint_dict['name'], columns=constraint_dict['column_names']
                )
            )

        return constraints

    def introspect_indexes(self, inspector, schema_name, relation_name) -> List[IndexModel]:
        """Introspect all indexes for a table or materialized view, describing the results as a List[IndexModel]"""
        indexes = []

        index_dicts = inspector.get_indexes(relation_name, schema_name)

        for index_dict in sorted(index_dicts, key=lambda d: d['name']):
            indexes.append(
                IndexModel(
                    name=index_dict['name'],
                    columns=index_dict['column_names'],
                    is_unique=index_dict['unique'],
                )
            )

        return indexes

    def introspect_primary_key(
        self, inspector: SchemaStrippingInspector, relation_name: str, schema_name: str
    ) -> Tuple[Optional[str], List[str]]:
        """Introspect the primary key of a table, returning the pkey name and list of columns in the primary key (if any).

        If no primary key index is defined, will return None for name, empty list for columns.
        """
        primary_index_dict = inspector.get_pk_constraint(relation_name, schema_name)

        # Athena dialect returns ... an empty _list_ instead of a dict, contrary to what
        # https://docs.sqlalchemy.org/en/14/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_pk_constraint
        # specifies for the return result from inspector.get_pk_constraint().
        if isinstance(primary_index_dict, dict):
            # MySQL at least can have unnamed primary keys. The returned dict will have 'name' -> None.
            # Sigh.
            pkey_name = primary_index_dict.get('name') or '(unnamed primary key)'

            if primary_index_dict['constrained_columns']:
                return pkey_name, primary_index_dict['constrained_columns']

        # No primary key to be returned.
        return None, []

    def introspect_columns(
        self, inspector: SchemaStrippingInspector, schema_name: str, relation_name: str
    ) -> List[ColumnModel]:
        column_dicts = inspector.get_columns(relation_name, schema=schema_name)

        retlist = []

        for col in column_dicts:
            comment = col.get('comment')  # Some dialects do not return.

            try:
                type_name = str(col['type'].as_generic()).lower()
            except NotImplementedError:
                # ENG-5268: More esoteric types like UUID do not implement .as_generic()
                type_name = str(col['type']).replace('()', '').lower()

            retlist.append(
                ColumnModel(
                    name=col['name'],
                    is_nullable=col['nullable'],
                    data_type=type_name,
                    default_expression=col['default'],
                    comment=comment,
                )
            )

        return retlist

    def get_datasource_id(self) -> UUID:
        """Convert a noteable_magics.sql.connection.Connection's name to the original
        UUID Gate knew it as.

        Will fail with ValueError if attempted against a legacy datasource handle like '@noteable', so
        please don't try to introspect within SQL cells from those.
        """
        handle = self.conn.name
        return UUID(handle[1:])


class RelationStructureMessager:
    """Context manager that collects the single-relation descriptions discovered
    within IntrospectAndStoreDatabaseCommand, buffers up to CAPACITY at a time, then
    POSTs the current collected group up to Gate in a single bulk POST (which accepts
    at most 10).

    Upon context exit, be sure to POST any partial remainder, then tell gate
    we're all done introspecting, allowing it to delete any old now no longer
    existing structures from prior introspections.

    Helper class simplifying IntrospectAndStoreDatabaseCommand.run().
    """

    # Overridden in test suite.
    CAPACITY = 10
    # As expected in real kernels in Notable. Overridden in test suite.
    JWT_PATHNAME = "/vault/secrets/jwt"

    _relations: List[RelationStructureDescription]
    _session: requests.Session
    _datasource_id: UUID
    _partition_counter: int
    _started_at: datetime

    def __init__(self, datasource_id: UUID):
        self._datasource_id = datasource_id

        # Set up HTTP session to message Gate about what is discovered.
        self._session = requests.Session()
        jwt = pathlib.Path(self.JWT_PATHNAME).read_text()
        self._session.headers.update({"Authorization": f"Bearer {jwt}"})

    def __enter__(self):
        self._relations = []
        self._started_at = datetime.utcnow()
        self._partition_counter = 1

        return self

    def queue_for_delivery(self, relation: RelationStructureDescription):
        """Stash this discovered relation. If we have enough already
        to send up to Gate, then do so.
        """
        self._relations.append(relation)

        # Time for an intermediate flush?
        if len(self._relations) == self.CAPACITY:
            self._send_relations_to_gate()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if not exc_value:
            # Successful introspection completion.
            self._send_relations_to_gate(completed_introspection=True)

        else:
            # Hit an exception. Report it back to gate.
            self._send_error_to_gate(exc_value)

            print(str(exc_value), file=sys.stderr)

            # Suppress the exception from being raised.
            return True

    def _send_relations_to_gate(self, completed_introspection: bool = False):
        base_url = f"http://gate.default/api/v1/datasources/{self._datasource_id}/schema/relations"

        if self._relations:
            # Assemble buffered relation descriptions into a single bulk upload payload
            jsonable_message = [
                relation_description.dict() for relation_description in self._relations
            ]

            # Upload in a single message.
            resp = self._session.post(
                base_url,
                json=jsonable_message,
            )

            if resp.status_code == 204:
                for relation_description in self._relations:
                    print(
                        f'Stored structure of {relation_description.schema_name}.{relation_description.relation_name} in partition {self._partition_counter}'
                    )
            else:
                error_message = f'Failed storing partition {self._partition_counter}: {resp.status_code}, {resp.text}'
                print(error_message, file=sys.stderr)
                # I guess let this kill us now? Arguable either way.
                raise Exception(error_message)

            # Prepare for next partition.
            self._partition_counter += 1
            self._relations = []

        if completed_introspection:
            # Message indicating all done through asking to clear out any stored relation structures
            # older than when we started. Curiously via DELETE, this signals the successful logical
            # end of the introspection lifecycle.
            self._session.delete(f"{base_url}?older_than={self._started_at.isoformat()}")

    def _send_error_to_gate(self, exception: Exception):
        # Sigh. Something, anything bad happened. Report it back to Gate.
        error_message: str = make_introspection_error_human_presentable(exception)

        jsonable_message = {'error': error_message}

        url = f"http://gate.default/api/v1/datasources/{self._datasource_id}/schema/introspection-error"

        # Don't care about the response code. There's nothing we can do at this point.
        self._session.post(
            url,
            json=jsonable_message,
        )


def constraints_dataframe(
    inspector: SchemaStrippingInspector, table_name: str, schema: Optional[str]
) -> DataFrame:
    """Transform results from inspector.get_check_constraints() into a single dataframe for display() purposes"""

    names: List[str] = []
    definitions: List[str] = []

    constraint_dicts: List[dict] = inspector.get_check_constraints(table_name, schema)

    for constraint_dict in sorted(constraint_dicts, key=lambda d: d['name']):
        names.append(constraint_dict['name'])
        definitions.append(constraint_dict['sqltext'])

    df = DataFrame(
        {
            'Constraint': names,
            'Definition': definitions,
        }
    )

    return set_dataframe_metadata(df, title='Check Constraints')


def foreignkeys_dataframe(
    inspector: SchemaStrippingInspector, table_name: str, schema: Optional[str]
) -> DataFrame:
    """Transform results from inspector.get_indexes() into a single dataframe for display() purposes"""

    names: List[str] = []  # Will be '(unnamed)' if the constraint was not named
    constrained_columns: List[str] = []  # Will be comma separated list for compound FKs
    referenced_qualified_tables: List[str] = []
    referenced_columns: List[str] = []  # Will be comma separated list for compound FKs

    fkey_dicts = inspector.get_foreign_keys(table_name, schema)

    for fk_dict in fkey_dicts:
        if fk_dict['referred_schema']:
            # Schema qualify the table.
            referred_table = f"{fk_dict['referred_schema']}.{fk_dict['referred_table']}"
        else:
            referred_table = fk_dict['referred_table']

        referenced_qualified_tables.append(referred_table)
        names.append(fk_dict.get('name', '(unnamed)'))
        constrained_columns.append(', '.join(fk_dict.get('constrained_columns')))
        referenced_columns.append(', '.join(fk_dict.get('referred_columns')))

    df = DataFrame(
        {
            'Foreign Key': names,
            'Columns': constrained_columns,
            'Referenced Table': referenced_qualified_tables,
            'Referenced Columns': referenced_columns,
        }
    )

    return set_dataframe_metadata(df, title='Foreign Keys')


def index_dataframe(
    inspector: SchemaStrippingInspector, table_name: str, schema: Optional[str]
) -> DataFrame:
    """Transform results from inspector.get_indexes() into a single dataframe for display() purposes"""

    index_names: List[str] = []
    column_lists: List[str] = []
    uniques: List[bool] = []

    # Primary key index is ... treated special by SQLA for some reason. Sigh.
    try:
        primary_index_dict = inspector.get_pk_constraint(table_name, schema)
    except NoSuchTableError:
        _raise_from_no_such_table(schema, table_name)

    # If it returned something truthy with nonempty constrained_columns, then
    # we assume it described a real primary key constraint here.
    if primary_index_dict and primary_index_dict.get('constrained_columns'):
        unnamed_name = '(unnamed primary key)'
        # Is a little ambiguous if 'name' will _always_ be in the returned dict? In
        # sqlite it is, but returns None, so be double-delicate here.
        index_names.append(primary_index_dict.get('name', unnamed_name) or unnamed_name)
        column_lists.append(', '.join(primary_index_dict['constrained_columns']))
        uniques.append(True)  # PK index is definitely unique.

    index_dicts: List[Dict[str, Any]] = inspector.get_indexes(table_name, schema)

    for i_d in sorted(index_dicts, key=lambda d: d['name']):
        index_names.append(i_d['name'])
        column_lists.append(', '.join(i_d['column_names']))  # List[str] to nice comma sep string.

        # Was this index UNIQUE? Wackily, if we ask sqlite, it returns 0 or 1. CRDB at least
        # returns expected boolean. So coerce to bool for consistency.
        uniques.append(bool(i_d['unique']))

        # Not doing anything with optional 'column_sorting' or 'dialect_options' at this time.
        # (although column_sorting should be fairly easy to spice in)

    df = DataFrame({'Index': index_names, 'Columns': column_lists, 'Unique': uniques})

    return set_dataframe_metadata(df, title='Indexes')


def set_dataframe_metadata(df: DataFrame, title=None) -> DataFrame:
    """Set noteable metadata in the dataframe for Dex to pick up"""

    # This is a stub for now. Expect a good number of additional kwargs to grow once
    # Shoup / Noel and I get together.

    df.attrs['noteable'] = {'decoration': {'title': title}}

    return df


def secondary_dataframe_to_html(df: DataFrame) -> HTML:
    """Because DEX expects at most one dataframe directly display()ed from a cell
    (the DEX control metadata is scoped singularly at the cell level), we cannot
    differentiate titles, display style, etc. between multiple dataframes emitted
    by a cell. So we need to hand-convert these additional datagframes down to
    HTML explicitly.
    """
    html_buf = []
    html_buf.append('<br />')
    if title := defaults_get(df, 'noteable.decoration.title'):
        html_buf.append(f'<h2>{title}</h2>')
        html_buf.append('<br />')

    html_buf.append(df.to_html(index=False))

    return HTML('\n'.join(html_buf))


def defaults_get(df: DataFrame, attribute_path: str) -> Optional[str]:
    elements = attribute_path.split(
        '.'
    )  # "noteable.decoration.title" -> ['noteable', 'decoration', 'title']
    current = df.attrs
    for elem in elements:
        if elem not in current:
            return None

        current = current[elem]

    return current


class HelpCommand(MetaCommand):
    r"""Implement \help"""

    description = "Help"
    invokers = ['\\help']
    accepts_args = True

    include_in_help = False

    def run(self, invoked_as: str, args: List[str]) -> Tuple[DataFrame, bool]:
        # If no args, will return DF describing usage of all registered subcommands.
        # If run with exactly one subcommand, find it in registry and just talk about that one.
        # If subcommand not found, then complain.
        # If run with more than a single argument, then complain.

        commands: Iterable[MetaCommand]

        if not args:
            # display all the help.
            commands = sorted(
                # Only document the ones wanting to be documented.
                (cls for cls in _all_command_classes if cls.include_in_help),
                key=lambda cls: cls.description,
            )
        else:
            if len(args) > 1:
                # Too many arguments: \help \foo bar
                raise MetaCommandException(r'Usage: \help [command]')
            elif args[0] in _registry:
                # Is '\foo' from "\help \foo", and we found "\foo" in registry.
                commands = [_registry[args[0]]]
            else:
                raise MetaCommandException(f'Unknown command "{args[0]}"')

        descriptions = []
        invokers = []
        docstrings = []

        for cmd in commands:
            descriptions.append(cmd.description)
            invokers.append(', '.join(cmd.invokers))
            docstrings.append(cmd.__doc__.strip())

        help_df = set_dataframe_metadata(
            DataFrame(
                data={
                    'Command': invokers,
                    'Description': descriptions,
                    'Documentation': docstrings,
                }
            ),
            title="SQL Introspection Commands",
        )

        # We do a better job displaying this info as static HTML than
        # DEX does currently by default. Can be revisited after we can
        # control how DEX displays this dataframe by default.
        display(secondary_dataframe_to_html(help_df))

        # We displayed it how we like, and expressly do not want DEX to touch it.
        return (help_df, False)


def displayable_relation_name(schema: Optional[str], relation_name: str) -> str:
    """If schema was specified, return dotted string. Otherwise just the relation name."""
    if schema:
        return f'{schema}.{relation_name}'
    else:
        return relation_name


# Populate simple registry of invocation command string -> concrete subclass.
# The order here also affects the order that they're listed in \help
_all_command_classes = [
    SingleRelationCommand,
    RelationsCommand,
    TablesCommand,
    ViewsCommand,
    SchemasCommand,
    IntrospectAndStoreDatabaseCommand,
    HelpCommand,
]
_registry = {}
for cls in _all_command_classes:
    for invoker in cls.invokers:
        assert invoker not in _registry, f'Cannot register {invoker} for {cls}, already registered!'
        _registry[invoker] = cls


def run_meta_command(
    shell: InteractiveShell, conn: Connection, command: str, assign_to_varname: str
) -> Optional[DataFrame]:
    """Dispatch to a MetaCommand implementation, return its result"""
    command_words = command.strip().split()  # ['\foo', 'bar.blat']
    invoker, args = command_words[0], command_words[1:]  # '\foo', ['bar.blat']

    implementation_class = _registry.get(invoker)
    if not implementation_class:
        raise MetaCommandException(f'Unknown command {invoker}')

    if args and not implementation_class.accepts_args:
        raise MetaCommandInvocationException(
            f'{invoker} does not expect arguments', invoked_with=invoker
        )

    instance = implementation_class(shell, conn, assign_to_varname)
    instance.do_run(invoker, args)


def handle_not_implemented(default: Any = None, default_factory: Callable[[], Any] = None):
    """Decorator to catch NotImplementedError, return either default constant or
    whatever  default_factory() returns."""
    assert default or default_factory, 'must provide one of default or default_factory'
    assert not (
        default and default_factory
    ), 'only provide one of either default or default_factory'

    def wrapper(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except NotImplementedError:
                if default_factory:
                    return default_factory()
                else:
                    return default

        return wrapped

    return wrapper


class SchemaStrippingInspector:
    """Proxy implementation that removes 'schema.' prefixing from results of underlying
    get_table_names() and get_view_names(). BigQuery dialect inspector seems to include
    the schema (dataset) name in those return results, unlike other dialects.
    """

    def __init__(self, underlying_inspector: Inspector):
        self.underlying_inspector = underlying_inspector

    # Direct passthrough attributes / methods
    @property
    def default_schema_name(self) -> Optional[str]:
        # BigQuery, Trino dialects may end up returning None.
        return self.underlying_inspector.default_schema_name

    def get_schema_names(self) -> List[str]:
        return self.underlying_inspector.get_schema_names()

    def get_columns(self, relation_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_columns(relation_name, schema=schema)

    @handle_not_implemented('(unobtainable)')
    def get_view_definition(self, view_name: str, schema: Optional[str] = None) -> str:
        return self.underlying_inspector.get_view_definition(view_name, schema=schema)

    def get_pk_constraint(self, table_name: str, schema: Optional[str] = None) -> dict:
        return self.underlying_inspector.get_pk_constraint(table_name, schema=schema)

    def get_foreign_keys(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_foreign_keys(table_name, schema=schema)

    @handle_not_implemented(default_factory=list)
    def get_check_constraints(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_check_constraints(table_name, schema=schema)

    def get_indexes(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_indexes(table_name, schema=schema)

    @handle_not_implemented(default_factory=list)
    def get_unique_constraints(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_unique_constraints(table_name, schema=schema)

    # Now the value-adding filtering methods.
    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        names = self.underlying_inspector.get_table_names(schema)
        return self._strip_schema(names, schema)

    def get_view_names(self, schema: Optional[str] = None) -> List[str]:
        names = self.underlying_inspector.get_view_names(schema)
        return self._strip_schema(names, schema)

    def _strip_schema(self, names: List[str], schema: Optional[str] = None) -> List[str]:
        if not schema:
            return names

        prefix = f'{schema}.'
        # Remove "schema." from the start of each name if starts with.
        # (name[False:] is equiv to name[0:], 'cause python bools are subclasses of ints)
        return [name[name.startswith(prefix) and len(prefix) :] for name in names]


def _raise_from_no_such_table(schema: str, relation_name: str):
    """Raise a MetaCommandException when eaten a NoSuchTableException"""
    if schema:
        msg = f'Relation {schema}.{relation_name} does not exist'
    else:
        msg = f'Relation {relation_name} does not exist'
    raise MetaCommandException(msg)


def make_introspection_error_human_presentable(exception: Exception) -> str:
    """Convert any exception encountered by introspection into database into a nice human presentable string."""

    # Will ultiamtely become complex due to N kinds of errors x M different SQLA database dialects possibly reporting errors differently

    # But to start with ...
    return str(exception)
