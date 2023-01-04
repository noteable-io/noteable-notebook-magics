from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from IPython.core.interactiveshell import InteractiveShell
from IPython.display import HTML, display
from pandas import DataFrame
from sqlalchemy import inspect
from sqlalchemy.engine.reflection import Inspector

from noteable_magics.sql.connection import Connection

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

        is_view = relation_name in inspector.get_view_names(schema)

        if not is_view:
            # Ensure is a table
            if relation_name not in inspector.get_table_names(schema):
                if schema:
                    msg = f'Relation {schema}.{relation_name} does not exist'
                else:
                    msg = f'Relation {relation_name} does not exist'
                raise MetaCommandException(msg)
            rtype = 'Table'
        else:
            rtype = 'View'

        column_dicts = inspector.get_columns(relation_name, schema=schema)

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

        main_relation_df = set_dataframe_metadata(
            DataFrame(data=data), title=f'{rtype} "{displayable_rname}" Structure'
        )

        display(main_relation_df)

        if is_view:
            view_definition = inspector.get_view_definition(relation_name, schema)

            # Would not surprise me if some dialects do not implement / return anything, so be cautious.
            if view_definition:
                display_view_name = displayable_relation_name(schema, relation_name)  # noqa: F841
                html_buf = []
                html_buf.append('<br />')
                html_buf.append(f'<h2>View <code>{display_view_name}</code> Definition</h2>')
                html_buf.append('<br />')
                html_buf.append(f'<pre>{view_definition}</pre>')

                display(HTML('\n'.join(html_buf)))
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
                    display(secondary_dataframe_to_html(secondary_df))

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

    title = f'Table <code>{displayable_relation_name(schema, table_name)}</code> Check Constraints'

    return set_dataframe_metadata(df, title=title)


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

    title = f'Table <code>{displayable_relation_name(schema, table_name)}</code> Foreign Keys'

    return set_dataframe_metadata(df, title=title)


def index_dataframe(
    inspector: SchemaStrippingInspector, table_name: str, schema: Optional[str]
) -> DataFrame:
    """Transform results from inspector.get_indexes() into a single dataframe for display() purposes"""

    index_names: List[str] = []
    column_lists: List[str] = []
    uniques: List[bool] = []

    # Primary key index is ... treated special by SQLA for some reason. Sigh.
    primary_index_dict = inspector.get_pk_constraint(table_name, schema)

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

    title = f'Table <code>{displayable_relation_name(schema, table_name)}</code> Indices'

    return set_dataframe_metadata(df, title=title)


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

    def run(self, invoked_as: str, args: List[str]) -> Tuple[DataFrame, bool]:
        # If no args, will return DF describing usage of all registered subcommands.
        # If run with exactly one subcommand, find it in registry and just talk about that one.
        # If subcommand not found, then complain.
        # If run with more than a single argument, then complain.

        commands: Iterable[MetaCommand]

        if not args:
            # display all the help.
            commands = sorted(
                # Omit talking about myself.
                (cls for cls in _all_command_classes if cls is not HelpCommand),
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


class SchemaStrippingInspector:
    """Proxy implementation that removes 'schema.' prefixing from results of underlying
    get_table_names() and get_view_names(). BigQuery dialect inspector seems to include
    the schema (dataset) name in those return results, unlike other dialects.
    """

    def __init__(self, underlying_inspector: Inspector):
        self.underlying_inspector = underlying_inspector

    # Direct passthrough attributes / methods
    @property
    def default_schema_name(self) -> str:
        return self.underlying_inspector.default_schema_name

    def get_schema_names(self) -> List[str]:
        return self.underlying_inspector.get_schema_names()

    def get_columns(self, relation_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_columns(relation_name, schema=schema)

    def get_view_definition(self, view_name: str, schema: Optional[str] = None) -> str:
        return self.underlying_inspector.get_view_definition(view_name, schema=schema)

    def get_pk_constraint(self, table_name: str, schema: Optional[str] = None) -> dict:
        return self.underlying_inspector.get_pk_constraint(table_name, schema=schema)

    def get_foreign_keys(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_foreign_keys(table_name, schema=schema)

    def get_check_constraints(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_check_constraints(table_name, schema=schema)

    def get_indexes(self, table_name: str, schema: Optional[str] = None) -> List[dict]:
        return self.underlying_inspector.get_indexes(table_name, schema=schema)

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
