import re
from typing import Dict, Iterable, List, Optional, Tuple

from IPython.core.interactiveshell import InteractiveShell
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
    """Base class for family of metadata commands to do operations like schema-introspection, etc.
    within SQL cells.
    """

    # One-line description of what this one does.
    description: str

    # List of strings that will invoke this subclass.
    # Primary human-readable and memorable invocation string should come first, then any shorthand
    # aliases. See how global _registry is populated towards bottom of
    invokers: List[str]

    # Does this command accept additional arguments?
    accepts_args: bool

    def __init__(self, shell: InteractiveShell, conn: Connection):
        self.shell = shell
        self.conn = conn

    def run(self, invoked_as: str, args: List[str]):
        raise NotImplementedError

    def get_inspector(self) -> Inspector:
        return inspect(self.conn._engine)


class SchemasCommand(MetaCommand):
    """List all the schemas (namespaces for tables, views) within a database."""

    """\nIf invoked with trailing '+', will also include the count of tables and views within each schema."""

    description = "List schemas (namespaces) within database"
    invokers = ['\\schemas', '\\schemas+', '\\dn', '\\dn+']
    accepts_args = False

    def run(self, invoked_as: str, args: List[str]):
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

        return DataFrame(data=data)


class RelationsCommand(MetaCommand):
    """List all the relations (tables and views) within one or more schemas (namespaces) of a database."""

    description = "List names of tables and views within database"
    invokers = [r'\list', r'\dr']
    accepts_args = True

    def run(self, invoked_as: str, args: List[str]):
        if len(args) > 1:
            raise MetaCommandException(f'Usage: {invoked_as} [[schema pattern].[table pattern]]')

        if not args:
            # All relations in the default schema.
            args = ['*']

        return relation_names(self.get_inspector(), args[0])


class TablesCommand(MetaCommand):
    """List all the tables (not views) within one or more schemas (namespaces) of a database."""

    description = "List names of tables within database"
    invokers = [r'\tables', r'\dt']
    accepts_args = True

    def run(self, invoked_as: str, args: List[str]):
        if len(args) > 1:
            raise MetaCommandException(f'Usage: {invoked_as} [[schema pattern].[table pattern]]')

        if not args:
            # All tables in the default schema.
            args = ['*']

        return relation_names(self.get_inspector(), args[0], include_views=False)


class ViewsCommand(MetaCommand):
    """List all the views (not tables) within one or more schemas (namespaces) of a database."""

    description = "List names of views within database"
    invokers = [r'\views', r'\dv']
    accepts_args = True

    def run(self, invoked_as: str, args: List[str]):
        if len(args) > 1:
            raise MetaCommandException(f'Usage: {invoked_as} [[schema pattern].[view pattern]]')

        if not args:
            # All views in the default schema.
            args = ['*']

        return relation_names(self.get_inspector(), args[0], include_tables=False)


def relation_names(
    inspector: Inspector,
    argument: str,
    include_tables=True,
    include_views=True,
) -> DataFrame:
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

    # Collect tables and / or views in each requested schema.
    schema_to_relations: Dict[str, str] = {}
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

        # Convert passing-through-relation_name_filter names into a nice comma list
        relations_comma_str = ', '.join(
            sorted(r for r in relations if relation_name_filter.match(r))
        )

        if relations_comma_str:
            schema_to_relations[schema] = relations_comma_str

    if include_tables and include_views:
        colname = 'Relations'
    elif include_tables:
        colname = 'Tables'
    else:
        colname = 'Views'

    return DataFrame(
        data={
            'Schema': list(schema_to_relations.keys()),
            colname: [schema_to_relations[k] for k in schema_to_relations],
        }
    )


def parse_schema_and_relation_glob(
    inspector: Inspector, schema_and_possible_relation: str
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


class HelpCommand(MetaCommand):
    r"""Implement \help"""

    description = "Help"
    invokers = ['\\help']
    accepts_args = True

    def run(self, invoked_as: str, args: List[str]):
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

        return DataFrame(
            data={
                'Description': descriptions,
                'Documentation': docstrings,
                'Invoke Using One Of': invokers,
            }
        )


# Populate simple registry of invocation command string -> concrete subclass.
_all_command_classes = [SchemasCommand, HelpCommand, RelationsCommand, TablesCommand, ViewsCommand]
_registry = {}
for cls in _all_command_classes:
    for invoker in cls.invokers:
        assert invoker not in _registry, f'Cannot register {invoker} for {cls}, already registered!'
        _registry[invoker] = cls


def run_meta_command(
    shell: InteractiveShell, conn: Connection, command: str
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

    instance = implementation_class(shell, conn)
    return instance.run(invoker, args)
