from typing import Optional

from sqlalchemy import inspect
from sqlalchemy.engine.reflection import Inspector

from pandas import DataFrame

from IPython.core.interactiveshell import InteractiveShell

from noteable_magics.sql.connection import Connection

__all__ = ['MetaCommandException', 'run_meta_command']


class MetaCommandException(Exception):
    """General exception when evaluating SQL cell meta commands"""

    pass


class MetaCommandInvocationException(MetaCommandException):
    """Invoked a specific meta command incorrectly.

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

    # List of strings that will invoke this subclass.
    # Primary human-readable and memorable invocation string should come first, then any shorthand
    # aliases. See how global _registry
    invokers: list[str]

    # Does this command accept additional arguments?
    accepts_args: bool

    def __init__(self, shell: InteractiveShell, conn: Connection):
        self.shell = shell
        self.conn = conn

    def run(self, invoked_as: str, args: list[str]):
        raise NotImplementedError

    def get_inspector(self) -> Inspector:
        return inspect(self.conn._engine)


class SchemasCommand(MetaCommand):
    """List all the schemas (namespaces for tables, views) within a database.

    If invoked with trailing '+', will also include the count of tables and views
    within each schema.
    """

    invokers = ['\\schemas', '\\schemas+', '\\dn', '\\dn+']
    accepts_args = False

    def run(self, invoked_as: str, args: list[str]):
        insp = self.get_inspector()

        default_schema = insp.default_schema_name
        all_schemas = sorted(insp.get_schema_names())

        # Want to have default schema always come first regardless of alpha order
        all_schemas.remove(default_schema)
        all_schemas.insert(0, default_schema)

        data = {'Schema': all_schemas, 'Default': [sn == default_schema for sn in all_schemas]}

        if invoked_as.endswith('+'):
            # Want extra info -- namely, the table + view counts.
            data['Table Count'] = [len(insp.get_table_names(sn)) for sn in all_schemas]

            view_counts = [len(insp.get_view_names(sn)) for sn in all_schemas]
            if any(view_counts):
                data['View Count'] = view_counts

        return DataFrame(data=data)


# Populate simple registry of invocation command string -> concrete subclass.
_registry = {}
for cls in [SchemasCommand]:
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
