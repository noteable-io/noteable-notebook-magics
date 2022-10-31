import pytest

from noteable_magics.sql.connection import Connection
from noteable_magics.sql.meta_commands import _all_command_classes


@pytest.mark.usefixtures("populated_sqlite_database", "populated_cockroach_database")
class TestListSchemas:
    @pytest.mark.parametrize('connection_handle', ['@sqlite', '@cockroach'])
    @pytest.mark.parametrize(
        'invocation,expect_extras',
        [
            (r'\schemas', False),
            (r'\schemas+', True),
            (r'\dn', False),
            (r'\dn+', True),
        ],
    )
    def test_list_schemas(
        self, connection_handle: str, invocation: str, expect_extras: bool, sql_magic
    ):
        print(Connection.connections[connection_handle])
        results = sql_magic.execute(f'{connection_handle} {invocation}')

        # Sqlite just has one schema, 'main', and is the default.
        assert len(results) == 1
        assert results['Schema'][0] == 'main'

        # wacky, if test with 'is', fails with 'assert True is True'
        assert results['Default'][0] == True  # noqa: E712

        if expect_extras:
            assert results['Table Count'][0] == 3  # int_table, str_table, references_int_table
            assert results['View Count'][0] == 1  # str_int_view
            assert results.columns.tolist() == ['Schema', 'Default', 'Table Count', 'View Count']
        else:
            assert results.columns.tolist() == ['Schema', 'Default']

    def test_list_schemas_when_no_views(self, sql_magic):
        r"""Prove that when no views exist, \schemas+ does not talk at all about a 'View Count' column"""

        # Drop the view.
        connection = Connection.connections['@sqlite']
        db = connection.session  # sic, a sqlalchemy.engine.base.Connection, not a Session. Sigh.
        db.execute('drop view str_int_view')

        results = sql_magic.execute(r'@sqlite \schemas+')

        assert len(results) == 1

        assert results.columns.tolist() == [
            'Schema',
            'Default',
            'Table Count',
        ]  # no 'View Count'

    def test_hates_arguments(self, sql_magic, capsys):
        sql_magic.execute(r'@sqlite \schemas foo')
        out, err = capsys.readouterr()
        assert (
            out
            == '\\schemas does not expect arguments\n(Use "\\help \\schemas"" for more assistance)\n'
        )


@pytest.mark.usefixtures("populated_sqlite_database")
class TestHelp:
    def test_general_help(self, sql_magic):
        results = sql_magic.execute(r'@sqlite \help')
        assert len(results) == len(_all_command_classes) - 1  # avoids talking about HelpCommand
        assert results.columns.tolist() == ['Description', 'Documentation', 'Invoke Using One Of']

    # Both these specific commands should regurgitate the same help row.
    @pytest.mark.parametrize('cmdname', [r'\schemas', r'\dn+'])
    def test_single_topic_help(self, cmdname, sql_magic):
        results = sql_magic.execute(rf'@sqlite \help {cmdname}')
        assert len(results) == 1
        assert results.columns.tolist() == ['Description', 'Documentation', 'Invoke Using One Of']
        assert results['Description'][0] == 'List schemas (namespaces) within database'
        assert results['Invoke Using One Of'][0] == r'\schemas, \schemas+, \dn, \dn+'
        assert results['Documentation'][0].startswith('List all the schemas')

    def test_help_hates_unknown_subcommands(self, sql_magic, capsys):
        sql_magic.execute(r'@sqlite \help \foo')
        out, err = capsys.readouterr()
        assert out == 'Unknown command "\\foo"\n(Use "\\help" for more assistance)\n'

    def test_help_wants_at_most_a_single_arg(self, sql_magic, capsys):
        sql_magic.execute(r'@sqlite \help \foo \bar')
        out, err = capsys.readouterr()
        assert out == 'Usage: \\help [command]\n(Use "\\help" for more assistance)\n'


@pytest.mark.usefixtures("populated_sqlite_database")
class TestMisc:
    def test_unknown_command(self, sql_magic, capsys):
        sql_magic.execute(r'@sqlite \unknown_subcommand')
        out, err = capsys.readouterr()
        assert out == 'Unknown command \\unknown_subcommand\n(Use "\\help" for more assistance)\n'
