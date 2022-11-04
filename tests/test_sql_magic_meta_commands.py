import re
from typing import Optional, Tuple

import pytest
from sqlalchemy.engine.reflection import Inspector

from noteable_magics.sql.connection import Connection
from noteable_magics.sql.meta_commands import (
    _all_command_classes,
    convert_relation_glob_to_regex,
    parse_schema_and_relation_glob,
)


@pytest.mark.usefixtures("populated_sqlite_database", "populated_cockroach_database")
class TestListSchemas:
    @pytest.mark.parametrize(
        'connection_handle,expected_results',
        [
            ('@sqlite', {'num_schemas': 1, 'primary_schema_name': 'main'}),
            ('@cockroach', {'num_schemas': 3, 'primary_schema_name': 'public'}),
        ],
    )
    @pytest.mark.parametrize(
        'invocation,expect_extras',
        [
            ('schemas', False),
            ('schemas+', True),
            ('dn', False),
            ('dn+', True),
        ],
    )
    def test_list_schemas(
        self,
        connection_handle: str,
        invocation: str,
        expected_results: dict,
        expect_extras: bool,
        sql_magic,
    ):
        r"""Test \schemas variants against both sqlite and CRDB for basic sanity purposes"""
        # prepend the slash. Having the slashes in the paramterized spelling makes pytest's printout
        # of this variant icky and hard to invoke directly.
        invocation = f'\\{invocation}'
        results = sql_magic.execute(f'{connection_handle} {invocation}')

        assert len(results) == expected_results['num_schemas']
        assert results['Schema'][0] == expected_results['primary_schema_name']

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


@pytest.mark.usefixtures("populated_sqlite_database", "populated_cockroach_database")
class TestRelationsCommand:
    @pytest.mark.parametrize(
        'connection_handle',
        [
            '@sqlite',
            '@cockroach',
        ],
    )
    @pytest.mark.parametrize(
        'invocation',
        ['list', 'dr'],
    )
    @pytest.mark.parametrize(
        'argument,exp_table_string',
        [
            ('', 'int_table, references_int_table, str_int_view, str_table'),
            ('int*', 'int_table'),
            ('int_tab??', 'int_table'),
            ('*.int*', 'int_table'),
            ('*int*', 'int_table, references_int_table, str_int_view'),
        ],
    )
    def test_list_relations(
        self,
        connection_handle: str,
        argument: str,
        exp_table_string: str,
        invocation: str,
        sql_magic,
    ):

        invocation = f'\\{invocation}'
        results = sql_magic.execute(f'{connection_handle} {invocation} {argument}')
        assert len(results) == 1
        assert results['Relations'][0] == exp_table_string

    def test_list_relations_multiple_schemas(
        self,
        sql_magic,
    ):
        # Show all relations in all schemas.
        results = sql_magic.execute(r'@cockroach \list *.*')
        assert len(results) == 3
        assert results['Schema'].tolist() == ['crdb_internal', 'information_schema', 'public']
        assert results['Relations'][2] == 'int_table, references_int_table, str_int_view, str_table'

        # Show all relations in single glob'd schema (matches 'public' only)
        results = sql_magic.execute(r'@cockroach \list p*.*')
        assert len(results) == 1
        assert results['Schema'][0] == 'public'
        assert results['Relations'][0] == 'int_table, references_int_table, str_int_view, str_table'

        # Show all tables in default schema, which in crdb, happens to be named 'public'
        # (either single asterisk arg, or no arg at all)
        for invocation_and_maybe_arg in [r'\list *', r'\list']:
            results = sql_magic.execute(f'@cockroach {invocation_and_maybe_arg}')
            assert len(results) == 1
            assert results['Schema'][0] == 'public'
            assert (
                results['Relations'][0]
                == 'int_table, references_int_table, str_int_view, str_table'
            )


@pytest.mark.usefixtures("populated_cockroach_database")
class TestTablesCommand:
    def test_list_tables(
        self,
        sql_magic,
    ):
        # Show only tables (no views) in all schemas.
        results = sql_magic.execute(r'@cockroach \tables *.*')
        assert len(results) == 3
        assert results['Schema'].tolist() == ['crdb_internal', 'information_schema', 'public']
        # No str_int_view!
        assert results['Tables'][2] == 'int_table, references_int_table, str_table'

        # Show all relations in single glob'd schema (matches 'public' only)
        results = sql_magic.execute(r'@cockroach \tables p*.*')
        assert len(results) == 1
        assert results['Schema'][0] == 'public'
        assert results['Tables'][0] == 'int_table, references_int_table, str_table'

        # Show all tables in default schema, which in crdb, happens to be named 'public'
        # (either single asterisk arg, or no arg at all)
        for invocation_and_maybe_arg in [r'\tables *', r'\tables', r'\dt']:
            results = sql_magic.execute(f'@cockroach {invocation_and_maybe_arg}')
            assert len(results) == 1
            assert results['Schema'][0] == 'public'
            assert results['Tables'][0] == 'int_table, references_int_table, str_table'


@pytest.mark.usefixtures("populated_cockroach_database")
class TestViewsCommand:
    def test_list_tables(
        self,
        sql_magic,
    ):
        # Show only tables (no views) in all schemas.
        results = sql_magic.execute(r'@cockroach \views *.*')
        assert len(results) == 2
        # Not exactly sure why it thinks 'information_schema' isn't chock full of views, but oh well.
        assert results['Schema'].tolist() == ['crdb_internal', 'public']
        assert results['Views'][1] == 'str_int_view'

        # Show all views in single glob'd schema (matches 'public' only)
        results = sql_magic.execute(r'@cockroach \views p*.*')
        assert len(results) == 1
        assert results['Schema'][0] == 'public'
        assert results['Views'][0] == 'str_int_view'

        # Show all views in default schema, which in crdb, happens to be named 'public'
        # (either single asterisk arg, or no arg at all)
        for invocation_and_maybe_arg in [r'\views *', r'\views', r'\dv']:
            results = sql_magic.execute(f'@cockroach {invocation_and_maybe_arg}')
            assert len(results) == 1
            assert results['Schema'][0] == 'public'
            assert results['Views'][0] == 'str_int_view'


@pytest.mark.usefixtures("populated_sqlite_database", "populated_cockroach_database")
class TestSingleRelationCommand:
    @pytest.mark.parametrize(
        'handle,defaults_include_int8', [('@cockroach', True), ('@sqlite', False)]
    )
    def test_table_without_schema(self, sql_magic, handle: str, defaults_include_int8: bool):
        results = sql_magic.execute(fr'{handle} \describe int_table')
        assert len(results) == 3
        assert results.columns.tolist() == ['Column', 'Type', 'Nullable', 'Default']
        assert results['Column'].tolist() == ['a', 'b', 'c']
        assert results['Type'].tolist() == ['integer'] * 3
        assert results['Nullable'].tolist() == [False] * 3

        if defaults_include_int8:
            # CRDB default values include type designation hint.
            expected_defaults = [None, '12:::INT8', '42:::INT8']
        else:
            expected_defaults = [None, '12', '42']

        assert results['Default'].tolist() == expected_defaults

    @pytest.mark.parametrize('invocation', [r'\describe', r'\d'])
    def test_varying_invocation(self, sql_magic, invocation: str):
        results = sql_magic.execute(rf'@cockroach {invocation} public.int_table')
        assert len(results) == 3
        assert results['Column'].tolist() == ['a', 'b', 'c']
        assert results['Type'].tolist() == ['integer'] * 3
        assert results['Nullable'].tolist() == [False] * 3

    @pytest.mark.parametrize('handle,text_type', [('@cockroach', 'varchar'), ('@sqlite', 'text')])
    def test_against_view(self, sql_magic, handle: str, text_type: str):
        results = sql_magic.execute(fr'{handle} \describe str_int_view')
        assert len(results) == 4
        assert results['Column'].tolist() == ['str_id', 'int_col', 'b', 'c']
        assert results['Type'].tolist() == [text_type, 'integer', 'integer', 'integer']
        # Alas, view columns always smell nullable, even if in reality they are not.
        assert results['Nullable'].tolist() == [True] * 4
        # Alas, sqlite doesn't support comments in a schema,
        # CRDB does, but the dialect doesn't currently dig them out of the system catalog
        # so we don't get them returned. We try, though!
        assert results.columns.tolist() == ['Column', 'Type', 'Nullable']

    def test_no_args_gets_table_list(self, sql_magic):
        results = sql_magic.execute(r'@sqlite \d')
        # Should have given us schema + table list instead of single-table details.
        assert len(results) == 1
        assert results.columns.tolist() == ['Schema', 'Relations']

    def test_hate_more_than_one_arg(self, sql_magic, capsys):
        sql_magic.execute(r'@sqlite \d foo bar')
        out, err = capsys.readouterr()
        assert out.startswith(r'Usage: \d [[schema].[relation_name]]')

    def test_nonexistent_table(self, sql_magic, capsys):
        sql_magic.execute(r'@cockroach \d foobar')
        out, err = capsys.readouterr()
        assert out.startswith(r'Relation foobar does not exist')

    def test_nonexistent_schema_qualified_table(self, sql_magic, capsys):
        sql_magic.execute(r'@cockroach \d public.foobar')
        out, err = capsys.readouterr()
        assert out.startswith(r'Relation public.foobar does not exist')

    def test_nonexistent_schema(self, sql_magic, capsys):
        sql_magic.execute(r'@cockroach \d sdfsdfsdf.foobar')
        out, err = capsys.readouterr()
        assert out.startswith(r'Relation sdfsdfsdf.foobar does not exist')


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


class TestParseSchemaAndRelationGlob:
    @pytest.mark.parametrize(
        'inp,expected_result',
        [
            ('*.*', ('*', '*')),  # all schemas, all tables.
            # no schema designator implies default schema. All tables starting with 'foo'
            (
                'foo*',
                ('default', 'foo*'),
            ),
            (
                'main.foo*',
                ('main', 'foo*'),
            ),  # Specific schema (main), all tables starting with foo.
            ('*schema*.*', ('*schema*', '*')),  # Schemas matching glob *schema*, any tables within.
        ],
    )
    def test_when_default_schema_is_available(
        self, inp: str, expected_result: Tuple[Optional[str], Optional[str]], mocker
    ):
        mock_inspector = mocker.Mock(Inspector)
        mock_inspector.default_schema_name = 'default'
        assert parse_schema_and_relation_glob(mock_inspector, inp) == expected_result

    def test_when_default_schema_is_not_available(self, mocker):
        """Test when the dialect's inspector doesn't distinguish any schema as the default, as BigQuery and Trino do"""
        mock_inspector = mocker.Mock(Inspector)
        mock_inspector.default_schema_name = None
        mock_inspector.get_schema_names.side_effect = lambda: ['first', 'random_schema']

        assert parse_schema_and_relation_glob(mock_inspector, 'foo') == ('first', 'foo')
        assert parse_schema_and_relation_glob(mock_inspector, '*') == ('first', '*')
        assert parse_schema_and_relation_glob(mock_inspector, '.') == ('first', '*')
        assert parse_schema_and_relation_glob(mock_inspector, 'schema.foo*') == ('schema', 'foo*')


@pytest.mark.parametrize(
    'inp,imply_prefix,expected_result',
    [
        # No glob chars at all imply prefix search if asked with imply_prefix
        ('foo', True, re.compile('foo.*')),
        ('foo', False, re.compile('foo')),
        # Explicit prefix search.
        ('foo*', False, re.compile('foo.*')),
        # glob wildcard -> regex wildcard.
        ('*', False, re.compile('.*')),
        # Skip trash
        ('f$%^&', True, re.compile('f.*')),
        # Spaces preserved, FWIW.
        ('foo_bar *', False, re.compile('foo_bar .*')),
        # Question marks work too.
        ('f??', True, re.compile('f..')),
    ],
)
def test_convert_relation_glob_to_regex(
    inp: str, imply_prefix, expected_result: Tuple[Optional[str], Optional[str]], mocker
):

    assert convert_relation_glob_to_regex(inp, imply_prefix=imply_prefix) == expected_result
