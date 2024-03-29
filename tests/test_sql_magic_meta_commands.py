import json
import re
import urllib.parse
from datetime import datetime
from typing import List, Optional, Tuple
from uuid import uuid4

import pandas as pd
import pytest
from IPython.display import HTML
from sqlalchemy.engine.reflection import Inspector

from noteable.sql.connection import InspectorProtocol, IntrospectableConnection, get_sqla_connection
from noteable.sql.meta_commands import (
    MetaCommandException,
    MetaCommandInvocationException,
    RelationStructureMessager,
    _all_command_classes,
    convert_relation_glob_to_regex,
    parse_schema_and_relation_glob,
)
from noteable.sql.sqlalchemy.utils import (
    BigQueryInspector,
    WrappedInspector,
    handle_not_implemented,
)
from noteable.sql.types import RelationStructureDescription
from tests.conftest import COCKROACH_HANDLE, COCKROACH_UUID, KNOWN_TABLES, KNOWN_TABLES_AND_KINDS


@pytest.mark.usefixtures("populated_sqlite_database", "populated_cockroach_database")
class TestListSchemas:
    @pytest.mark.parametrize(
        'connection_handle,expected_results',
        [
            ('@sqlite', {'num_schemas': 1, 'primary_schema_name': 'main'}),
            (COCKROACH_HANDLE, {'num_schemas': 1, 'primary_schema_name': 'public'}),
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
        ipython_namespace,
        mock_display,
    ):
        r"""Test \schemas variants against both sqlite and CRDB for basic sanity purposes"""
        # prepend the slash. Having the slashes in the paramterized spelling makes pytest's printout
        # of this variant icky and hard to invoke directly.
        invocation = f'\\{invocation}'
        sql_magic.execute(f'{connection_handle} {invocation}')

        # The magic meta command does not return the dataframe, but instead
        # directly calls display() and assigns into '_' (or to an arbitrary name
        # if invoked with '<<')
        results = ipython_namespace['_']
        mock_display.assert_called_with(results)

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

    def test_list_schemas_when_no_views(
        self,
        sql_magic,
        ipython_namespace,
        mock_display,
    ):
        r"""Prove that when no views exist, \schemas+ does not talk at all about a 'View Count' column"""

        # Drop the view.
        db = get_sqla_connection("@sqlite")
        db.execute('drop view str_int_view')

        sql_magic.execute(r'@sqlite \schemas+')
        results = ipython_namespace['_']
        mock_display.assert_called_with(results)

        assert len(results) == 1

        assert results.columns.tolist() == [
            'Schema',
            'Default',
            'Table Count',
        ]  # no 'View Count'

    def test_hates_arguments(self, sql_magic):
        with pytest.raises(MetaCommandInvocationException) as excinfo:
            sql_magic.execute(r'@sqlite \schemas foo')
            assert str(excinfo) == '\\schemas does not expect arguments'


@pytest.mark.usefixtures("populated_sqlite_database", "populated_cockroach_database")
class TestRelationsCommand:
    @pytest.mark.parametrize(
        'connection_handle',
        [
            '@sqlite',
            COCKROACH_HANDLE,
        ],
    )
    @pytest.mark.parametrize(
        'invocation',
        ['list', 'dr'],
    )
    @pytest.mark.parametrize(
        'argument,exp_relations,exp_kinds',
        [
            (
                '',
                ['int_table', 'references_int_table', 'str_int_view', 'str_table'],
                ['table', 'table', 'view', 'table'],
            ),
            ('int*', ['int_table'], ['table']),
            ('int_tab??', ['int_table'], ['table']),
            ('*.int*', ['int_table'], ['table']),
            (
                '*int*',
                ['int_table', 'references_int_table', 'str_int_view'],
                ['table', 'table', 'view'],
            ),
        ],
    )
    def test_list_relations(
        self,
        connection_handle: str,
        argument: str,
        exp_relations: List[str],
        exp_kinds: List[str],
        invocation: str,
        sql_magic,
        ipython_namespace,
        mock_display,
    ):
        invocation = f'\\{invocation}'
        sql_magic.execute(f'{connection_handle} {invocation} {argument}')

        results = ipython_namespace['_']
        mock_display.assert_called_with(results)

        assert results.columns.tolist() == ['Schema', 'Relation', 'Kind']
        assert results['Relation'].tolist() == exp_relations
        assert results['Kind'].tolist() == exp_kinds

    def test_list_relations_multiple_schemas(
        self,
        sql_magic,
        ipython_namespace,
        mock_display,
    ):
        # Show all relations in all schemas.
        sql_magic.execute(rf'{COCKROACH_HANDLE} \list *.*')

        results = ipython_namespace['_']
        mock_display.assert_called_with(results)

        assert len(results) == 4  #
        assert set(results['Schema'].tolist()) == set(('public',))

        assert results[results.Schema == 'public']['Relation'].tolist() == [
            'int_table',
            'references_int_table',
            'str_int_view',
            'str_table',
        ]

        # Show all relations in single glob'd schema (matches 'public' only)
        sql_magic.execute(rf'{COCKROACH_HANDLE} \list p*.*')
        results = ipython_namespace['_']
        assert len(results) == 4
        assert set(results['Schema'].tolist()) == set(('public',))
        assert results['Relation'].tolist() == [
            'int_table',
            'references_int_table',
            'str_int_view',
            'str_table',
        ]

        # Show all tables in default schema, which in crdb, happens to be named 'public'
        # (either single asterisk arg, or no arg at all)
        for invocation_and_maybe_arg in [r'\list *', r'\list']:
            sql_magic.execute(f'{COCKROACH_HANDLE} {invocation_and_maybe_arg}')
            results = ipython_namespace['_']
            assert len(results) == 4
            assert results['Schema'][0] == 'public'
            assert results['Relation'].tolist() == [
                'int_table',
                'references_int_table',
                'str_int_view',
                'str_table',
            ]


@pytest.mark.usefixtures("populated_cockroach_database")
class TestTablesCommand:
    def test_list_tables(
        self,
        sql_magic,
        ipython_namespace,
        mock_display,
    ):
        # Show only tables (no views) in all schemas. Also test out assignment to non='_' var.
        results = sql_magic.execute(rf'{COCKROACH_HANDLE} tables << \tables *.*')

        results = ipython_namespace['tables']
        mock_display.assert_called_with(results)

        assert results.columns.tolist() == ['Schema', 'Table']
        assert len(results) == 3
        assert set(results['Schema'].tolist()) == set(['public'])
        # No str_int_view!
        assert results[results.Schema == 'public']['Table'].tolist() == [
            'int_table',
            'references_int_table',
            'str_table',
        ]

        # Show all relations in single glob'd schema (matches 'public' only)
        sql_magic.execute(rf'{COCKROACH_HANDLE} \tables p*.*')
        results = ipython_namespace['_']

        assert len(results) == 3
        assert set(results['Schema'].tolist()) == set(('public',))
        assert results['Table'].tolist() == ['int_table', 'references_int_table', 'str_table']

        # Show all tables in default schema, which in crdb, happens to be named 'public'
        # (either single asterisk arg, or no arg at all)
        for invocation_and_maybe_arg in [r'\tables *', r'\tables', r'\dt']:
            sql_magic.execute(f'{COCKROACH_HANDLE} {invocation_and_maybe_arg}')
            results = ipython_namespace['_']

        assert len(results) == 3
        assert set(results['Schema'].tolist()) == set(('public',))
        assert results['Table'].tolist() == ['int_table', 'references_int_table', 'str_table']


@pytest.mark.usefixtures("populated_cockroach_database")
class TestViewsCommand:
    def test_list_views(self, sql_magic, ipython_namespace):
        # Show only views (no tables) in all schemas.
        sql_magic.execute(rf'{COCKROACH_HANDLE} \views *.*')
        results = ipython_namespace['_']

        # Not exactly sure why it thinks 'information_schema' isn't chock full of views, but oh well.
        assert results.columns.tolist() == ['Schema', 'View']
        assert results['Schema'].unique().tolist() == ['public']
        assert results[results.Schema == 'public']['View'].tolist() == ['str_int_view']

        # Show all views in single glob'd schema (matches 'public' only)
        sql_magic.execute(rf'{COCKROACH_HANDLE} \views p*.*')
        results = ipython_namespace['_']
        assert len(results) == 1
        assert results['Schema'][0] == 'public'
        assert results['View'][0] == 'str_int_view'

        # Show all views in default schema, which in crdb, happens to be named 'public'
        # (either single asterisk arg, or no arg at all)
        for invocation_and_maybe_arg in [r'\views *', r'\views', r'\dv']:
            sql_magic.execute(f'{COCKROACH_HANDLE} {invocation_and_maybe_arg}')
            results = ipython_namespace['_']
            assert len(results) == 1
            assert results['Schema'][0] == 'public'
            assert results['View'][0] == 'str_int_view'


@pytest.mark.usefixtures("populated_sqlite_database", "populated_cockroach_database")
class TestSingleRelationCommand:
    @pytest.mark.parametrize(
        'handle,defaults_might_include_int8,expected_pk_index_name',
        [(COCKROACH_HANDLE, True, 'int_table_pkey'), ('@sqlite', False, '(unnamed primary key)')],
    )
    def test_table_without_schema(
        self,
        sql_magic,
        ipython_namespace,
        handle: str,
        defaults_might_include_int8: bool,
        expected_pk_index_name: str,
        mock_display,
    ):
        sql_magic.execute(fr'{handle} \describe int_table')
        results = ipython_namespace['_']

        assert len(results) == 3
        assert results.columns.tolist() == ['Column', 'Type', 'Nullable', 'Default']
        assert results['Column'].tolist() == ['a', 'b', 'c']
        assert results['Type'].tolist() == ['integer'] * 3
        assert results['Nullable'].tolist() == [False] * 3

        # A list of lists 'cause CRDB might give us one or the other based on version.
        expected_defaults = [
            [None, '12', '42'],
        ]
        if defaults_might_include_int8:
            # CRDB default values might include type designation hint based on which version CRDB we're talking to.
            expected_defaults.append([None, '12:::INT8', '42:::INT8'])

        assert results['Default'].tolist() in expected_defaults

        # Two things will be display()ed ...
        assert mock_display.call_count == 2

        # 1) The dataframe describing the table columns.
        df_displayed = mock_display.call_args_list[0].args[0]
        assert isinstance(df_displayed, pd.DataFrame)
        assert results is df_displayed

        # 2) HTML describing the indices
        index_df_html = mock_display.call_args_list[1].args[0]
        assert isinstance(index_df_html, HTML)
        assert '<h2>Indexes</h2>' in index_df_html.data  # title was projected.

        # Convert the HTML spelling of the indices back into a DF to test the output.
        df_from_index_html = pd.read_html(index_df_html.data)[0]

        # primary key index, secondary index should be be described.
        assert len(df_from_index_html) == 2
        assert df_from_index_html['Index'].tolist() == [
            expected_pk_index_name,
            'int_table_whole_row_idx',
        ]
        assert df_from_index_html['Columns'].tolist() == ['a', 'a, b, c']
        # Both indices are unique.
        assert df_from_index_html['Unique'].tolist() == [True, True]

    @pytest.mark.parametrize('invocation', [r'\describe', r'\d'])
    def test_varying_invocation(self, sql_magic, ipython_namespace, invocation: str):
        sql_magic.execute(rf'{COCKROACH_HANDLE} {invocation} public.int_table')
        results = ipython_namespace['_']

        assert len(results) == 3
        assert results['Column'].tolist() == ['a', 'b', 'c']
        assert results['Type'].tolist() == ['integer'] * 3
        assert results['Nullable'].tolist() == [False] * 3

    @pytest.mark.parametrize(
        'handle,text_type', [(COCKROACH_HANDLE, 'varchar'), ('@sqlite', 'text')]
    )
    def test_against_view(
        self, sql_magic, ipython_namespace, handle: str, text_type: str, mock_display
    ):
        sql_magic.execute(fr'{handle} \describe str_int_view')
        results = ipython_namespace['_']

        assert len(results) == 4
        assert results['Column'].tolist() == ['str_id', 'int_col', 'b', 'c']
        assert results['Type'].tolist() == [text_type, 'integer', 'integer', 'integer']
        # Alas, view columns always smell nullable, even if in reality they are not.
        assert results['Nullable'].tolist() == [True] * 4
        # Alas, sqlite doesn't support comments in a schema,
        # CRDB does, but the dialect doesn't currently dig them out of the system catalog
        # so we don't get them returned. We try, though!
        assert results.columns.tolist() == ['Column', 'Type', 'Nullable']

        # Two things will be display()ed ...
        assert mock_display.call_count == 2

        # 1) The dataframe describing the view columns.
        df_displayed = mock_display.call_args_list[0].args[0]
        assert isinstance(df_displayed, pd.DataFrame)
        assert results is df_displayed

        # 2) The HTML blob describing the view definition
        html_obj = mock_display.call_args_list[1].args[0]
        assert isinstance(html_obj, HTML)
        html_contents: str = html_obj.data
        assert html_contents.startswith('<br />\n<h2>View Definition</h2>')
        # Some dialects include a 'CREATE VIEW' statement, others just start with 'select\n', and will vary by case.
        matcher = re.compile(
            '.*<pre>.*select.*s.str_id, s.int_col.*</pre>$',
            re.IGNORECASE + re.MULTILINE + re.DOTALL,
        )
        assert matcher.search(html_contents)

    def test_against_uuid_column(self, sql_magic, ipython_namespace):
        """Test that we can introspect into a table that has a UUID column.

        Because SQLA's UUID handling class doesn't implement .as_generic(),
        our SingleRelationCommand needed to grow a try/except block.
        """
        table_name = f'test_table_{uuid4().hex}'

        sql_magic.execute(
            f'{COCKROACH_HANDLE}\ncreate table {table_name}(id uuid not null primary key, name text not null)'
        )

        sql_magic.execute(fr'{COCKROACH_HANDLE} \describe {table_name}')

        df = ipython_namespace['_']

        assert df['Type'].tolist() == ['uuid', 'varchar']

    def test_against_schema_qualified_view(self, sql_magic, ipython_namespace, mock_display):
        # Sub-case of test_against_view(), but when schema-qualified.
        # Test that we schema qualify correctly in the <h2> when schema was explicitly mentioned.

        sql_magic.execute(rf'{COCKROACH_HANDLE} \describe public.str_int_view')

        df = mock_display.call_args_list[0].args[0]
        assert df.attrs['noteable']['decoration']['title'] == 'View "public.str_int_view" Structure'

        html_obj = mock_display.call_args_list[1].args[0]
        assert isinstance(html_obj, HTML)
        html_contents: str = html_obj.data
        assert html_contents.startswith('<br />\n<h2>View Definition</h2>'), html_contents

    @pytest.mark.parametrize(
        'handle,schema', [(COCKROACH_HANDLE, ''), (COCKROACH_HANDLE, 'public'), ('@sqlite', '')]
    )
    def test_foreign_keys(self, sql_magic, ipython_namespace, mock_display, handle, schema):
        """Describing table `references_int_table` should talk about a foreign key over to int_table"""

        # If was asked with schema qualification, then various outputs will also be schema qualified.
        qualified_references_int_table = (
            f'{schema}.references_int_table' if schema else 'references_int_table'
        )
        qualified_int_table = f'{schema}.int_table' if schema else 'int_table'

        sql_magic.execute(rf'{handle} \d {qualified_references_int_table}')

        assert (
            len(mock_display.call_args_list) == 3
        )  # main structure DF, index DF-as-html, foreign key DF-as-html.
        fk_html = mock_display.call_args_list[2].args[0]
        assert isinstance(fk_html, HTML)
        html_contents: str = fk_html.data

        assert html_contents.startswith('<br />\n<h2>Foreign Keys</h2>'), html_contents

        # Convert the HTML table back to dataframe to complete test.
        fk_df = pd.read_html(html_contents)[0]

        assert fk_df.columns.tolist() == [
            'Foreign Key',
            'Columns',
            'Referenced Table',
            'Referenced Columns',
        ]
        assert fk_df['Columns'].tolist() == ['a_id']
        assert fk_df['Referenced Table'].tolist() == [qualified_int_table]
        assert fk_df['Referenced Columns'].tolist() == ['a']

        # Also test against a table with a compound foreign key. Must create table pair ad hoc. Will be cleaned
        # up upon test cleanup.

        sql_magic.execute(
            f'{handle}\ncreate table {qualified_int_table}_2 (a int, b int, primary key(a, b))'
        )

        sql_magic.execute(
            f'{handle}\ncreate table {qualified_references_int_table}_2 (a_ref int primary key, b_ref int, constraint a_b_fk foreign key (a_ref, b_ref) references {qualified_int_table}_2(a, b))'
        )

        mock_display.reset_mock()

        sql_magic.execute(fr'{handle} \describe {qualified_references_int_table}_2')

        assert (
            len(mock_display.call_args_list) == 3
        )  # main structure DF, index DF-as-html, foreign key DF-as-html.
        fk2_html = mock_display.call_args_list[2].args[0]

        fk2_df = pd.read_html(fk2_html.data)[0]
        assert fk2_df['Columns'][0] == 'a_ref, b_ref'
        assert fk2_df['Referenced Columns'][0] == 'a, b'

    def test_against_table_without_a_primary_key(self, sql_magic, ipython_namespace, mock_display):
        # str_table on sqlite will not have any primary key or any indices at all
        # (all tables in cockroach have an implicit PK, so can't test with it)

        # The output should NOT include an HTML blob describing indices.
        sql_magic.execute(r'@sqlite \d str_table')

        assert len(mock_display.call_args_list) == 2  # main df, constraints df-as-html

        df = mock_display.call_args_list[0].args[0]
        assert isinstance(df, pd.DataFrame)
        assert df.attrs['noteable']['decoration']['title'] == 'Table "str_table" Structure'

        # Test test_constraints() will exercise this further. Only mention it here
        # because will be returned and should not be talking about primary key / indices.
        constraint_html = mock_display.call_args_list[1].args[0].data
        assert constraint_html.startswith('<br />\n<h2>Check Constraints</h2>')

    # All CRDB tables have a primary key, so conditionally expect it to be described.
    @pytest.mark.parametrize(
        'handle,expected_display_callcount', [(COCKROACH_HANDLE, 3), ('@sqlite', 2)]
    )
    def test_constraints(
        self, handle, expected_display_callcount, sql_magic, ipython_namespace, mock_display
    ):
        sql_magic.execute(rf'{handle} \d str_table')

        assert (
            len(mock_display.call_args_list) == expected_display_callcount
        )  # main df, maybe index html, constraints df-as-html

        # The constraints HTML blob will be the final one always.
        constraint_html = mock_display.call_args_list[-1].args[0].data
        assert constraint_html.startswith('<br />\n<h2>Check Constraints</h2>')

        # Convert back to dataframe
        constraint_df = pd.read_html(constraint_html)[0]

        assert len(constraint_df) == 3  # Three check constraints on this table

        assert constraint_df.columns.tolist() == [
            'Constraint',
            'Definition',
        ]

        # Should be alpha sorted by constraint name.
        assert constraint_df['Constraint'].tolist() == [
            'never_f_10',
            'only_even_int_col_values',
            'single_char_str_id',
        ]

        # The SQL dialects convert the constraint expressions back to strings with slightly
        # varying spellings (as expected), so can't simply blindly assert all of them.
        constraint_definitions = constraint_df['Definition'].tolist()

        # This one happens to be regurgitated consistently between sqlite and CRDB.
        assert 'length(str_id) = 1' in constraint_definitions
        # Little gentler substring matching for the other two.
        assert any("str_id = 'f'" in cd for cd in constraint_definitions)
        assert any("int_col % 2" in cd for cd in constraint_definitions)

    def test_no_args_gets_table_list(self, sql_magic, ipython_namespace):
        sql_magic.execute(r'@sqlite \d')
        results = ipython_namespace['_']

        # Should have given us schema + table list instead of single-table details.
        assert len(results) == 4
        assert results.columns.tolist() == ['Schema', 'Relation', 'Kind']
        assert results['Relation'].tolist() == [
            'int_table',
            'references_int_table',
            'str_int_view',
            'str_table',
        ]

    def test_hate_more_than_one_arg(self, sql_magic):
        with pytest.raises(MetaCommandException) as excinfo:
            sql_magic.execute(r'@sqlite \d foo bar')
            assert str(excinfo).startswith(r'Usage: \d [[schema].[relation_name]]')

    def test_nonexistent_table(self, sql_magic):
        with pytest.raises(MetaCommandException) as excinfo:
            sql_magic.execute(rf'{COCKROACH_HANDLE} \d foobar')
            assert str(excinfo).startswith(r'Relation foobar does not exist')

    def test_nonexistent_schema_qualified_table(self, sql_magic):
        with pytest.raises(MetaCommandException) as excinfo:
            sql_magic.execute(rf'{COCKROACH_HANDLE} \d public.foobar')
            assert str(excinfo).startswith(r'Relation public.foobar does not exist')

    def test_nonexistent_schema(self, sql_magic):
        with pytest.raises(MetaCommandException) as excinfo:
            sql_magic.execute(rf'{COCKROACH_HANDLE} \d sdfsdfsdf.foobar')
            assert str(excinfo).startswith(r'Relation sdfsdfsdf.foobar does not exist')


@pytest.mark.usefixtures("populated_cockroach_database")
class TestFullIntrospection:
    @pytest.fixture()
    def patched_relation_structure_messager(self, tmp_path):
        original_jwt_pathname = RelationStructureMessager.JWT_PATHNAME
        tmp_jwt_path = tmp_path / 'kernel_jwt'
        tmp_jwt_path.write_text('kerneljwtcontents')

        RelationStructureMessager.JWT_PATHNAME = str(tmp_jwt_path)

        # Also make it only buffer at most 2 relations so we'll get some intermediate flushes
        # when introspecting more than 2 relations.

        original_capacity = RelationStructureMessager.CAPACITY

        RelationStructureMessager.CAPACITY = 2

        yield

        RelationStructureMessager.JWT_PATHNAME = original_jwt_pathname
        RelationStructureMessager.CAPACITY = original_capacity

    @pytest.fixture()
    def patched_requests_mock(self, patched_relation_structure_messager, requests_mock):
        # RelationStructureMessager will do POSTs to this URL.
        requests_mock.post(
            f"http://gate.default/api/v1/datasources/{COCKROACH_UUID}/schema/relations",
            status_code=204,
        )

        # And deletes to this one.
        requests_mock.delete(
            f"http://gate.default/api/v1/datasources/{COCKROACH_UUID}/schema/relations",
            status_code=204,
        )

        yield requests_mock

    def test_full_introspection(self, sql_magic, capsys, patched_requests_mock):
        # Create some more tables, for a total of > 10, so that exiting the scope of
        # the RelationStructureMessager will have some dregs to POST.
        # (see RelationStructureMessager.__exit__())

        NUM_NEW_TABLES = 10
        for idx in range(NUM_NEW_TABLES):
            sql_magic.execute(
                rf'{COCKROACH_HANDLE} create table dregs_table_{idx} (id int primary key not null, name text)'
            )

        # Now introspect the whole DB.
        introspection_start = datetime.utcnow()
        sql_magic.execute(rf'{COCKROACH_HANDLE} \introspect')
        introspection_end = datetime.utcnow()

        out, err = capsys.readouterr()

        assert 'Exception' not in out

        # Expect the core tables plus our dregs_table.
        # (The 'Done.\n' comes from the 'create table' execution creating dregs_table.)
        assert f'Discovered {len(KNOWN_TABLES) + NUM_NEW_TABLES} relations' in out

        for name, kind in KNOWN_TABLES_AND_KINDS:
            assert f'Introspected {kind} public.{name}' in out
            assert f'Stored structure of public.{name}' in out

        assert 'Done introspecting and messaging gate' in out

        # The delete should be the last request, and should specify a since_when timestamp param
        # which should be between when introspection started and ended (in UTC).
        delete_request = patched_requests_mock.request_history[-1]
        assert delete_request.method == 'DELETE'
        provided_since_when: str = urllib.parse.parse_qs(delete_request.query)['older_than'][0]
        assert introspection_start < datetime.fromisoformat(provided_since_when) < introspection_end

        # Each POST body should be a RelationStructureDescription, whose
        # relation_names should add up to be all the expected ones.
        described_relation_names = set()

        # Across the tables, shoulda observed indexes, primary key names,
        # columns, ...
        had_columns = False
        had_primary_key_columns = False
        had_indexes = False
        had_unique_constraints = False
        had_check_constraints = False
        had_foreign_keys = False

        post_call_count = 0
        for req in patched_requests_mock.request_history:
            if req.method == 'POST' and req.url.endswith('/schema/relations'):
                post_call_count += 1
                # POST body should correspond to a list of RelationStructureDescription each describing a single relation.
                dict_list = req.json()
                for member_dict in dict_list:
                    from_json = RelationStructureDescription(**member_dict)
                    described_relation_names.add(from_json.relation_name)

                    if from_json.indexes:
                        had_indexes = True

                    if from_json.primary_key_name and from_json.primary_key_columns:
                        had_primary_key_columns = True
                    else:
                        #
                        assert (
                            from_json.primary_key_name is None
                            and from_json.primary_key_columns == []
                        )

                    if from_json.columns:
                        had_columns = True

                    if from_json.unique_constraints:
                        had_unique_constraints = True

                    if from_json.check_constraints:
                        had_check_constraints = True

                    if from_json.foreign_keys:
                        had_foreign_keys = True

        # Expected 7 POST calls for the 14 tables, since we fixture
        # tuned RelationStructureMessager.CAPACITY down to 2.
        assert post_call_count == 7

        # Every expected relation should have been described.
        expected_relation_names = set(KNOWN_TABLES)  # The default table set
        for idx in range(NUM_NEW_TABLES):
            expected_relation_names.add(f'dregs_table_{idx}')  # plus our per-test extras.
        assert described_relation_names == expected_relation_names

        # Every substructure variation should have been covered across all these tables/view.
        assert had_columns
        assert had_primary_key_columns
        assert had_indexes
        assert had_unique_constraints
        assert had_check_constraints
        assert had_foreign_keys

    @pytest.fixture()
    def patched_error_requests_mock(
        self,
        bad_port_number_cockroach_connection,
        requests_mock,
        patched_relation_structure_messager,
    ):
        conn_uuid, _ = bad_port_number_cockroach_connection

        # RelationStructureMessager will POST error report here
        requests_mock.post(
            f"http://gate.default/api/v1/datasources/{conn_uuid}/schema/introspection-error",
            status_code=204,
        )

        yield requests_mock

    def test_broken_datasource_bad_port_number_full_introspection_fail(
        self, sql_magic, capsys, patched_error_requests_mock, bad_port_number_cockroach_connection
    ):
        """Test that if an error, such as inability to connect to the datasource
        is encountered at introspection time, the error will be POSTed up to gate
        as per ENG-5774.

        This test tries to introspect a CRDB database with a bad port number that we
        cannot connect to.
        """

        _, bad_conn_handle = bad_port_number_cockroach_connection
        sql_magic.execute(rf'{bad_conn_handle} \introspect')

        out, err = capsys.readouterr()

        assert 'psycopg2.OperationalError' in err

        # Should have only posted to the error route
        assert len(patched_error_requests_mock.request_history) == 1
        post_error_request = patched_error_requests_mock.request_history[0]
        assert post_error_request.method == 'POST'
        assert post_error_request.url.endswith('schema/introspection-error')

        error_payload = json.loads(post_error_request.text)
        assert len(error_payload) == 1 and 'error' in error_payload

        posted_error_msg = error_payload['error']

        assert 'psycopg2.OperationalError' in posted_error_msg

    @pytest.fixture()
    def patch_make_all_pks_unnamed(self, mocker):
        """Monkeypatch SchemaStrippingInspector.get_pk_constraint to describe all primary keys as
        name = None, as MySQL might have"""

        orig_implementation = WrappedInspector.get_pk_constraint

        def get_pk_constaint_all_anonymous(
            self, table_name: str, schema: Optional[str] = None
        ) -> dict:
            """Make all primary keys smell unnamed, like some MySQL and/or SingleStore tables may smell"""
            orig_dict = orig_implementation(self, table_name, schema)
            orig_dict['name'] = None

            return orig_dict

        WrappedInspector.get_pk_constraint = get_pk_constaint_all_anonymous

        try:
            yield

        finally:
            # Repair the class.
            WrappedInspector.get_pk_constraint = orig_implementation

    @pytest.mark.usefixtures("populated_sqlite_database")
    def test_cannot_introspect_legacy_datasource(self, sql_magic, capsys):
        r"""Only datasource-uuid-based connections should be \introspect fodder"""

        sql_magic.execute(r'@sqlite \introspect')
        out, err = capsys.readouterr()

        assert err == 'Cannot introspect into this resource.\n'
        assert out == ''


class TestRelationStructure:
    """Unit tests over RelationStructureDescription pydantic validation."""

    def test_success_no_primary_key(self):
        # primary_key_name=None is how to describe no primary key / columns.
        self.try_cons(primary_key_name=None, primary_key_columns=[])

    @pytest.mark.parametrize('pkey_name', ['', 'foo_pkey'])
    def test_fail_primary_key_no_primary_key_columns(self, pkey_name):
        # Only None should be allowed, see test_success_no_primary_key()
        with pytest.raises(
            ValueError, match="No primary_key_columns requires primary_key_name = None"
        ):
            self.try_cons(primary_key_name=pkey_name, primary_key_columns=[])

    def test_fail_primary_key_columns_no_primary_key_name(self):
        with pytest.raises(
            ValueError, match="primary_key_columns requires nonempty primary_key_name"
        ):
            self.try_cons(primary_key_name=None, primary_key_columns=['id'])

    def test_success_view_with_definition(self):
        self.try_cons(kind='view', view_definition='select 1')

    @pytest.mark.parametrize('kind,view_defn', [('table', 'select 1'), ('view', None)])
    def test_fail_view_kind_vs_view_defn_mismatch(self, kind, view_defn):
        with pytest.raises(
            ValueError, match="Views require definitions; tables must not have view definition"
        ):
            self.try_cons(kind=kind, view_definition=view_defn)

    def try_cons(self, **overlay_kwargs) -> RelationStructureDescription:
        params = dict(
            schema_name='foo',
            relation_name='bar',
            kind='table',
            view_definition=None,
            primary_key_name=None,
            primary_key_columns=[],
            columns=[],
            indexes=[],
            unique_constraints=[],
            check_constraints=[],
            foreign_keys=[],
        )

        params.update(**overlay_kwargs)

        RelationStructureDescription(**params)


@pytest.mark.usefixtures("populated_sqlite_database")
class TestHelp:
    def test_general_help(self, sql_magic, ipython_namespace):
        sql_magic.execute(r'@sqlite \help')
        results = ipython_namespace['_']

        assert (
            len(results) == len(_all_command_classes) - 2
        )  # avoids talking about HelpCommand, IntrospectAndStoreDatabaseCommand
        assert results.columns.tolist() == ['Command', 'Description', 'Documentation']

        # Each description and documentation value should end with a period
        for column in ['Description', 'Documentation']:
            assert all(
                value.endswith('.') for value in results[column]
            ), f"{column} strings should be full sentences ending with a period: {[s for s in results[column] if not s.endswith('.')]}"

    # Both these specific commands should regurgitate the same help row.
    @pytest.mark.parametrize('cmdname', [r'\schemas', r'\dn+'])
    def test_single_topic_help(self, cmdname, sql_magic, ipython_namespace):
        sql_magic.execute(rf'@sqlite \help {cmdname}')
        results = ipython_namespace['_']

        assert len(results) == 1
        assert results.columns.tolist() == ['Command', 'Description', 'Documentation']
        assert results['Description'][0] == 'List schemas within database.'
        assert results['Command'][0] == r'\schemas, \schemas+, \dn, \dn+'
        assert results['Documentation'][0].startswith('List all the schemas')

    def test_help_hates_unknown_subcommands(self, sql_magic):
        with pytest.raises(MetaCommandException) as excinfo:
            sql_magic.execute(r'@sqlite \help \foo')
            assert str(excinfo) == 'Unknown command "\\foo"'

    def test_help_wants_at_most_a_single_arg(self, sql_magic):
        with pytest.raises(MetaCommandException) as excinfo:
            sql_magic.execute(r'@sqlite \help \foo \bar')
            assert str(excinfo) == 'Usage: \\help [command]'


@pytest.mark.usefixtures("populated_sqlite_database")
class TestMisc:
    def test_unknown_command(self, sql_magic):
        with pytest.raises(MetaCommandException) as excinfo:
            sql_magic.execute(r'@sqlite \unknown_subcommand')
            assert str(excinfo) == 'Unknown command \\unknown_subcommand'

    def test_handles_sql_comment_at_front(self, sql_magic, capsys, ipython_namespace):
        """Test that even if the cell starts with a comment line, can still invoke a meta-command properly"""
        sql_magic.execute('@sqlite help_df << -- this is a sql comment as first line\n\\help')
        help_df = ipython_namespace.get('help_df')

        out, err = capsys.readouterr()

        assert type(help_df) is pd.DataFrame
        assert help_df.columns.tolist() == ['Command', 'Description', 'Documentation']

        # Will have displayed a prerendered HTML table of the help dataframe.
        assert (
            '<IPython.core.display.HTML object>' in out
        )  # ... amoungst other things that \\help outputs!


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
        mock_conn = mocker.Mock(IntrospectableConnection)
        mock_conn.default_schema_name = 'default'
        assert parse_schema_and_relation_glob(mock_conn, inp) == expected_result

    def test_when_default_schema_is_not_available(self, mocker):
        """Test when the dialect's inspector doesn't distinguish any schema as the default, as BigQuery and Trino do"""
        mock_conn = mocker.Mock(InspectorProtocol)
        mock_conn.default_schema_name = None
        mock_conn.get_schema_names.side_effect = lambda: ['first', 'random_schema']

        assert parse_schema_and_relation_glob(mock_conn, 'foo') == ('first', 'foo')
        assert parse_schema_and_relation_glob(mock_conn, '*') == ('first', '*')
        assert parse_schema_and_relation_glob(mock_conn, '.') == ('first', '*')
        assert parse_schema_and_relation_glob(mock_conn, 'schema.foo*') == ('schema', 'foo*')


class TestWrappedInspector:
    @pytest.fixture()
    def wrapped_inspector(self):
        class AllUnimplmented:
            def __getattr__(self, attr):
                raise NotImplementedError

        return WrappedInspector(AllUnimplmented())

    def test_handles_unimplemented_get_view_definition(self, wrapped_inspector):
        assert wrapped_inspector.get_view_definition('foo', 'schema') == '(unobtainable)'

    def test_handles_unimplemented_get_check_constraints(self, wrapped_inspector):
        assert wrapped_inspector.get_check_constraints('foo', 'schema') == []

    def test_handles_unimplemented_get_unique_constraints(self, wrapped_inspector):
        assert wrapped_inspector.get_unique_constraints('foo', 'schema') == []


class TestBigQueryInspectorInspector:
    def test_strip_schema_strips(self):
        ssi = BigQueryInspector(None)
        assert ssi._strip_schema(['foo.t1', 'foo.t2'], 'foo') == ['t1', 't2']

    def test_strip_schema_if_no_schema(self):
        ssi = BigQueryInspector(None)
        assert ssi._strip_schema(['foo.t1', 'foo.t2']) == ['foo.t1', 'foo.t2']

    @pytest.mark.parametrize('schema,expected_view_name', [('foo', 'foo.bar'), ('', 'bar')])
    def test_get_view_definition(self, schema, expected_view_name, mocker):
        mock_underlying = mocker.Mock(Inspector)
        mock_underlying.get_view_definition.side_effect = (
            lambda view_name, schema: f'select * from {view_name}'
        )

        assert expected_view_name in BigQueryInspector(mock_underlying).get_view_definition(
            'bar', schema
        )

    def test_get_table_names_does_strip_schema(self, mocker):
        mock_underlying = mocker.Mock(Inspector)
        mock_underlying.get_table_names.side_effect = lambda _: ['foo.t1', 'foo.t2']

        assert BigQueryInspector(mock_underlying).get_table_names('foo') == ['t1', 't2']

    def test_get_view_names_does_strip_schema(self, mocker):
        mock_underlying = mocker.Mock(Inspector)
        mock_underlying.get_view_names.side_effect = lambda _: ['foo.v1', 'foo.v2']

        assert BigQueryInspector(mock_underlying).get_view_names('foo') == ['v1', 'v2']


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


class TestHandleNotImplemented:
    def test_returns_underlying_when_implemented(self):
        @handle_not_implemented(default='no')
        def func():
            return 12

        assert func() == 12

    def test_returns_default_when_not_implemented(self):
        @handle_not_implemented(default='no')
        def func():
            raise NotImplementedError

        assert func() == 'no'

    def test_returns_default_factory_when_not_implemented(self):
        @handle_not_implemented(default_factory=list)
        def func():
            raise NotImplementedError

        assert func() == []

    def test_hates_both_default_and_default_factory(self):
        with pytest.raises(AssertionError, match='only provide one'):

            @handle_not_implemented(default='no', default_factory=list)
            def func():
                raise NotImplementedError

    def test_requires_either_default_or_default_factory(self):
        with pytest.raises(AssertionError, match='must provide one of'):

            @handle_not_implemented()
            def func():
                raise NotImplementedError
