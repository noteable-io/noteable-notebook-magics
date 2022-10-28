import pytest


@pytest.mark.usefixtures("populated_sqlite_database")
class TestListSchemas:
    @pytest.mark.parametrize(
        'invocation,expect_extras',
        [
            (r'\schemas', False),
            (r'\schemas+', True),
            (r'\dn', False),
            (r'\dn+', True),
        ],
    )
    def test_list_schemas(self, invocation: str, expect_extras: bool, sql_magic):

        results = sql_magic.execute(f'@sqlite {invocation}')

        # Sqlite just has one schema, 'main', and is the default.
        assert len(results) == 1
        assert results['Schema'][0] == 'main'

        # wacky, if test with 'is', fails with 'assert True is True'
        assert results['Default'][0] == True  # noqa: E712

        if expect_extras:
            assert results['Table Count'][0] == 2  # int_table, str_table.
            # No views in this sqlite db
            assert len(results.columns) == 3
        else:
            assert len(results.columns) == 2

    def test_hates_arguments(self, sql_magic, capsys):
        sql_magic.execute(r'@sqlite \schemas foo')
        out, err = capsys.readouterr()
        assert (
            out
            == '\\schemas does not expect arguments\n(Use "\\help \\schemas"" for more assistance)\n'
        )


@pytest.mark.usefixtures("populated_sqlite_database")
class TestMisc:
    def test_unknown_command(self, sql_magic, capsys):
        sql_magic.execute(r'@sqlite \unknown_subcommand')
        out, err = capsys.readouterr()
        assert out == 'Unknown command \\unknown_subcommand\n(Use "\\help" for more assistance)\n'
