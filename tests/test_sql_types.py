import pytest

from noteable.sql.types import ForeignKeysModel, RelationKind, RelationStructureDescription


class TestForeignKeysModel:
    def test_none_for_schema(self):
        """Ensure None -> '' promotion for referenced_schema."""
        fk = ForeignKeysModel(
            name='my_fk',
            referenced_schema=None,
            referenced_relation='foo',
            columns=['my_fk'],
            referenced_columns=['id'],
        )
        assert fk.referenced_schema == ''

    def test_non_null_schema(self):
        fk = ForeignKeysModel(
            name='my_fk',
            referenced_schema='public',
            referenced_relation='foo',
            columns=['my_fk'],
            referenced_columns=['id'],
        )
        assert fk.referenced_schema == 'public'


class TestRelationStructureDescription:
    def cons(self, **kwargs) -> RelationStructureDescription:
        """Construct a RelationStructureDescription with overriding kwargs"""
        init_dict = dict(
            relation_name='foo',
            kind=RelationKind.table,
            primary_key_columns=[],
            columns=[],
            indexes=[],
            unique_constraints=[],
            check_constraints=[],
            foreign_keys=[],
        )

        init_dict.update(kwargs)

        return RelationStructureDescription(**init_dict)

    def test_none_for_schema(self):
        # None schema name should be promoted to empty string
        structure = self.cons(schema_name=None)

        assert structure.schema_name == ''

    def test_hate_view_definition_on_a_table(self):
        with pytest.raises(ValueError, match='tables must not have'):
            self.cons(
                kind=RelationKind.table,
                view_definition='select * from bar where deleted_at is null',
            )

    def test_allow_view_definition_on_a_view(self):
        struct = self.cons(
            kind=RelationKind.view,
            view_definition='select * from bar where deleted_at is null',
        )
        assert struct.view_definition == 'select * from bar where deleted_at is null'

    def test_allow_empty_view_definition_on_a_view(self):
        # Some dialects will report things as views, but not able to get at a real definition.
        struct = self.cons(
            kind=RelationKind.view,
            view_definition='',
        )
        assert struct.view_definition == ''
