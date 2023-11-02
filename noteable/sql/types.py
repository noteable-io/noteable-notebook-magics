import enum
from typing import List, Optional

from pydantic import field_validator, ConfigDict, BaseModel, Field, model_validator

"""
Pydantic Types used for neutrally describing structures within SQL databases
"""


@enum.unique
class RelationKind(str, enum.Enum):
    """Enumeration differentating between tables and views"""

    table = "table"
    view = "view"
    model_config = ConfigDict(extra="forbid")


class ColumnModel(BaseModel):
    """Pydantic model defining a column of an introspected table or view."""

    name: str
    is_nullable: bool
    data_type: str
    default_expression: Optional[str] = None
    comment: Optional[str] = None
    model_config = ConfigDict(extra="forbid")


class IndexModel(BaseModel):
    """Pydantic model defining an index."""

    name: str
    is_unique: bool
    columns: List[str]
    model_config = ConfigDict(extra="forbid")


class UniqueConstraintModel(BaseModel):
    """Pydantic model defining a unique constraint."""

    name: str
    columns: List[str]
    model_config = ConfigDict(extra="forbid")


class CheckConstraintModel(BaseModel):
    """Pydantic model defining a check constraint."""

    name: str
    expression: str
    model_config = ConfigDict(extra="forbid")


class ForeignKeysModel(BaseModel):
    """Pydantic model defining a foreign key constraint."""

    name: str
    # `referenced_schema` Can be constructed with None, but will be promoted to empty string.
    referenced_schema: Optional[str] = None
    referenced_relation: str
    columns: List[str]
    referenced_columns: List[str]

    @field_validator('referenced_schema')
    @classmethod
    def validate_schema_name(cls, v):
        """Promote from None to empty string"""
        if v is None:
            v = ''

        return v

    @model_validator(mode="after")
    def check_lists_same_length(self):
        if len(self.columns) != len(self.referenced_columns):
            raise ValueError("columns and referenced_columns must be same length")

        return self

    model_config = ConfigDict(extra="forbid")


class RelationStructureDescription(BaseModel):
    """Pydantic model describing the POST structure kernel-space will use to describe a table or view within a data connection."""

    # First, the singular fields.
    schema_name: Optional[str] = Field(
        None,
        description="Name of schema containing the relation. Empty string for degenerate value.",
    )
    relation_name: str = Field(description="Name of the table or view")
    kind: RelationKind = Field(description="Relation type: table or a view")
    relation_comment: Optional[str] = Field(
        None, description="Optional comment describing the relation."
    )
    view_definition: Optional[str] = Field(None, description="Definition of the view if kind=view")
    primary_key_name: Optional[str] = Field(
        None, description="Name of the primary key constraint, if any"
    )

    # Now the plural fields.
    primary_key_columns: List[str] = Field(
        description="List of column names comprising the primary key, if any."
    )
    columns: List[ColumnModel] = Field(description="List of column definitions")
    indexes: List[IndexModel] = Field(description="List of index definitions")
    unique_constraints: List[UniqueConstraintModel] = Field(
        description="List of unique constraint definitions"
    )
    check_constraints: List[CheckConstraintModel] = Field(
        description="List of check constraint definitions"
    )
    foreign_keys: List[ForeignKeysModel] = Field(description="List of foreign key definitions")

    @field_validator('schema_name')
    @classmethod
    def validate_schema_name(cls, v):
        """Promote from None to empty string"""
        if v is None:
            v = ''

        return v

    @model_validator(mode="after")
    def view_definition_vs_kind(self):
        """Fail if a tring to describe a view with None for the view definition. At worst
        empty string is allowed.

        Likewise, if describing a table, then view definition _must_ be None.
        """
        if not (self.view_definition is None) == (self.kind == RelationKind.table):
            raise ValueError("Views require definitions; tables must not have view definition")

        return self

    @model_validator(mode="after")
    def pkey_name_only_if_has_pkey_columns(self):
        if len(self.primary_key_columns) > 0 and not self.primary_key_name:
            raise ValueError("primary_key_columns requires nonempty primary_key_name")
        elif len(self.primary_key_columns) == 0 and self.primary_key_name is not None:
            raise ValueError("No primary_key_columns requires primary_key_name = None")

        return self

    model_config = ConfigDict(extra="forbid")
