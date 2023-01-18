# Declare the message types

import enum
from typing import List, Optional

from pydantic import BaseModel, Field, root_validator

r"""
Messaging types used for SQL cell meta command \introspect when describing
the structure of a discovered table or view up to Gate for permanent storage for
front-end GUI schema navigation uses.

These types culminating in RelationStructureDescription are inputs to the Gate
POST route(s).
"""


@enum.unique
class RelationKind(str, enum.Enum):
    """Enumeration differentating between tables and views"""

    table = "table"
    view = "view"

    class Config:
        extra = "forbid"


class ColumnModel(BaseModel):
    """Pydantic model defining a column of an introspected table or view.

    Used in two contexts:
       * SQLAlchemy validation prior to assigning new value into DatasourceRelationDAO.columns JSONB column
       * Subcomponent of upcoming pydantic model(s) used for route I/O (ENG-5356, ENG-5359).
    """

    name: str
    is_nullable: bool
    data_type: str
    default_expression: Optional[str] = None
    comment: Optional[str] = None

    class Config:
        extra = "forbid"


class IndexModel(BaseModel):
    """Pydantic model defining an introspected index.

    Used in two contexts:
       * SQLAlchemy validation prior to assigning new value into DatasourceRelationDAO.indexes JSONB column
       * Subcomponent of upcoming pydantic models used for route I/O (ENG-5356, ENG-5359).
    """

    name: str
    is_unique: bool
    columns: List[str]

    class Config:
        extra = "forbid"


class UniqueConstraintModel(BaseModel):
    """Pydantic model defining an introspected unique constraint.

    Used in two contexts:
       * SQLAlchemy validation prior to assigning new value into DatasourceRelationDAO.unique_constraints JSONB column
       * Subcomponent of upcoming pydantic models used for route I/O (ENG-5356, ENG-5359).
    """

    name: str
    columns: List[str]

    class Config:
        extra = "forbid"


class CheckConstraintModel(BaseModel):
    """Pydantic model defining an introspected check constraint.

    Used in two contexts:
       * SQLAlchemy validation prior to assigning new value into DatasourceRelationDAO.check_constraints JSONB column
       * Subcomponent of upcoming pydantic models used for route I/O (ENG-5356, ENG-5359).
    """

    name: str
    expression: str

    class Config:
        extra = "forbid"


class ForeignKeysModel(BaseModel):
    """Pydantic model defining an introspected foreign key constraint.

    Used in two contexts:
       * SQLAlchemy validation prior to assigning new value into DatasourceRelationDAO.foreign_keys JSONB column
       * Subcomponent of upcoming pydantic models used for route I/O (ENG-5356, ENG-5359).
    """

    name: str
    referenced_schema: str
    referenced_relation: str
    columns: List[str]
    referenced_columns: List[str]

    @root_validator
    def check_lists_same_length(cls, values):
        if not ("columns" in values and "referenced_columns" in values):
            raise ValueError("columns and referenced_columns required")

        if len(values["columns"]) != len(values["referenced_columns"]):
            raise ValueError("columns and referenced_columns must be same length")

        return values

    class Config:
        extra = "forbid"


class RelationStructureDescription(BaseModel):
    """Pydantic model describing the POST structure kernel-space will use to describe a
    schema-discovered table or view within a datasource.
    """

    # First, the singular fields.
    schema_name: str = Field(
        description="Name of schema containing the relation. Empty string for degenerate value."
    )
    relation_name: str = Field(description="Name of the table or view")
    kind: RelationKind = Field(description="Relation type: table or a view")
    relation_comment: Optional[str] = Field(description="Optional comment describing the relation.")
    view_definition: Optional[str] = Field(description="Definition of the view if kind=view")
    primary_key_name: Optional[str] = Field(description="Name of the primary key constraint")

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

    @root_validator
    def view_definition_vs_kind(cls, values):
        if not (values.get("view_definition") is None) == (
            values.get("kind") == RelationKind.table
        ):
            raise ValueError("Views require definitions, tables must not have view definition")

        return values

    class Config:
        extra = "forbid"
