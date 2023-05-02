"""API types returned and used with planar-ally"""
import enum
from enum import Enum, auto
from typing import Generic, List, TypeVar

from pydantic import BaseModel, Field
from pydantic.generics import GenericModel


class UserMessage(BaseModel):
    message: str = Field(
        description="A human readable message that should be displayed to the requesting user"
    )


class RemoteFileChangeType(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

    modified = auto()
    added = auto()
    deleted = auto()


class RemoteFileChange(BaseModel):
    change_type: RemoteFileChangeType = Field(
        description="The type of file change that happened remotely compared to the "
        "local kernel's file system"
    )
    path: str = Field(
        description="The path of the file that was changed, relative to the provided prefix"
    )

    @property
    def style(self) -> str:
        if self.is_added():
            return "green"
        elif self.is_deleted():
            return "red"
        elif self.is_modified():
            return "yellow"
        return ""

    @property
    def change_prefix(self):
        if self.is_added():
            return "added"
        elif self.is_deleted():
            return "deleted"
        elif self.is_modified():
            return "modified"
        return ""

    def is_added(self) -> bool:
        return self.change_type is RemoteFileChangeType.added

    def is_modified(self) -> bool:
        return self.change_type is RemoteFileChangeType.modified

    def is_deleted(self) -> bool:
        return self.change_type is RemoteFileChangeType.deleted


class RemoteStatus(BaseModel):
    file_changes: List[RemoteFileChange]

    def has_changes(self) -> bool:
        return len(self.file_changes) > 0


class FileKind(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

    project = auto()
    dataset = auto()

    def __str__(self):
        return self.value


class StreamType(enum.Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

    error = enum.auto()

    file_progress_start = enum.auto()
    file_progress_update = enum.auto()
    file_progress_end = enum.auto()


class StreamHeader(BaseModel):
    type: StreamType


StreamMessageContentT = TypeVar("StreamMessageContentT")


class StreamMessage(GenericModel, Generic[StreamMessageContentT]):
    header: StreamHeader
    content: StreamMessageContentT


class StreamErrorContent(BaseModel):
    detail: str
    status_code: int


class StreamErrorMessage(StreamMessage[StreamErrorContent]):
    """An error in the stream processing"""

    header: StreamHeader = StreamHeader(type=StreamType.error)


class FileProgressStartMessage(StreamMessage[UserMessage]):
    """The start of a file progress stream"""

    header: StreamHeader = StreamHeader(type=StreamType.file_progress_start)

    @classmethod
    def new(cls, message: str) -> "FileProgressStartMessage":
        return cls(content=UserMessage(message=message))


class FileProgressUpdateContent(BaseModel):
    file_name: str
    percent_complete: float = Field(description="0.0 to 1.0 percent complete")


class FileProgressUpdateMessage(StreamMessage[FileProgressUpdateContent]):
    """An update from a file progress stream"""

    header: StreamHeader = StreamHeader(type=StreamType.file_progress_update)


class FileProgressEndMessage(StreamMessage[UserMessage]):
    """The end of a file progress stream"""

    header: StreamHeader = StreamHeader(type=StreamType.file_progress_end)

    @classmethod
    def new(cls, message: str) -> "FileProgressEndMessage":
        return cls(content=UserMessage(message=message))
