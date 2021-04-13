import uuid
from dataclasses import dataclass
from typing import List, Protocol, TypeVar

import backoff
import redis
from pydantic import BaseModel

from .pb.gen.s3.v1.sidecar_pb2 import (
    GetRemoteStatusOperation,
    Operation,
    PullResult,
    RemoteFileChangeType,
    RemoteStatus,
    ResultStatus,
    SyncFromS3Operation,
    SyncResult,
    SyncToS3Operation,
)


class ProtobufMessage(Protocol):
    def SerializeToString(self) -> bytes:
        ...

    def ParseFromString(self, s: bytes) -> None:
        ...


T = TypeVar("T")


def with_pb_identifier(pb: T) -> T:
    pb.identifier = uuid.uuid4().hex
    return pb


@dataclass(frozen=True)
class RequestRemoteStatusResponse:
    prefix: str
    redis_result_key: str


@dataclass(frozen=True)
class RequestProjectPushResponse:
    prefix: str
    redis_result_key: str


@dataclass(frozen=True)
class RequestProjectPullResponse:
    prefix: str
    redis_result_key: str


class RemoteFileChangeResponse(BaseModel):
    change_type: int
    path: str

    class Config:
        orm_mode = True

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
        return self.change_type == RemoteFileChangeType.REMOTE_FILE_CHANGE_TYPE_ADDED

    def is_modified(self) -> bool:
        return self.change_type == RemoteFileChangeType.REMOTE_FILE_CHANGE_TYPE_MODIFIED

    def is_deleted(self) -> bool:
        return self.change_type == RemoteFileChangeType.REMOTE_FILE_CHANGE_TYPE_DELETED


class RemoteStatusResponse(BaseModel):
    prefix: str
    file_changes: List[RemoteFileChangeResponse]

    class Config:
        orm_mode = True

    def has_changes(self) -> bool:
        return len(self.file_changes) > 0


class SyncResultResponse(BaseModel):
    prefix: str
    status: int
    status_message: str

    class Config:
        orm_mode = True

    def is_ok(self) -> bool:
        return self.status == ResultStatus.RESULT_STATUS_OK

    def is_error(self) -> bool:
        return self.status == ResultStatus.RESULT_STATUS_ERROR


class PullResultResponse(BaseModel):
    status: int
    status_message: str

    class Config:
        orm_mode = True

    def is_ok(self) -> bool:
        return self.status == ResultStatus.RESULT_STATUS_OK

    def is_error(self) -> bool:
        return self.status == ResultStatus.RESULT_STATUS_ERROR


class S3SidecarService:
    """The interface point for interacting with the S3 sidecar"""

    def __init__(
        self, redis_dsn: str, channel_name: str, redis_results_max_wait_time_seconds: int
    ) -> None:
        self._redis = redis.Redis.from_url(redis_dsn)
        self._channel_name = channel_name
        self._redis_results_max_wait_time_seconds = redis_results_max_wait_time_seconds

    def request_project_push(self, prefix: str) -> RequestProjectPushResponse:
        """Sends a message to redis to push the local project file changes to s3"""
        sync_to_s3_op = SyncToS3Operation(prefix=prefix)
        op = with_pb_identifier(Operation(sync_to_s3=sync_to_s3_op))

        self._redis.publish(self._channel_name, op.SerializeToString())
        return RequestProjectPushResponse(prefix=prefix, redis_result_key=op.identifier)

    def request_project_pull(self, prefix: str) -> RequestProjectPullResponse:
        """Sends a message to redis to pull the latest s3 files to the local kernel"""
        op = with_pb_identifier(Operation(sync_from_s3=SyncFromS3Operation(prefix=prefix)))
        self._redis.publish(self._channel_name, op.SerializeToString())
        return RequestProjectPullResponse(prefix=prefix, redis_result_key=op.identifier)

    def request_remote_status(self, prefix: str) -> RequestRemoteStatusResponse:
        """
        Send a request via redis to the s3 sidecar to put the status of the changes
        between the local kernel and s3 in a redis key.

        The user of this method should check the returned value's `redis_result_key` value,
        which will be a protobuf encoded result of the status.
        """
        remote_status_op = GetRemoteStatusOperation(prefix=prefix)
        op = with_pb_identifier(Operation(get_remote_status=remote_status_op))

        self._redis.publish(self._channel_name, op.SerializeToString())
        return RequestRemoteStatusResponse(prefix=prefix, redis_result_key=op.identifier)

    def retrieve_remote_status(self, identifier: str) -> RemoteStatusResponse:
        """Retrieve the remote status set in redis by the s3 sidecar."""
        status = self._retrieve_redis_result(identifier, RemoteStatus())
        return RemoteStatusResponse(
            prefix=status.prefix,
            file_changes=[RemoteFileChangeResponse.from_orm(c) for c in status.file_changes],
        )

    def retrieve_sync_result(self, identifier: str) -> SyncResultResponse:
        result = self._retrieve_redis_result(identifier, SyncResult())
        return SyncResultResponse.from_orm(result)

    def retrieve_pull_result(self, identifier: str) -> PullResultResponse:
        result = self._retrieve_redis_result(identifier, PullResult())
        return PullResultResponse.from_orm(result)

    def _retrieve_redis_result(self, identifier: str, pb_out: ProtobufMessage):
        @backoff.on_predicate(backoff.constant, max_time=self._redis_results_max_wait_time_seconds)
        def _get_redis_result():
            # This function will run until a truthy value is returned or max_time is reached.
            if res := self._redis.get(identifier):
                pb_out.ParseFromString(res)
                return pb_out
            return None

        if rres := _get_redis_result():
            return rres

        raise TimeoutError("timed out waiting for result in redis, try again")
