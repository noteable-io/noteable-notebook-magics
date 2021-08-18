import json
from typing import Any, ContextManager, Dict, Iterator, Optional, Union

import httpx
import structlog

from . import errors
from .types import (
    FileKind,
    FileProgressEndMessage,
    FileProgressStartMessage,
    FileProgressUpdateMessage,
    RemoteStatus,
    StreamErrorMessage,
    StreamHeader,
    StreamType,
    UserMessage,
)

logger = structlog.get_logger(__name__)
ResponseType = Optional[Union[Dict[str, Any], httpx.Response]]


class PlanarAllyAPI:
    def __init__(
        self,
        base_url: str = "http://localhost:7000",
        version: str = "v0",
        default_total_timeout_seconds: float = 60.0,
    ):
        self._base_url = base_url
        self._api_url = f"{base_url}/api/{version}/"
        self._client = httpx.Client(http2=True)
        self._client.headers["User-Agent"] = "noteable-notebook-magics"
        self._default_timeout = httpx.Timeout(default_total_timeout_seconds, connect=0.5)

    def fs(self, kind: FileKind) -> "FileSystemAPI":
        return FileSystemAPI(self, kind)

    def dataset_fs(self) -> "DatasetFileSystemAPI":
        return DatasetFileSystemAPI(self, FileKind.dataset)

    def change_log_level(self, app_log_level: str, ext_log_level: Optional[str] = None) -> None:
        self.post(
            "logs",
            "change log level",
            json={
                "new_app_level": app_log_level,
                "new_ext_level": ext_log_level,
            },
            base_url=f"{self._base_url}/instance/",
        )

    def post(self, endpoint: str, operation: str, **kwargs) -> ResponseType:
        return self._request("POST", endpoint, operation, **kwargs)

    def delete(self, endpoint: str, operation: str, **kwargs) -> ResponseType:
        return self._request("DELETE", endpoint, operation, **kwargs)

    def get(self, endpoint: str, operation: str, **kwargs) -> ResponseType:
        return self._request("GET", endpoint, operation, **kwargs)

    def _request(
        self, method: str, endpoint: str, operation: str, raw_response=False, **kwargs
    ) -> ResponseType:
        full_url = f"{kwargs.pop('base_url', self._api_url)}{endpoint}"
        kwargs.setdefault("timeout", self._default_timeout)
        logger.debug(
            "making api request to planar-ally",
            method=method,
            endpoint=endpoint,
            operation=operation,
        )

        try:
            resp = self._client.request(method, full_url, **kwargs)
        except httpx.TimeoutException as e:
            raise errors.PlanarAllyAPITimeoutError(operation) from e
        except httpx.HTTPError as e:
            raise errors.PlanarAllyUnableToConnectError(operation) from e

        if raw_response:
            if resp.status_code != 200:
                raise errors.PlanarAllyAPIError(resp.status_code, resp, operation)
            return resp

        return self._check_response(resp, operation)

    def stream(
        self, method: str, endpoint: str, operation: str, **kwargs
    ) -> ContextManager[httpx.Response]:
        full_url = f"{self._api_url}{endpoint}"
        kwargs.setdefault("timeout", self._default_timeout)
        logger.debug(
            "making api request to planar-ally",
            method=method,
            endpoint=endpoint,
            operation=operation,
        )

        try:
            return self._client.stream(method, full_url, **kwargs)
        except httpx.TimeoutException as e:
            raise errors.PlanarAllyAPITimeoutError(operation) from e
        except httpx.HTTPError as e:
            raise errors.PlanarAllyUnableToConnectError(operation) from e

    def _check_response(self, resp: httpx.Response, operation: str) -> Optional[Dict[str, Any]]:
        if resp.status_code == 204:
            return None

        try:
            response = resp.json()
        except ValueError:
            response = resp.text

        if resp.status_code != 200:
            raise errors.PlanarAllyAPIError(resp.status_code, response, operation)
        elif isinstance(response, (str, bytes)):
            raise errors.PlanarAllyBadAPIResponseError()

        return response


class FileSystemAPI:
    def __init__(self, api: PlanarAllyAPI, kind: FileKind):
        self._api = api
        self._kind = kind
        self._url_prefix = f"fs/{self._kind}"

    def pull(self, path: str, **kwargs) -> UserMessage:
        resp = self._api.post(f"{self._url_prefix}/{path}/pull", "pull files", **kwargs)
        return UserMessage.parse_obj(resp)

    def push(self, path: str, **kwargs) -> UserMessage:
        resp = self._api.post(f"{self._url_prefix}/{path}/push", "push files", **kwargs)
        return UserMessage.parse_obj(resp)

    def delete(self, path: str, **kwargs) -> UserMessage:
        resp = self._api.delete(f"{self._url_prefix}/{path}", "delete files", **kwargs)
        return UserMessage.parse_obj(resp)

    def move(self, path: str, **kwargs) -> UserMessage:
        resp = self._api.post(f"{self._url_prefix}/{path}/move", "move files", **kwargs)
        return UserMessage.parse_obj(resp)

    def get_remote_status(self, path: str, **kwargs) -> RemoteStatus:
        resp = self._api.get(f"{self._url_prefix}/{path}/status", "get file status", **kwargs)
        return RemoteStatus.parse_obj(resp)


class DatasetFileSystemAPI:
    def __init__(self, api: PlanarAllyAPI, kind: FileKind):
        self._api = api
        self._kind = kind
        self._url_prefix = f"fs/{self._kind}"

    def pull(self, path: str, **kwargs) -> "DatasetOperationStream":
        kwargs.update({"timeout": None})
        resp = self._api.stream("POST", f"{self._url_prefix}/{path}/pull", "pull files", **kwargs)
        return DatasetOperationStream(resp, "pull files")

    def push(self, path: str, **kwargs) -> "DatasetOperationStream":
        kwargs.update({"timeout": None})
        resp = self._api.stream("POST", f"{self._url_prefix}/{path}/push", "push files", **kwargs)
        return DatasetOperationStream(resp, "push files")


class DatasetOperationStream:
    _msg_type_lookup = {
        StreamType.file_progress_start: FileProgressStartMessage,
        StreamType.file_progress_update: FileProgressUpdateMessage,
        StreamType.file_progress_end: FileProgressEndMessage,
        StreamType.error: StreamErrorMessage,
    }

    def __init__(self, resp_ctx_mgr: ContextManager[httpx.Response], operation: str):
        self._resp_context_manager = resp_ctx_mgr
        self._operation = operation
        self._response: Optional[httpx.Response] = None
        self._lines: Optional[Iterator[str]] = None

    def __enter__(self):
        try:
            self._response = self._resp_context_manager.__enter__()
        except httpx.TimeoutException as e:
            raise errors.PlanarAllyAPITimeoutError(self._operation) from e
        except httpx.HTTPError as e:
            raise errors.PlanarAllyUnableToConnectError(self._operation) from e

        if self._response.status_code != 200:
            raise errors.PlanarAllyAPIError(self._response.status_code, None, self._operation)
        self._lines = self._response.iter_lines()
        return self

    def __exit__(self, *exc_info):
        return self._resp_context_manager.__exit__(*exc_info)

    def __iter__(self):
        return self

    def __next__(self):
        while not (line := next(self._lines)):
            pass

        parsed_line = json.loads(line)
        header = StreamHeader.parse_obj(parsed_line["header"])
        return self._msg_type_lookup[header.type].parse_obj(parsed_line)
