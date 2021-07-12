from typing import Any, Dict

import requests
import structlog
from urllib3.util import Timeout

from . import errors
from .types import FileKind, RemoteStatus, UserMessage

logger = structlog.get_logger(__name__)


class PlanarAllyAPI:
    def __init__(
        self,
        base_url: str = "http://localhost:7000/api",
        version: str = "v0",
        default_total_timeout_seconds: float = 60.0,
    ):
        self._base_url = f"{base_url}/{version}/"
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "noteable-notebook-magics"
        self._default_timeout = Timeout(total=default_total_timeout_seconds, connect=0.5)

    def fs(self, kind: FileKind) -> "FileSystemAPI":
        if kind is FileKind.dataset:
            return DatasetFileSystemAPI(self, kind)
        return FileSystemAPI(self, kind)

    def post(self, endpoint: str, operation: str, **kwargs) -> Dict[str, Any]:
        return self._request("POST", endpoint, operation, **kwargs)

    def delete(self, endpoint: str, operation: str, **kwargs) -> Dict[str, Any]:
        return self._request("DELETE", endpoint, operation, **kwargs)

    def get(self, endpoint: str, operation: str, **kwargs) -> Dict[str, Any]:
        return self._request("GET", endpoint, operation, **kwargs)

    def _request(self, method: str, endpoint: str, operation: str, **kwargs) -> Dict[str, Any]:
        full_url = f"{self._base_url}{endpoint}"
        kwargs.setdefault("timeout", self._default_timeout)
        logger.debug(
            "making api request to planar-ally",
            method=method,
            endpoint=endpoint,
            operation=operation,
        )

        try:
            resp = self._session.request(method, full_url, **kwargs)
        except requests.Timeout as e:
            raise errors.PlanarAllyAPITimeoutError(operation) from e
        except requests.ConnectionError as e:
            raise errors.PlanarAllyUnableToConnectError() from e

        return self._check_response(resp, operation)

    def _check_response(self, resp: requests.Response, operation: str) -> Dict[str, Any]:
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


class DatasetFileSystemAPI(FileSystemAPI):
    def __init__(self, api: PlanarAllyAPI, kind: FileKind):
        super().__init__(api, kind)

    def delete(self, path: str, **kwargs) -> UserMessage:
        raise errors.PlanarAllyError("delete is not supported for dataset files")

    def move(self, path: str, **kwargs) -> UserMessage:
        raise errors.PlanarAllyError("move is not supported for dataset files")

    def get_remote_status(self, path: str, **kwargs) -> RemoteStatus:
        raise errors.PlanarAllyError("get_remote_status is not supported for dataset files")
