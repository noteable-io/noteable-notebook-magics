from typing import Any, Dict

import requests
from urllib3.util import Timeout

from .errors import PlanarAllyAPIError, PlanarAllyAPITimeoutError, PlanarAllyBadAPIResponseError
from .types import FileKind, RemoteStatus, UserMessage


class PlanarAllyAPI:
    def __init__(
        self,
        base_url: str = "http://localhost:7000/api",
        version: str = "v0",
        total_timeout_seconds: float = 60.0,
    ):
        self._base_url = f"{base_url}/{version}/"
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "noteable-notebook-magics"
        self._timeout = Timeout(total=total_timeout_seconds, connect=0.5)

    def fs(self, kind: FileKind) -> "FileSystemAPI":
        return FileSystemAPI(self, kind)

    def post(self, endpoint: str, operation: str) -> Dict[str, Any]:
        return self._request("POST", endpoint, operation)

    def delete(self, endpoint: str, operation: str) -> Dict[str, Any]:
        return self._request("DELETE", endpoint, operation)

    def get(self, endpoint: str, operation: str) -> Dict[str, Any]:
        return self._request("GET", endpoint, operation)

    def _request(self, method: str, endpoint: str, operation: str, **kwargs) -> Dict[str, Any]:
        full_url = f"{self._base_url}{endpoint}"
        kwargs.setdefault("timeout", self._timeout)

        try:
            resp = self._session.request(method, full_url, **kwargs)
        except requests.Timeout:
            raise PlanarAllyAPITimeoutError(operation)

        return self._check_response(resp, operation)

    def _check_response(self, resp: requests.Response, operation: str) -> Dict[str, Any]:
        try:
            response = resp.json()
        except ValueError:
            response = resp.text

        if resp.status_code != 200:
            raise PlanarAllyAPIError(resp.status_code, response, operation)
        elif isinstance(response, (str, bytes)):
            raise PlanarAllyBadAPIResponseError()

        return response


class FileSystemAPI:
    def __init__(self, api: PlanarAllyAPI, kind: FileKind):
        self._api = api
        self._kind = kind
        self._url_prefix = f"fs/{self._kind}"

    def pull(self, path: str) -> UserMessage:
        resp = self._api.post(f"{self._url_prefix}/{path}/pull", "pull files")
        return UserMessage.parse_obj(resp)

    def push(self, path: str) -> UserMessage:
        resp = self._api.post(f"{self._url_prefix}/{path}/push", "push files")
        return UserMessage.parse_obj(resp)

    def delete(self, path: str) -> UserMessage:
        resp = self._api.delete(f"{self._url_prefix}/{path}", "delete files")
        return UserMessage.parse_obj(resp)

    def move(self, path: str) -> UserMessage:
        resp = self._api.post(f"{self._url_prefix}/{path}/move", "move files")
        return UserMessage.parse_obj(resp)

    def get_remote_status(self, path: str) -> RemoteStatus:
        resp = self._api.get(f"{self._url_prefix}/{path}/status", "get file status")
        return RemoteStatus.parse_obj(resp)
