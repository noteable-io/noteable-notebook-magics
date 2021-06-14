from typing import Any


class PlanarAllyError(Exception):
    def __init__(self, msg):
        super().__init__(msg or "There was an error while making that request, contact support.")

    def user_error(self) -> str:
        return str(self)


class PlanarAllyAPIError(PlanarAllyError):
    def __init__(self, status_code: int, body: Any, operation: str):
        self.status_code = status_code
        self.body = body
        self.operation = operation
        super().__init__(
            f"received {self.status_code} status from planar-ally for {self.operation}"
        )

    def user_error(self) -> str:
        return (
            f"There was an error while doing the {self.operation} operation. "
            f"Contact support with error code {self.status_code}."
        )


class PlanarAllyBadAPIResponseError(PlanarAllyError):
    def __init__(self):
        super().__init__("Unable to parse response from remote service, contact support.")


class PlanarAllyAPITimeoutError(PlanarAllyError):
    def __init__(self, operation):
        super().__init__(f"Timed out waiting on operation: {operation}")
