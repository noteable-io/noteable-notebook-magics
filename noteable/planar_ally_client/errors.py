import json
from typing import Any


class PlanarAllyError(Exception):
    def __init__(self, msg):
        super().__init__(msg or "There was an error while making that request.")

    def user_error(self) -> str:
        return str(self)


class PlanarAllyAPIError(PlanarAllyError):
    def __init__(self, status_code: int, body: Any, operation: str):
        self.status_code = status_code
        self.body = body
        self.operation = operation
        super().__init__(
            f"received {self.status_code} status from remove service for {self.operation}"
        )

    def user_error(self) -> str:
        try:
            if not isinstance(self.body, dict):
                self.body = json.loads(self.body)

            if message := self.body.get("detail"):
                return f"There was an error while doing the {self.operation} operation.\n{message}"
            if errors := self.body.get("errors"):
                lines = []
                for error in errors:
                    lines.append(f"{' '.join(error['loc'])}: {error['msg']}")
                message = '\n'.join(lines)
                return f"There was an error while doing the {self.operation} operation.\n{message}"
        except json.JSONDecodeError:
            pass

        return (
            f"There was an error while doing the {self.operation} operation. "
            f"Contact support with error code {self.status_code}."
        )


class PlanarAllyBadAPIResponseError(PlanarAllyError):
    def __init__(self):
        super().__init__("Unable to parse response from remote service.")


class PlanarAllyAPITimeoutError(PlanarAllyError):
    def __init__(self, operation):
        super().__init__(f"Timed out waiting on operation: {operation}")


class PlanarAllyUnableToConnectError(PlanarAllyError):
    def __init__(self, operation):
        super().__init__(f"Unable to connect to remote service to perform operation '{operation}'")
