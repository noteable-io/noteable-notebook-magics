import pytest

from noteable_magics.logging import configure_logging
from noteable_magics.planar_ally_client.api import PlanarAllyAPI
from noteable_magics.planar_ally_client.types import FileKind


@pytest.fixture(scope="session", autouse=True)
def _configure_logging():
    configure_logging(True, "INFO", "DEBUG")


@pytest.fixture()
def api():
    yield PlanarAllyAPI()


@pytest.fixture
def fs(api):
    return api.fs(FileKind.project)


@pytest.fixture
def ds(api):
    return api.fs(FileKind.dataset)


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


@pytest.fixture()
def mock_success():
    yield MockResponse(
        {"message": "Success", 'file_changes': [{'path': 'foo/bar', 'change_type': 'added'}]}, 200
    )
