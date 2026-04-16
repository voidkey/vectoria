import pytest
from httpx import AsyncClient, ASGITransport

from config import get_settings
from main import app


@pytest.fixture(autouse=True)
def _disable_parser_isolation(monkeypatch):
    """Default-off for tests: in-process parsing so mocks on DocumentConverter
    and friends take effect. Tests exercising the subprocess pool directly
    (tests/test_parsers/test_isolation.py) bypass this by not calling parser
    classes.
    """
    monkeypatch.setattr(get_settings(), "parser_isolation", False)


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
