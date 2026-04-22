"""Integration tests for vigilens.services.screening.screen_chunk."""

import json
from unittest.mock import patch

import pytest
from yarl import URL

try:
    from aioresponses import aioresponses

    HAS_AIORESPONSES = True
except ImportError:
    HAS_AIORESPONSES = False

pytestmark = [
    pytest.mark.skipif(not HAS_AIORESPONSES, reason="aioresponses not installed"),
    pytest.mark.asyncio,
]


SCREENER_BASE_URL = "http://screener-test:9999"


@pytest.fixture(autouse=True)
def _patch_screener_settings():
    with patch("vigilens.services.screening.settings") as ms:
        ms.screener_timeout = 5
        ms.screener_base_url = SCREENER_BASE_URL
        ms.screener_api_key = "test-key"
        ms.screener_model = "test-model"
        yield ms


@pytest.fixture()
def mock_video_to_data_url():
    """Avoid reading actual files in screener tests."""
    with patch(
        "vigilens.services.screening.video_to_data_url",
        return_value="data:video/mp4;base64,AAAA",
    ) as m:
        yield m


class TestScreenChunk:
    async def test_successful_screening(self, mock_video_to_data_url):
        from vigilens.services.screening import screen_chunk

        response_body = {
            "data": [
                {"score": 0.85},
                {"score": 0.12},
            ]
        }

        with aioresponses() as m:
            m.post(
                f"{SCREENER_BASE_URL}/v1/score",
                payload=response_body,
            )
            result = await screen_chunk("/tmp/chunk.mp4", ["person falling", "fire"])
            assert result["data"][0]["score"] == 0.85
            assert result["data"][1]["score"] == 0.12

    async def test_server_error_retries(self, mock_video_to_data_url):
        """500 errors should trigger retries (tenacity)."""
        from vigilens.services.screening import screen_chunk

        with aioresponses() as m:
            # First 2 calls fail, third succeeds
            m.post(f"{SCREENER_BASE_URL}/v1/score", status=500)
            m.post(f"{SCREENER_BASE_URL}/v1/score", status=500)
            m.post(
                f"{SCREENER_BASE_URL}/v1/score",
                payload={"data": [{"score": 0.9}]},
            )
            result = await screen_chunk("/tmp/chunk.mp4", ["fall"])
            assert result["data"][0]["score"] == 0.9

    async def test_non_retryable_error_raises_immediately(self, mock_video_to_data_url):
        """A 400 error should not be retried."""
        from aiohttp import ClientResponseError
        from vigilens.services.screening import screen_chunk

        with aioresponses() as m:
            m.post(f"{SCREENER_BASE_URL}/v1/score", status=400)
            with pytest.raises(ClientResponseError):
                await screen_chunk("/tmp/chunk.mp4", ["fall"])

    async def test_sends_correct_model(self, mock_video_to_data_url):
        from vigilens.services.screening import screen_chunk

        with aioresponses() as m:
            m.post(
                f"{SCREENER_BASE_URL}/v1/score",
                payload={"data": [{"score": 0.5}]},
            )
            await screen_chunk("/tmp/chunk.mp4", ["query"])
            # Verify the request body
            url_key = ("POST", URL(f"{SCREENER_BASE_URL}/v1/score"))
            call = m.requests[url_key][0]
            body = json.loads(call.kwargs.get("data", "{}"))
            assert body["model"] == "test-model"

    async def test_sends_authorization_header(self, mock_video_to_data_url):
        from vigilens.services.screening import screen_chunk

        with aioresponses() as m:
            m.post(
                f"{SCREENER_BASE_URL}/v1/score",
                payload={"data": [{"score": 0.5}]},
            )
            await screen_chunk("/tmp/chunk.mp4", ["query"])
            url_key = ("POST", URL(f"{SCREENER_BASE_URL}/v1/score"))
            call = m.requests[url_key][0]
            assert "Bearer test-key" in call.kwargs.get("headers", {}).get(
                "Authorization", ""
            )

