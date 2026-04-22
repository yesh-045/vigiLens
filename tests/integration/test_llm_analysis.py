"""Integration tests for vigilens.integrations.llm_client.llm_analysis."""

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

LLM_BASE_URL = "http://llm-test:8888"


@pytest.fixture(autouse=True)
def _patch_llm_settings():
    with patch("vigilens.integrations.llm_client.settings") as ms:
        ms.llm_timeout = 5
        ms.llm_base_url = LLM_BASE_URL
        ms.llm_api_key = "test-llm-key"
        ms.llm_model = "test-model"
        yield ms


class TestLLMAnalysis:
    async def test_successful_call(self, sample_llm_response):
        from vigilens.integrations.llm_client import llm_analysis
        from vigilens.models.contracts.prompts import VideoAnalysisResultList

        resp = sample_llm_response()

        with aioresponses() as m:
            m.post(f"{LLM_BASE_URL}/chat/completions", payload=resp)
            result = await llm_analysis(
                "Analyze the video",
                "person falling",
                VideoAnalysisResultList,
                ["http://example.com/chunk.mp4"],
            )
            assert "choices" in result
            assert result["choices"][0]["message"]["content"]

    async def test_sends_correct_model(self, sample_llm_response):
        from vigilens.integrations.llm_client import llm_analysis
        from vigilens.models.contracts.prompts import VideoAnalysisResultList

        with aioresponses() as m:
            m.post(
                f"{LLM_BASE_URL}/chat/completions",
                payload=sample_llm_response(),
            )
            await llm_analysis(
                "prompt", "query", VideoAnalysisResultList, ["http://x.com/c.mp4"]
            )
            url_key = ("POST", URL(f"{LLM_BASE_URL}/chat/completions"))
            call = m.requests[url_key][0]
            body = call.kwargs.get("json", {})
            assert body["model"] == "test-model"

    async def test_sends_auth_header(self, sample_llm_response):
        from vigilens.integrations.llm_client import llm_analysis
        from vigilens.models.contracts.prompts import VideoAnalysisResultList

        with aioresponses() as m:
            m.post(
                f"{LLM_BASE_URL}/chat/completions",
                payload=sample_llm_response(),
            )
            await llm_analysis(
                "prompt", "query", VideoAnalysisResultList, ["http://x.com/c.mp4"]
            )
            url_key = ("POST", URL(f"{LLM_BASE_URL}/chat/completions"))
            call = m.requests[url_key][0]
            assert "Bearer test-llm-key" in call.kwargs.get("headers", {}).get(
                "Authorization", ""
            )

    async def test_includes_json_schema_response_format(self, sample_llm_response):
        from vigilens.integrations.llm_client import llm_analysis
        from vigilens.models.contracts.prompts import VideoAnalysisResultList

        with aioresponses() as m:
            m.post(
                f"{LLM_BASE_URL}/chat/completions",
                payload=sample_llm_response(),
            )
            await llm_analysis(
                "prompt", "query", VideoAnalysisResultList, ["http://x.com/c.mp4"]
            )
            url_key = ("POST", URL(f"{LLM_BASE_URL}/chat/completions"))
            call = m.requests[url_key][0]
            body = call.kwargs.get("json", {})
            assert body["response_format"]["type"] == "json_schema"
            schema = body["response_format"]["json_schema"]["schema"]
            assert "is_action_detected" in schema.get("properties", {})

    async def test_retries_on_server_error(self, sample_llm_response):
        from vigilens.integrations.llm_client import llm_analysis
        from vigilens.models.contracts.prompts import VideoAnalysisResultList

        with aioresponses() as m:
            m.post(f"{LLM_BASE_URL}/chat/completions", status=500)
            m.post(f"{LLM_BASE_URL}/chat/completions", status=502)
            m.post(
                f"{LLM_BASE_URL}/chat/completions",
                payload=sample_llm_response(),
            )
            result = await llm_analysis(
                "prompt", "query", VideoAnalysisResultList, ["http://x.com/c.mp4"]
            )
            assert "choices" in result

    async def test_non_retryable_error_raises(self, sample_llm_response):
        from aiohttp import ClientResponseError
        from vigilens.integrations.llm_client import llm_analysis
        from vigilens.models.contracts.prompts import VideoAnalysisResultList

        with aioresponses() as m:
            m.post(f"{LLM_BASE_URL}/chat/completions", status=401)
            with pytest.raises(ClientResponseError):
                await llm_analysis(
                    "prompt", "query", VideoAnalysisResultList, ["http://x.com/c.mp4"]
                )

    async def test_query_included_in_messages(self, sample_llm_response):
        from vigilens.integrations.llm_client import llm_analysis
        from vigilens.models.contracts.prompts import VideoAnalysisResultList

        with aioresponses() as m:
            m.post(
                f"{LLM_BASE_URL}/chat/completions",
                payload=sample_llm_response(),
            )
            await llm_analysis(
                "prompt",
                "detect person falling",
                VideoAnalysisResultList,
                ["http://x.com/c.mp4"],
            )
            url_key = ("POST", URL(f"{LLM_BASE_URL}/chat/completions"))
            call = m.requests[url_key][0]
            body = call.kwargs.get("json", {})
            user_content = body["messages"][2]["content"]
            # First element is the text query
            assert any("detect person falling" in str(c) for c in user_content)

