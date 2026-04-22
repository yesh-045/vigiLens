"""Integration tests for LLMAnalysisWorker webhook delivery."""

from unittest.mock import MagicMock, patch

import pytest
from vigilens.models.contracts.prompts import VideoAnalysisResult, VideoAnalysisResultList
from vigilens.apps.workers.llm.webhook import send_webhook_with_retry

try:
    from aioresponses import aioresponses

    HAS_AIORESPONSES = True
except ImportError:
    HAS_AIORESPONSES = False

pytestmark = [
    pytest.mark.skipif(not HAS_AIORESPONSES, reason="aioresponses not installed"),
    pytest.mark.asyncio,
]


@pytest.fixture()
def worker():
    with patch("vigilens.apps.workers.llm.worker.AsyncRedisStreamQueue") as MockQueue:
        MockQueue.return_value = MagicMock()
        from vigilens.apps.workers.llm.worker import LLMAnalysisWorker

        w = LLMAnalysisWorker()
        return w


class TestSendWebhook:
    @staticmethod
    def _message(is_action_detected: bool = True) -> VideoAnalysisResultList:
        return VideoAnalysisResultList(
            results=[
                VideoAnalysisResult(
                    video_path="chunk_001",
                    analysis="analysis",
                    title="person",
                    key_identifiers=["person"],
                    key_frame_numbers=[1],
                    is_action_detected=is_action_detected,
                    is_continuation=False,
                )
            ],
            is_action_detected=is_action_detected,
        )

    async def test_successful_delivery(self, worker):
        urls = ["http://hook1.example.com/alert", "http://hook2.example.com/alert"]
        message = self._message()
        chunk_presigned_urls = ["http://x.com/chunk_001.mp4"]

        with aioresponses() as m:
            m.post(urls[0], status=200)
            m.post(urls[1], status=200)
            # Should not raise
            await worker.send_webhook(urls, message, chunk_presigned_urls)

    async def test_partial_failure_does_not_raise(self, worker):
        """If one webhook fails but another succeeds, send_webhook should not raise."""
        urls = ["http://good.example.com/hook", "http://bad.example.com/hook"]
        message = self._message()
        chunk_presigned_urls = ["http://x.com/chunk_001.mp4"]

        with aioresponses() as m:
            m.post(urls[0], status=200)
            m.post(urls[1], status=500)
            m.post(urls[1], status=500)
            m.post(urls[1], status=500)  # all retries fail
            # Should complete without raising (failures are logged)
            await worker.send_webhook(urls, message, chunk_presigned_urls)

    async def test_all_fail_does_not_raise(self, worker):
        urls = ["http://fail1.example.com", "http://fail2.example.com"]
        message = self._message()
        chunk_presigned_urls = ["http://x.com/chunk_001.mp4"]

        with aioresponses() as m:
            for url in urls:
                for _ in range(3):  # 3 retry attempts
                    m.post(url, status=500)
            await worker.send_webhook(urls, message, chunk_presigned_urls)


class TestSendWebhookWithRetry:
    async def test_retries_on_server_error(self, worker):
        url = "http://hook.example.com/alert"
        message = TestSendWebhook._message()
        chunk_presigned_urls = ["http://x.com/chunk_001.mp4"]
        with aioresponses() as m:
            m.post(url, status=500)
            m.post(url, status=502)
            m.post(url, status=200)  # succeeds on third try
            await send_webhook_with_retry(url, message, chunk_presigned_urls)

    async def test_gives_up_after_max_retries(self, worker):
        from tenacity import RetryError

        url = "http://hook.example.com/alert"
        message = TestSendWebhook._message()
        chunk_presigned_urls = ["http://x.com/chunk_001.mp4"]
        with aioresponses() as m:
            for _ in range(5):
                m.post(url, status=500)
            with pytest.raises((RetryError, Exception)):
                await send_webhook_with_retry(url, message, chunk_presigned_urls)

    async def test_non_retryable_error_fails_fast(self, worker):
        from aiohttp import ClientResponseError

        url = "http://hook.example.com/alert"
        message = TestSendWebhook._message()
        chunk_presigned_urls = ["http://x.com/chunk_001.mp4"]
        with aioresponses() as m:
            m.post(url, status=401)
            with pytest.raises(ClientResponseError):
                await send_webhook_with_retry(url, message, chunk_presigned_urls)

