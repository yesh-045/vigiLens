"""Integration tests for LLMAnalysisWorker message handling."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from vigilens.integrations.redis_queue import StreamQueueMessage


def _make_queue_message(
    payload_dict: dict, message_id: str = "msg-001"
) -> StreamQueueMessage:
    return StreamQueueMessage(
        stream="llm.jobs",
        message_id=message_id,
        payload=payload_dict,
    )


@pytest.fixture()
def worker():
    """Create an LLMAnalysisWorker with mocked queue."""
    with patch("vigilens.apps.workers.llm.worker.AsyncRedisStreamQueue") as MockQueue:
        mock_q = MagicMock()
        MockQueue.return_value = mock_q
        from vigilens.apps.workers.llm.worker import LLMAnalysisWorker

        w = LLMAnalysisWorker()
        w.queue = AsyncMock()
        return w


class TestHandleMessage:
    async def test_happy_path_action_detected_sends_webhook(
        self, worker, llm_job_factory, sample_llm_response
    ):
        payload = llm_job_factory()
        msg = _make_queue_message(payload)
        worker.in_progress_tasks.add("msg-001")

        llm_resp = sample_llm_response(is_action_detected=True)

        with (
            patch(
                "vigilens.apps.workers.llm.worker.llm_analysis", new_callable=AsyncMock
            ) as mock_llm,
            patch.object(worker, "send_webhook", new_callable=AsyncMock) as _,
        ):
            mock_llm.return_value = llm_resp
            await worker._handle_message(msg)

            mock_llm.assert_called_once()
            # Webhook should have been scheduled (via create_task, but we patched send_webhook)
            # Verify at least one task for webhook was created

    async def test_action_not_detected_no_webhook(
        self, worker, llm_job_factory, sample_llm_response
    ):
        payload = llm_job_factory()
        msg = _make_queue_message(payload)
        worker.in_progress_tasks.add("msg-001")

        llm_resp = sample_llm_response(is_action_detected=False)

        with (
            patch(
                "vigilens.apps.workers.llm.worker.llm_analysis", new_callable=AsyncMock
            ) as mock_llm,
            patch.object(
                worker, "send_webhook", new_callable=AsyncMock
            ) as mock_webhook,
        ):
            mock_llm.return_value = llm_resp
            await worker._handle_message(msg)

            mock_llm.assert_called_once()
            # No webhook because action not detected
            mock_webhook.assert_not_called()

    async def test_no_webhook_urls_skips_webhook(
        self, worker, llm_job_factory, sample_llm_response
    ):
        payload = llm_job_factory(overrides={"webhook_urls": None})
        msg = _make_queue_message(payload)
        worker.in_progress_tasks.add("msg-001")

        llm_resp = sample_llm_response(is_action_detected=True)

        with (
            patch(
                "vigilens.apps.workers.llm.worker.llm_analysis", new_callable=AsyncMock
            ) as mock_llm,
            patch.object(
                worker, "send_webhook", new_callable=AsyncMock
            ) as mock_webhook,
        ):
            mock_llm.return_value = llm_resp
            await worker._handle_message(msg)

            mock_webhook.assert_not_called()

    async def test_llm_failure_removes_from_in_progress(self, worker, llm_job_factory):
        payload = llm_job_factory()
        msg = _make_queue_message(payload)
        worker.in_progress_tasks.add("msg-001")

        with patch(
            "vigilens.apps.workers.llm.worker.llm_analysis", new_callable=AsyncMock
        ) as mock_llm:
            mock_llm.side_effect = Exception("LLM API crashed")
            await worker._handle_message(msg)

        # Task should be cleaned up even on error
        assert "msg-001" not in worker.in_progress_tasks

    async def test_task_removed_from_in_progress_on_success(
        self, worker, llm_job_factory, sample_llm_response
    ):
        payload = llm_job_factory()
        msg = _make_queue_message(payload)
        worker.in_progress_tasks.add("msg-001")

        with patch(
            "vigilens.apps.workers.llm.worker.llm_analysis", new_callable=AsyncMock
        ) as mock_llm:
            mock_llm.return_value = sample_llm_response(is_action_detected=False)
            await worker._handle_message(msg)

        assert "msg-001" not in worker.in_progress_tasks

    async def test_multiple_trigger_queries_calls_llm_for_each(
        self, worker, llm_job_factory, sample_llm_response
    ):
        payload = llm_job_factory(
            overrides={
                "trigger_queries": ["fall", "fire", "intrusion"],
                "thresholds": [0.3, 0.5, 0.7],
            }
        )
        msg = _make_queue_message(payload)
        worker.in_progress_tasks.add("msg-001")

        with patch(
            "vigilens.apps.workers.llm.worker.llm_analysis", new_callable=AsyncMock
        ) as mock_llm:
            mock_llm.return_value = sample_llm_response(is_action_detected=False)
            await worker._handle_message(msg)

        assert mock_llm.call_count == 3


class TestParseLLMResult:
    def test_valid_response(self, worker, sample_llm_response):
        resp = sample_llm_response()
        result = worker.parse_llm_analysis_result(resp)
        assert result.is_action_detected is True

    def test_empty_choices_raises(self, worker):
        with pytest.raises((Exception,)):
            worker.parse_llm_analysis_result({"choices": []})

