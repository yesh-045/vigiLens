"""Contract tests — ensure message schemas are compatible between producers and consumers.

These tests verify that:
1. The API service produces StreamJobMessage payloads that the StreamProcessWorker can consume.
2. The StreamProcessWorker produces LLMJobMessage payloads that the LLMAnalysisWorker can consume.
3. The webhook payload matches expected structure.
"""

import json
import uuid


from vigilens.models.contracts.messages import LLMJobMessage, StreamJobMessage


class TestAPIToStreamWorkerContract:
    """The API endpoint builds a StreamJobMessage dict and enqueues it.
    The StreamProcessWorker deserialises it with StreamJobMessage.model_validate.
    These tests verify the round-trip.
    """

    def _simulate_api_payload(self) -> dict:
        """Reproduce the dict the API builds in submit_stream()."""
        stream_id = f"s_{uuid.uuid4().hex[:12]}"
        return StreamJobMessage(
            stream_id=stream_id,
            tenant_id="t_test",
            name=f"stream_{stream_id}",
            rtsp_url="rtsp://cam.local/stream",
            trigger_queries=["person falling", "fire"],
            thresholds=[0.5, 0.7],
            webhook_urls=["http://hook.example.com"],
            chunk_seconds=10,
            fps=1,
            video_target_width=640,
            video_target_height=360,
            status="queued",
        ).model_dump()

    def test_api_payload_is_valid_stream_job_message(self):
        payload = self._simulate_api_payload()
        msg = StreamJobMessage.model_validate(payload)
        assert msg.status == "queued"

    def test_api_payload_survives_json_serialization(self):
        """The payload goes through Redis as JSON — verify the round-trip."""
        payload = self._simulate_api_payload()
        json_str = json.dumps(payload)
        restored = json.loads(json_str)
        msg = StreamJobMessage.model_validate(restored)
        assert msg.stream_id == payload["stream_id"]

    def test_api_payload_with_none_optionals(self):
        payload = self._simulate_api_payload()
        payload["webhook_urls"] = None
        payload["video_target_width"] = None
        payload["video_target_height"] = None
        msg = StreamJobMessage.model_validate(payload)
        assert msg.webhook_urls is None

    def test_api_payload_multiple_queries_with_matching_thresholds(self):
        """trigger_queries and thresholds must have the same length."""
        payload = self._simulate_api_payload()
        payload["trigger_queries"] = ["a", "b", "c"]
        payload["thresholds"] = [0.1, 0.2, 0.3]
        msg = StreamJobMessage.model_validate(payload)
        assert len(msg.trigger_queries) == len(msg.thresholds)


class TestStreamWorkerToLLMWorkerContract:
    """StreamProcessWorker.handle_selected_chunks builds an LLMJobMessage and
    enqueues it for the LLMAnalysisWorker.
    """

    def _simulate_llm_job_payload(self) -> dict:
        """Reproduce the dict built in handle_selected_chunks()."""
        return LLMJobMessage(
            stream_id="s_abc123",
            chunk_paths=["s3://bucket/s_abc123/chunks/chunk_001.mp4"],
            chunk_presigned_urls=[
                "http://minio:9000/bucket/s_abc123/chunks/chunk_001.mp4?sig=x"
            ],
            trigger_queries=["person falling"],
            thresholds=[0.5],
            webhook_urls=["http://hook.example.com"],
        ).model_dump()

    def test_llm_job_payload_is_valid(self):
        payload = self._simulate_llm_job_payload()
        msg = LLMJobMessage.model_validate(payload)
        assert msg.stream_id == "s_abc123"

    def test_llm_job_payload_survives_json_serialization(self):
        payload = self._simulate_llm_job_payload()
        json_str = json.dumps(payload)
        restored = json.loads(json_str)
        msg = LLMJobMessage.model_validate(restored)
        assert msg.chunk_paths == payload["chunk_paths"]

    def test_llm_job_without_webhooks(self):
        payload = self._simulate_llm_job_payload()
        payload["webhook_urls"] = None
        msg = LLMJobMessage.model_validate(payload)
        assert msg.webhook_urls is None

    def test_llm_job_with_multiple_queries(self):
        payload = self._simulate_llm_job_payload()
        payload["trigger_queries"] = ["fall", "fire"]
        payload["thresholds"] = [0.3, 0.7]
        msg = LLMJobMessage.model_validate(payload)
        assert len(msg.trigger_queries) == 2


class TestWebhookPayloadContract:
    """Verify the webhook payload structure from LLMAnalysisWorker._handle_message."""

    def test_webhook_payload_has_required_keys(self):
        """The worker sends this structure to webhook URLs."""
        from vigilens.models.contracts.prompts import VideoAnalysisResult

        result = VideoAnalysisResult(
            video_path="chunk_001",
            title="Person falling",
            analysis="Fall detected.",
            key_identifiers=["person"],
            key_frame_numbers=[10],
            is_action_detected=True,
            is_continuation=False,
        )

        # Reproduce the webhook message from _handle_message
        webhook_message = {
            "stream_id": "s_abc",
            "trigger_query": "person falling",
            "result": result.model_dump_json(),
            "chunk_path": "s3://bucket/chunk.mp4",
            "chunk_presigned_url": "http://presigned.url",
        }

        assert "stream_id" in webhook_message
        assert "trigger_query" in webhook_message
        assert "result" in webhook_message
        assert "chunk_path" in webhook_message
        assert "chunk_presigned_url" in webhook_message

        # The result should be parseable back
        parsed = VideoAnalysisResult.model_validate_json(webhook_message["result"])
        assert parsed.is_action_detected is True

    def test_webhook_payload_is_json_serializable(self):
        from vigilens.models.contracts.prompts import VideoAnalysisResult

        result = VideoAnalysisResult(
            video_path="chunk_002",
            analysis="No action.",
            title="No action",
            key_identifiers=[],
            key_frame_numbers=[],
            is_action_detected=False,
            is_continuation=False,
        )

        webhook_message = {
            "stream_id": "s_xyz",
            "trigger_query": "fire",
            "result": result.model_dump_json(),
            "chunk_path": "s3://bucket/chunk.mp4",
            "chunk_presigned_url": "http://presigned.url",
        }

        # Must be JSON-serializable
        serialized = json.dumps(webhook_message)
        restored = json.loads(serialized)
        assert restored["stream_id"] == "s_xyz"

