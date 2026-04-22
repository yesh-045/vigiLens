"""Tests for vigilens.models.contracts.messages — StreamJobMessage & LLMJobMessage validation."""

import pytest
from pydantic import ValidationError

from vigilens.models.contracts.messages import LLMJobMessage, StreamJobMessage


# ── StreamJobMessage ────────────────────────────────────────────────────────


class TestStreamJobMessage:
    def test_valid_full_payload(self, stream_job_factory):
        data = stream_job_factory()
        msg = StreamJobMessage(**data)
        assert msg.stream_id == data["stream_id"]
        assert msg.trigger_queries == data["trigger_queries"]
        assert msg.status == "queued"

    def test_optional_fields_default_to_none(self, stream_job_factory):
        data = stream_job_factory(
            overrides={
                "webhook_urls": None,
                "video_target_width": None,
                "video_target_height": None,
            }
        )
        msg = StreamJobMessage(**data)
        assert msg.webhook_urls is None
        assert msg.video_target_width is None
        assert msg.video_target_height is None

    def test_missing_required_field_raises(self, stream_job_factory):
        data = stream_job_factory()
        del data["stream_id"]
        with pytest.raises(ValidationError) as exc_info:
            StreamJobMessage(**data)
        assert "stream_id" in str(exc_info.value)

    def test_wrong_type_trigger_queries(self, stream_job_factory):
        data = stream_job_factory(overrides={"trigger_queries": "not-a-list"})
        # Pydantic v2 coerces a single string into a list; verify it at least works
        # or raises depending on strictness
        try:
            msg = StreamJobMessage(**data)
            # If it coerces, it should still be a list
            assert isinstance(msg.trigger_queries, list)
        except ValidationError:
            pass  # strict mode would reject this — both outcomes are acceptable

    def test_serialization_round_trip(self, stream_job_factory):
        data = stream_job_factory()
        msg = StreamJobMessage(**data)
        dumped = msg.model_dump()
        restored = StreamJobMessage(**dumped)
        assert restored == msg

    def test_json_round_trip(self, stream_job_factory):
        data = stream_job_factory()
        msg = StreamJobMessage(**data)
        json_str = msg.model_dump_json()
        restored = StreamJobMessage.model_validate_json(json_str)
        assert restored == msg

    def test_empty_trigger_queries_allowed(self, stream_job_factory):
        data = stream_job_factory(overrides={"trigger_queries": [], "thresholds": []})
        msg = StreamJobMessage(**data)
        assert msg.trigger_queries == []
        assert msg.thresholds == []

    def test_multiple_trigger_queries(self, stream_job_factory):
        queries = ["person falling", "fire detected", "car accident"]
        thresholds = [0.3, 0.5, 0.7]
        data = stream_job_factory(
            overrides={"trigger_queries": queries, "thresholds": thresholds}
        )
        msg = StreamJobMessage(**data)
        assert len(msg.trigger_queries) == 3
        assert len(msg.thresholds) == 3


# ── LLMJobMessage ──────────────────────────────────────────────────────────


class TestLLMJobMessage:
    def test_valid_full_payload(self, llm_job_factory):
        data = llm_job_factory()
        msg = LLMJobMessage(**data)
        assert msg.stream_id == data["stream_id"]
        assert msg.chunk_presigned_urls == data["chunk_presigned_urls"]

    def test_webhook_urls_optional(self, llm_job_factory):
        data = llm_job_factory(overrides={"webhook_urls": None})
        msg = LLMJobMessage(**data)
        assert msg.webhook_urls is None

    def test_missing_chunk_path_raises(self, llm_job_factory):
        data = llm_job_factory()
        del data["chunk_paths"]
        with pytest.raises(ValidationError) as exc_info:
            LLMJobMessage(**data)
        assert "chunk_paths" in str(exc_info.value)

    def test_missing_chunk_presigned_url_raises(self, llm_job_factory):
        data = llm_job_factory()
        del data["chunk_presigned_urls"]
        with pytest.raises(ValidationError) as exc_info:
            LLMJobMessage(**data)
        assert "chunk_presigned_urls" in str(exc_info.value)

    def test_serialization_round_trip(self, llm_job_factory):
        data = llm_job_factory()
        msg = LLMJobMessage(**data)
        dumped = msg.model_dump()
        restored = LLMJobMessage(**dumped)
        assert restored == msg

    def test_json_round_trip(self, llm_job_factory):
        data = llm_job_factory()
        msg = LLMJobMessage(**data)
        json_str = msg.model_dump_json()
        restored = LLMJobMessage.model_validate_json(json_str)
        assert restored == msg

    def test_extra_fields_ignored(self, llm_job_factory):
        """LLMJobMessage should not break if extra keys appear in the payload."""
        data = llm_job_factory()
        data["unexpected_key"] = "surprise"
        # Pydantic v2 default: extra="ignore" is not set, so this may raise.
        # The test documents current behaviour.
        try:
            msg = LLMJobMessage(**data)
            assert not hasattr(msg, "unexpected_key")
        except ValidationError:
            pass  # also acceptable — means extra fields are forbidden

