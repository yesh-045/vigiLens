"""Tests for vigilens.models.contracts.prompts — VideoAnalysisResult model."""

import json

import pytest
from pydantic import ValidationError

from vigilens.models.contracts.prompts import VideoAnalysisResult, LLM_VIDEO_ANALYSIS_PROMPT


class TestVideoAnalysisResult:
    def test_valid_construction(self):
        r = VideoAnalysisResult(
            video_path="chunk_001",
            title="person falling",
            analysis="Detected a person falling in frames 10–15.",
            key_identifiers=["person", "floor"],
            key_frame_numbers=[10, 12, 15],
            is_action_detected=True,
            is_continuation=False,
        )
        assert r.is_action_detected is True
        assert r.video_path == "chunk_001"
        assert len(r.key_identifiers) == 2

    def test_action_not_detected(self):
        r = VideoAnalysisResult(
            video_path="chunk_002",
            title="no action",
            analysis="No relevant action observed.",
            key_identifiers=[],
            key_frame_numbers=[],
            is_action_detected=False,
            is_continuation=False,
        )
        assert r.is_action_detected is False
        assert r.key_identifiers == []

    def test_missing_video_path_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            VideoAnalysisResult(
                analysis="test",
                title="test",
                key_identifiers=[],
                key_frame_numbers=[],
                is_action_detected=False,
                is_continuation=False,
            )
        assert "video_path" in str(exc_info.value)

    def test_missing_is_action_detected_raises(self):
        with pytest.raises(ValidationError):
            VideoAnalysisResult(
                video_path="chunk_001",
                analysis="test",
                key_identifiers=[],
                key_frame_numbers=[],
                # is_action_detected intentionally missing
            )

    def test_json_round_trip(self):
        r = VideoAnalysisResult(
            video_path="chunk_003",
            analysis="Something happened.",
            key_identifiers=["car"],
            key_frame_numbers=[5],
            title="car",
            is_action_detected=True,
            is_continuation=False,
        )
        json_str = r.model_dump_json()
        restored = VideoAnalysisResult.model_validate_json(json_str)
        assert restored == r

    def test_model_validate_json_from_raw_string(self):
        """Simulate what parse_llm_analysis_result does with LLM output."""
        raw = json.dumps(
            {
                "video_path": "chunk_004",
                "analysis": "Fall detected.",
                "key_identifiers": ["person"],
                "title": "person falling",
                "key_frame_numbers": [1, 2, 3],
                "is_action_detected": True,
                "is_continuation": False,
            }
        )
        result = VideoAnalysisResult.model_validate_json(raw)
        assert result.is_action_detected is True

    def test_invalid_json_raises(self):
        with pytest.raises(ValidationError):
            VideoAnalysisResult.model_validate_json("this is not json")

    def test_json_schema_generation(self):
        """The schema is sent to the LLM API — it must be JSON-serializable."""
        schema = VideoAnalysisResult.model_json_schema()
        assert "properties" in schema
        assert "video_path" in schema["properties"]
        assert "is_action_detected" in schema["properties"]
        # Must be JSON-serializable
        json.dumps(schema)


class TestPromptConstants:
    def test_prompt_is_non_empty_string(self):
        assert isinstance(LLM_VIDEO_ANALYSIS_PROMPT, str)
        assert len(LLM_VIDEO_ANALYSIS_PROMPT.strip()) > 20

    def test_prompt_mentions_video_analysis(self):
        prompt_lower = LLM_VIDEO_ANALYSIS_PROMPT.lower()
        assert "video" in prompt_lower
        assert "analys" in prompt_lower  # "analyst" or "analysis"

