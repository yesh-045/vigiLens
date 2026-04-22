"""Tests for LLMAnalysisWorker.parse_llm_analysis_result."""

import json

import pytest
from pydantic import ValidationError

from vigilens.models.contracts.prompts import VideoAnalysisResultList
from vigilens.apps.workers.llm.worker import LLMAnalysisWorker


@pytest.fixture()
def worker():
    """Create a worker instance with mocked dependencies."""
    from unittest.mock import patch, MagicMock

    with patch("vigilens.apps.workers.llm.worker.AsyncRedisStreamQueue") as MockQueue:
        mock_queue = MagicMock()
        MockQueue.return_value = mock_queue
        w = LLMAnalysisWorker()
        return w


class TestParseLLMAnalysisResult:
    def test_valid_response(self, worker, sample_llm_response):
        resp = sample_llm_response(is_action_detected=True)
        result = worker.parse_llm_analysis_result(resp)
        assert isinstance(result, VideoAnalysisResultList)
        assert result.is_action_detected is True

    def test_action_not_detected(self, worker, sample_llm_response):
        resp = sample_llm_response(is_action_detected=False)
        result = worker.parse_llm_analysis_result(resp)
        assert result.is_action_detected is False

    def test_empty_choices_raises(self, worker):
        resp = {"choices": []}
        with pytest.raises((ValidationError, ValueError, IndexError)):
            worker.parse_llm_analysis_result(resp)

    def test_missing_choices_key_raises(self, worker):
        resp = {}
        with pytest.raises((ValidationError, ValueError)):
            worker.parse_llm_analysis_result(resp)

    def test_missing_content_raises(self, worker):
        resp = {"choices": [{"message": {}}]}
        with pytest.raises((ValidationError, ValueError)):
            worker.parse_llm_analysis_result(resp)

    def test_malformed_json_content_raises(self, worker):
        resp = {"choices": [{"message": {"content": "not valid json {{"}}]}
        with pytest.raises((ValidationError, ValueError)):
            worker.parse_llm_analysis_result(resp)

    def test_content_missing_required_field(self, worker):
        content = json.dumps(
            {
                "results": [
                    {
                        "video_path": "chunk_001",
                        "analysis": "something",
                    }
                ],
            }
        )
        resp = {"choices": [{"message": {"content": content}}]}
        with pytest.raises((ValidationError, ValueError)):
            worker.parse_llm_analysis_result(resp)

    def test_preserves_all_fields(self, worker, sample_llm_response):
        resp = sample_llm_response(
            video_path="my_chunk",
            title="Dog with red ball in hallway",
            analysis="Detailed analysis here",
            key_identifiers=["dog", "ball"],
            key_frame_numbers=[1, 5, 10],
            is_action_detected=True,
        )
        result = worker.parse_llm_analysis_result(resp)
        assert result.results[0].video_path == "my_chunk"
        assert result.results[0].title == "Dog with red ball in hallway"
        assert result.results[0].analysis == "Detailed analysis here"
        assert result.results[0].key_identifiers == ["dog", "ball"]
        assert result.results[0].key_frame_numbers == [1, 5, 10]

