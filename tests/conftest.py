"""
Root conftest.py — shared fixtures for the entire test suite.

Provides:
  - Deterministic Settings (no .env file side-effects)
  - Message payload factories for StreamJobMessage / LLMJobMessage
  - Sample LLM API response builder
  - Tiny MP4 stub file generator
"""

import json
import uuid
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Settings fixture — override *before* any vigilens module is imported so the
# module-level `settings = get_settings()` picks up test values.
# ---------------------------------------------------------------------------

TEST_ENV = {
    "S3_ENDPOINT": "http://localhost:9000",
    "S3_BUCKET": "test-bucket",
    "AWS_ACCESS_KEY_ID": "testkey",
    "AWS_SECRET_ACCESS_KEY": "testsecret",
    "REDIS_URL": "redis://localhost:6379/0",
    "STREAM_JOB_STREAM": "test.stream.jobs",
    "STREAM_JOB_GROUP": "test.stream.processors",
    "LLM_JOB_STREAM": "test.llm.jobs",
    "LLM_JOB_GROUP": "test.llm.processors",
    "STREAM_TMP_DIR": "/tmp/test_streams",
    "SCREENER_BASE_URL": "http://localhost:9999",
    "SCREENER_API_KEY": "test-screener-key",
    "LLM_API_KEY": "test-llm-key",
    "LLM_BASE_URL": "http://localhost:8888",
    "LLM_MODEL": "test-model",
    "VIGILENS_API_ROOT_PATH": "",
}


@pytest.fixture()
def test_settings():
    """Return a Settings instance built from TEST_ENV only (no .env file)."""
    from vigilens.core.config import Settings

    return Settings(**TEST_ENV)


@pytest.fixture()
def _patch_settings(test_settings):
    """Monkey-patch the module-level ``settings`` and ``get_settings`` so every
    module that does ``from vigilens.core.config import settings`` uses the test values."""
    with (
        patch("vigilens.core.config.settings", test_settings),
        patch("vigilens.core.config.get_settings", return_value=test_settings),
    ):
        yield test_settings


# ---------------------------------------------------------------------------
# Message factories
# ---------------------------------------------------------------------------


def _make_stream_job(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = {
        "stream_id": f"s_{uuid.uuid4().hex[:12]}",
        "rtsp_url": "rtsp://localhost:8554/test",
        "name": "test_stream",
        "chunk_seconds": 10,
        "trigger_queries": ["person falling down"],
        "thresholds": [0.5],
        "webhook_urls": ["http://localhost:5000/hook"],
        "fps": 1,
        "video_target_width": 640,
        "video_target_height": 360,
        "status": "queued",
    }
    if overrides:
        base.update(overrides)
    return base


def _make_llm_job(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = {
        "stream_id": f"s_{uuid.uuid4().hex[:12]}",
        "chunk_paths": ["s3://test-bucket/stream/chunks/chunk_001.mp4"],
        "chunk_presigned_urls": [
            "http://localhost:9000/test-bucket/stream/chunks/chunk_001.mp4?sig=abc"
        ],
        "trigger_queries": ["person falling down"],
        "thresholds": [0.5],
        "webhook_urls": ["http://localhost:5000/hook"],
    }
    if overrides:
        base.update(overrides)
    return base


@pytest.fixture()
def stream_job_factory():
    """Factory fixture — call with optional overrides to get a StreamJobMessage dict."""
    return _make_stream_job


@pytest.fixture()
def llm_job_factory():
    """Factory fixture — call with optional overrides to get an LLMJobMessage dict."""
    return _make_llm_job


# ---------------------------------------------------------------------------
# Sample LLM API response
# ---------------------------------------------------------------------------


def make_llm_api_response(
    *,
    video_path: str = "chunk_001",
    title: str = "Person falling near staircase",
    analysis: str = "Action detected in frames 10-15",
    key_identifiers: Optional[List[str]] = None,
    key_frame_numbers: Optional[List[int]] = None,
    is_action_detected: bool = True,
    is_continuation: bool = False,
) -> Dict[str, Any]:
    """Build a dict that looks like the OpenAI chat-completions response."""
    content = json.dumps(
        {
            "results": [
                {
                    "video_path": video_path,
                    "title": title,
                    "analysis": analysis,
                    "key_identifiers": key_identifiers or ["person"],
                    "key_frame_numbers": key_frame_numbers or [10, 12, 15],
                    "is_action_detected": is_action_detected,
                    "is_continuation": is_continuation,
                }
            ],
            "is_action_detected": is_action_detected,
        }
    )
    return {
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": content,
                }
            }
        ]
    }


@pytest.fixture()
def sample_llm_response():
    return make_llm_api_response


# ---------------------------------------------------------------------------
# Tiny MP4 stub file (valid enough for os.path.getsize, but not for ffprobe)
# ---------------------------------------------------------------------------

# Minimal ftyp box — just enough bytes so the file is non-empty and looks like
# an MP4 container on disk.
_FTYP_BOX = (
    b"\x00\x00\x00\x14"  # box size = 20
    b"ftyp"  # box type
    b"isom"  # major brand
    b"\x00\x00\x00\x00"  # minor version
    b"isom"  # compatible brand
)


@pytest.fixture()
def make_stub_mp4(tmp_path):
    """Factory: creates a tiny stub .mp4 file in tmp_path, returns the path."""

    def _create(name: str = "chunk_20260101_120000.mp4") -> str:
        p = tmp_path / name
        p.write_bytes(_FTYP_BOX)
        return str(p)

    return _create

