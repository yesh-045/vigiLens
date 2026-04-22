"""Tests for vigilens.integrations.llm_client — utility functions and retry logic."""

import asyncio
import base64

import aiohttp
import pytest

from vigilens.integrations.llm_client import (
    RETRYABLE_STATUS_CODES,
    construct_payload,
    is_retryable_exception,
    video_to_data_url,
)


# ── is_retryable_exception ─────────────────────────────────────────────────


class TestIsRetryableException:
    @pytest.mark.parametrize("status", sorted(RETRYABLE_STATUS_CODES))
    def test_retryable_status_codes(self, status):
        exc = aiohttp.ClientResponseError(
            request_info=None,
            history=(),
            status=status,  # type: ignore[arg-type]
        )
        assert is_retryable_exception(exc) is True

    @pytest.mark.parametrize("status", [200, 201, 400, 401, 403, 404, 422])
    def test_non_retryable_status_codes(self, status):
        exc = aiohttp.ClientResponseError(
            request_info=None,
            history=(),
            status=status,  # type: ignore[arg-type]
        )
        assert is_retryable_exception(exc) is False

    def test_connection_error_is_retryable(self):
        assert is_retryable_exception(aiohttp.ClientConnectionError()) is True

    def test_timeout_is_retryable(self):
        assert is_retryable_exception(asyncio.TimeoutError()) is True

    def test_generic_exception_is_not_retryable(self):
        assert is_retryable_exception(ValueError("boom")) is False

    def test_keyboard_interrupt_is_not_retryable(self):
        assert is_retryable_exception(KeyboardInterrupt()) is False


# ── video_to_data_url ──────────────────────────────────────────────────────


class TestVideoToDataUrl:
    def test_returns_data_url_with_correct_prefix(self, make_stub_mp4):
        path = make_stub_mp4("test.mp4")
        result = video_to_data_url(path)
        assert result.startswith("data:video/mp4;base64,")

    def test_custom_mime_type(self, make_stub_mp4):
        path = make_stub_mp4("test.webm")
        result = video_to_data_url(path, mime="video/webm")
        assert result.startswith("data:video/webm;base64,")

    def test_base64_is_decodable(self, make_stub_mp4):
        path = make_stub_mp4("test.mp4")
        result = video_to_data_url(path)
        b64_part = result.split(",", 1)[1]
        decoded = base64.b64decode(b64_part)
        # Must match original file bytes
        with open(path, "rb") as f:
            assert decoded == f.read()


# ── construct_payload ──────────────────────────────────────────────────────


class TestConstructPayload:
    def test_http_url_used_directly(self):
        urls = ["http://example.com/chunk1.mp4", "https://example.com/chunk2.mp4"]
        payload = construct_payload(urls)
        # Each URL produces 2 entries: a text entry (Chunk ID) + a video_url entry
        assert len(payload) == 4
        assert payload[1]["type"] == "video_url"
        assert payload[1]["video_url"]["url"] == urls[0]
        assert payload[3]["video_url"]["url"] == urls[1]

    def test_local_file_gets_base64_encoded(self, make_stub_mp4):
        path = make_stub_mp4("local_chunk.mp4")
        payload = construct_payload([path])
        assert len(payload) == 2
        assert payload[0]["type"] == "text"
        assert "local_chunk" in payload[0]["text"]
        assert payload[1]["type"] == "video_url"
        assert payload[1]["video_url"]["url"].startswith("data:video/mp4;base64,")

    def test_chunk_id_extracted_from_basename(self):
        url = "http://cdn.example.com/stream/chunks/chunk_20260101_120000.mp4"
        payload = construct_payload([url])
        assert "chunk_20260101_120000" in payload[0]["text"]

    def test_empty_list_returns_empty(self):
        assert construct_payload([]) == []

    def test_mixed_local_and_remote(self, make_stub_mp4):
        local = make_stub_mp4("c1.mp4")
        remote = "https://cdn.example.com/c2.mp4"
        payload = construct_payload([local, remote])
        assert len(payload) == 4
        # First pair: local → data url
        assert payload[1]["video_url"]["url"].startswith("data:")
        # Second pair: remote → direct url
        assert payload[3]["video_url"]["url"] == remote

