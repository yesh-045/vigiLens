"""
Integration test fixtures — fakeredis, aioresponses, mocked S3, mocked settings.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

try:
    import fakeredis
    import fakeredis.aioredis

    HAS_FAKEREDIS = True
except ImportError:
    HAS_FAKEREDIS = False

try:
    from aioresponses import aioresponses as _aioresponses

    HAS_AIORESPONSES = True
except ImportError:
    HAS_AIORESPONSES = False


# ---------------------------------------------------------------------------
# Fake Redis
# ---------------------------------------------------------------------------


@pytest.fixture()
def fake_redis_server():
    """A shared in-process fakeredis server (sync)."""
    if not HAS_FAKEREDIS:
        pytest.skip("fakeredis not installed")
    server = fakeredis.FakeServer()
    yield server


@pytest.fixture()
def fake_redis_url():
    """Placeholder URL — actual connection is replaced by fakeredis patches."""
    return "redis://fake:6379/0"


# ---------------------------------------------------------------------------
# aioresponses
# ---------------------------------------------------------------------------


@pytest.fixture()
def aio_responses():
    """Context-managed aiohttp mock."""
    if not HAS_AIORESPONSES:
        pytest.skip("aioresponses not installed")
    with _aioresponses() as m:
        yield m


# ---------------------------------------------------------------------------
# Mocked S3 client
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_s3_client():
    """Returns a MagicMock that behaves like vigilens.integrations.storage.S3Client."""
    client = MagicMock()
    client.bucket = "test-bucket"
    client.upload_chunk_to_s3.return_value = {
        "url": "s3://test-bucket/stream/chunks/chunk_001.mp4",
        "presigned_url": "http://localhost:9000/test-bucket/stream/chunks/chunk_001.mp4?sig=abc",
    }
    client.upload_file.return_value = "s3://test-bucket/some/key"
    client.get_presigned_url.return_value = "http://localhost:9000/presigned"
    return client


# ---------------------------------------------------------------------------
# Mocked async Redis queue
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_async_queue():
    """Returns a fully async-mocked AsyncRedisStreamQueue."""
    q = AsyncMock()
    q.enqueue = AsyncMock(return_value="msg-id-001")
    q.dequeue = AsyncMock(return_value=[])
    q.ack = AsyncMock(return_value=1)
    q.ack_many = AsyncMock(return_value=0)
    q.close = AsyncMock()
    return q

