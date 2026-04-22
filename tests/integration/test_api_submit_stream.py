"""Integration tests for the FastAPI /streams/submit endpoint."""

import pytest
from unittest.mock import AsyncMock, patch

import httpx
import pytest_asyncio

pytestmark = pytest.mark.asyncio


@pytest.fixture()
def mock_queue():
    """Mock the module-level stream_job_queue used in vigilens.apps.api.stream."""
    q = AsyncMock()
    q.enqueue = AsyncMock(return_value="msg-001")
    return q


@pytest_asyncio.fixture()
async def client(mock_queue):
    """Create an httpx AsyncClient against the FastAPI test app with queue mocked."""
    with patch("vigilens.apps.api.stream.stream_job_queue", mock_queue):
        from vigilens.apps.api.app import app

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


class TestSubmitStream:
    def _valid_payload(self):
        return {
            "tenant_id": "t_123",
            "rtsp_url": "rtsp://cam1.local/stream",
            "trigger_queries": [
                {
                    "query": "person falling",
                    "threshold": 0.5,
                    "alert_payload_description": "Fall detection alert",
                }
            ],
            "webhook_urls": ["http://hook.example.com/alert"],
            "chunk_seconds": 10,
            "fps": 1,
        }

    async def test_submit_valid_stream_returns_200(self, client, mock_queue):
        resp = await client.post("/streams/submit", json=self._valid_payload())
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "queued"
        assert body["tenant_id"] == "t_123"
        assert body["rtsp_url"] == "rtsp://cam1.local/stream"
        assert body["stream_id"].startswith("s_")
        assert body["queue_message_id"] == "msg-001"

    async def test_submit_enqueues_to_redis(self, client, mock_queue):
        await client.post("/streams/submit", json=self._valid_payload())
        mock_queue.enqueue.assert_called_once()
        payload = mock_queue.enqueue.call_args[0][0]
        assert payload["tenant_id"] == "t_123"
        assert payload["status"] == "queued"
        assert payload["trigger_queries"] == ["person falling"]
        assert payload["thresholds"] == [0.5]

    async def test_submit_missing_rtsp_url_returns_422(self, client):
        payload = self._valid_payload()
        del payload["rtsp_url"]
        resp = await client.post("/streams/submit", json=payload)
        assert resp.status_code == 422

    async def test_submit_missing_trigger_queries_returns_422(self, client):
        payload = self._valid_payload()
        del payload["trigger_queries"]
        resp = await client.post("/streams/submit", json=payload)
        assert resp.status_code == 422

    async def test_submit_empty_trigger_queries(self, client, mock_queue):
        payload = self._valid_payload()
        payload["trigger_queries"] = []
        resp = await client.post("/streams/submit", json=payload)
        # Empty is valid at the pydantic level
        assert resp.status_code == 200

    async def test_submit_multiple_trigger_queries(self, client, mock_queue):
        payload = self._valid_payload()
        payload["trigger_queries"] = [
            {"query": "fall", "threshold": 0.3, "alert_payload_description": "Fall"},
            {"query": "fire", "threshold": 0.7, "alert_payload_description": "Fire"},
        ]
        resp = await client.post("/streams/submit", json=payload)
        assert resp.status_code == 200
        enqueued = mock_queue.enqueue.call_args[0][0]
        assert enqueued["trigger_queries"] == ["fall", "fire"]
        assert enqueued["thresholds"] == [0.3, 0.7]

    async def test_submit_with_custom_dimensions(self, client, mock_queue):
        payload = self._valid_payload()
        payload["video_target_width"] = 1280
        payload["video_target_height"] = 720
        resp = await client.post("/streams/submit", json=payload)
        assert resp.status_code == 200
        enqueued = mock_queue.enqueue.call_args[0][0]
        assert enqueued["video_target_width"] == 1280
        assert enqueued["video_target_height"] == 720

    async def test_submit_redis_failure_returns_503(self, client, mock_queue):
        mock_queue.enqueue.side_effect = ConnectionError("Redis down")
        resp = await client.post("/streams/submit", json=self._valid_payload())
        assert resp.status_code == 503
        assert "Failed to queue" in resp.json()["detail"]

    async def test_default_chunk_seconds(self, client, mock_queue):
        payload = self._valid_payload()
        del payload["chunk_seconds"]
        resp = await client.post("/streams/submit", json=payload)
        assert resp.status_code == 200
        enqueued = mock_queue.enqueue.call_args[0][0]
        assert enqueued["chunk_seconds"] == 10  # default

    async def test_default_fps(self, client, mock_queue):
        payload = self._valid_payload()
        del payload["fps"]
        resp = await client.post("/streams/submit", json=payload)
        assert resp.status_code == 200
        enqueued = mock_queue.enqueue.call_args[0][0]
        assert enqueued["fps"] == 1  # default


class TestHealthEndpoint:
    async def test_health_returns_ok(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

