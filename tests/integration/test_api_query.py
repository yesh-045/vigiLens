from unittest.mock import AsyncMock, patch

import httpx
import pytest
import pytest_asyncio

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture()
async def client():
    from vigilens.apps.api.app import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestQueryEndpoint:
    async def test_event_query_routes_and_maps_response(self, client):
        with (
            patch(
                "vigilens.apps.api.query.query_events", new_callable=AsyncMock
            ) as mock_events,
            patch(
                "vigilens.apps.api.query.query_scene_timeline", new_callable=AsyncMock
            ) as mock_scene,
        ):
            mock_events.return_value = [
                {
                    "timestamp": "2026-04-15 12:00:00",
                    "camera_id": "cam_1",
                    "description": "person fell near stairs",
                    "clip_url": "s3://bucket/clip.mp4",
                    "confidence": 0.93,
                }
            ]

            resp = await client.post(
                "/query",
                json={"query": "did someone fall", "camera_id": "cam_1"},
            )

            assert resp.status_code == 200
            body = resp.json()
            assert body["route"] == "event"
            assert len(body["results"]) == 1
            assert body["results"][0]["source"] == "event"
            assert body["results"][0]["summary"] == "person fell near stairs"
            mock_events.assert_called_once()
            mock_scene.assert_not_called()

    async def test_activity_query_routes_and_maps_response(self, client):
        with (
            patch(
                "vigilens.apps.api.query.query_events", new_callable=AsyncMock
            ) as mock_events,
            patch(
                "vigilens.apps.api.query.query_scene_timeline", new_callable=AsyncMock
            ) as mock_scene,
        ):
            mock_scene.return_value = [
                {
                    "timestamp": "2026-04-15 12:02:00",
                    "camera_id": "cam_2",
                    "summary": "person walking near sofa",
                    "clip_url": "s3://bucket/scene.mp4",
                }
            ]

            resp = await client.post(
                "/query",
                json={"query": "what activity happened", "camera_id": "cam_2"},
            )

            assert resp.status_code == 200
            body = resp.json()
            assert body["route"] == "activity"
            assert len(body["results"]) == 1
            assert body["results"][0]["source"] == "activity"
            assert body["results"][0]["summary"] == "person walking near sofa"
            mock_scene.assert_called_once()
            mock_events.assert_not_called()
