from unittest.mock import AsyncMock, patch

import pytest

from vigilens.core import db as db_module
from vigilens.models.contracts.prompts import VideoAnalysisResultList
from vigilens.services.events import save_verified_events


def test_save_event_deduplicates_by_dedupe_key(tmp_path):
    db_module.DB_PATH = str(tmp_path / "events.db")
    db_module.init_db()

    inserted_1 = db_module.save_event(
        event_id="evt_1",
        timestamp="2026-01-01 00:00:01",
        camera_id="cam_1",
        event_type="fall",
        confidence=1.0,
        description="person falling",
        clip_url="s3://bucket/clip.mp4",
        stream_id="s_1",
        dedupe_key="same-key",
    )
    inserted_2 = db_module.save_event(
        event_id="evt_2",
        timestamp="2026-01-01 00:00:02",
        camera_id="cam_1",
        event_type="fall",
        confidence=1.0,
        description="person falling",
        clip_url="s3://bucket/clip.mp4",
        stream_id="s_1",
        dedupe_key="same-key",
    )

    rows = db_module.query_recent_events(camera_id="cam_1", within_hours=999999, limit=10)

    assert inserted_1 == 1
    assert inserted_2 == 0
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_save_verified_events_uses_deterministic_dedupe_keys():
    parsed = VideoAnalysisResultList.model_validate(
        {
            "results": [
                {
                    "video_path": "chunk_001",
                    "analysis": "person falling",
                    "title": "fall detected",
                    "key_identifiers": ["person"],
                    "key_frame_numbers": [10, 11],
                    "is_action_detected": True,
                    "is_continuation": False,
                },
                {
                    "video_path": "chunk_001",
                    "analysis": "person falling",
                    "title": "fall detected",
                    "key_identifiers": ["person"],
                    "key_frame_numbers": [10, 11],
                    "is_action_detected": True,
                    "is_continuation": False,
                },
            ],
            "is_action_detected": True,
        }
    )

    with patch("vigilens.services.events.save_event_async", new_callable=AsyncMock) as mock_save:
        mock_save.side_effect = [1, 0]
        saved_count = await save_verified_events(
            parsed_result=parsed,
            stream_id="s_1",
            camera_id="cam_1",
            clip_url="s3://bucket/clip.mp4",
            query="did someone fall",
        )

    assert saved_count == 1
    assert mock_save.await_count == 2
    first_kwargs = mock_save.await_args_list[0].kwargs
    second_kwargs = mock_save.await_args_list[1].kwargs
    assert first_kwargs["dedupe_key"] == second_kwargs["dedupe_key"]
