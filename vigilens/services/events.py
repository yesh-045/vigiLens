import hashlib
import json
import uuid
from datetime import datetime, timezone

from vigilens.core.db import (
    compress_old_scene_timeline_async,
    query_recent_events_async,
    query_recent_scene_timeline_async,
    save_event_async,
    save_scene_timeline_async,
)
from vigilens.models.contracts.prompts import VideoAnalysisResultList
from vigilens.observability import trace


def _utc_now_sql() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _event_dedupe_key(
    *,
    stream_id: str,
    camera_id: str | None,
    clip_url: str,
    query: str,
    item_payload: dict,
) -> str:
    base = {
        "stream_id": stream_id,
        "camera_id": camera_id,
        "clip_url": clip_url,
        "query": query,
        "title": item_payload.get("title"),
        "video_path": item_payload.get("video_path"),
        "analysis": item_payload.get("analysis"),
        "key_identifiers": item_payload.get("key_identifiers", []),
        "key_frame_numbers": item_payload.get("key_frame_numbers", []),
    }
    canonical = json.dumps(base, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@trace(name="save_verified_events")
async def save_verified_events(
    *,
    parsed_result: VideoAnalysisResultList,
    stream_id: str,
    camera_id: str | None,
    clip_url: str,
    query: str,
) -> int:
    """Persist one event row per detection item from a structured Gemini result."""
    saved = 0
    if not parsed_result.is_action_detected:
        return saved

    for item in parsed_result.results:
        if not item.is_action_detected:
            continue
        item_payload = item.model_dump()
        inserted = await save_event_async(
            event_id=f"evt_{uuid.uuid4().hex[:12]}",
            timestamp=_utc_now_sql(),
            camera_id=camera_id,
            event_type=item.title or query,
            confidence=1.0,
            description=item.analysis,
            clip_url=clip_url,
            stream_id=stream_id,
            dedupe_key=_event_dedupe_key(
                stream_id=stream_id,
                camera_id=camera_id,
                clip_url=clip_url,
                query=query,
                item_payload=item_payload,
            ),
        )
        if inserted:
            saved += 1

    return saved


async def save_scene_summary(
    *,
    stream_id: str,
    camera_id: str | None,
    summary: str,
    clip_url: str,
) -> None:
    await save_scene_timeline_async(
        timeline_id=f"scn_{uuid.uuid4().hex[:12]}",
        timestamp=_utc_now_sql(),
        camera_id=camera_id,
        summary=summary,
        clip_url=clip_url,
        stream_id=stream_id,
        is_compacted=0,
    )


async def run_scene_retention_compression(retention_hours: int) -> int:
    return await compress_old_scene_timeline_async(retention_hours)


async def query_events(
    *, camera_id: str | None, within_hours: int, limit: int
) -> list[dict]:
    return await query_recent_events_async(
        camera_id=camera_id,
        within_hours=within_hours,
        limit=limit,
    )


async def query_scene_timeline(
    *, camera_id: str | None, within_hours: int, limit: int
) -> list[dict]:
    return await query_recent_scene_timeline_async(
        camera_id=camera_id,
        within_hours=within_hours,
        limit=limit,
    )
