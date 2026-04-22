from typing import Any, Literal

from fastapi import APIRouter
from pydantic import BaseModel

from vigilens.core.config import settings
from vigilens.services.events import query_events, query_scene_timeline
from vigilens.services.query_router import route_query


query_router = APIRouter()


class QueryRequest(BaseModel):
    query: str
    camera_id: str | None = None


class QueryItem(BaseModel):
    source: Literal["event", "activity"]
    timestamp: str
    camera_id: str | None = None
    summary: str
    clip_url: str
    confidence: float | None = None


class QueryResponse(BaseModel):
    route: Literal["event", "activity"]
    results: list[QueryItem]


@query_router.post("/query", tags=["query"], response_model=QueryResponse)
async def run_query(payload: QueryRequest) -> QueryResponse:
    route = route_query(payload.query)

    if route == "event":
        rows = await query_events(
            camera_id=payload.camera_id,
            within_hours=settings.query_lookback_hours,
            limit=settings.query_event_limit,
        )
        items = [
            QueryItem(
                source="event",
                timestamp=str(row["timestamp"]),
                camera_id=row.get("camera_id"),
                summary=row.get("description") or "",
                clip_url=row.get("clip_url") or "",
                confidence=row.get("confidence"),
            )
            for row in rows
        ]
        return QueryResponse(route="event", results=items)

    rows = await query_scene_timeline(
        camera_id=payload.camera_id,
        within_hours=settings.query_lookback_hours,
        limit=settings.query_activity_limit,
    )
    items = [
        QueryItem(
            source="activity",
            timestamp=str(row["timestamp"]),
            camera_id=row.get("camera_id"),
            summary=row.get("summary") or "",
            clip_url=row.get("clip_url") or "",
        )
        for row in rows
    ]
    return QueryResponse(route="activity", results=items)
