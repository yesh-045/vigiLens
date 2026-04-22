import uuid
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from vigilens.core.config import settings
from vigilens.integrations.redis_queue import AsyncRedisStreamQueue
from vigilens.models.contracts.messages import StreamJobMessage
from vigilens.core.db import create_stream_async, get_stream_async

import logging

logger = logging.getLogger(__name__)

stream_router = APIRouter()

stream_job_queue = AsyncRedisStreamQueue(
    redis_url=settings.redis_url,
    stream_name=settings.stream_job_stream,
    group_name=settings.stream_job_group,
    create_consumer_group=True,
    maxlen=settings.stream_job_maxlen,
)


class TriggerQuery(BaseModel):
    query: str
    threshold: Optional[float] = None
    alert_payload_description: Optional[str] = None


class StreamCreate(BaseModel):
    tenant_id: Optional[str] = None
    camera_id: Optional[str] = None
    name: Optional[str] = None
    rtsp_url: str
    trigger_queries: List[TriggerQuery]
    webhook_urls: Optional[List[str]] = None
    chunk_seconds: int = 10
    fps: int = 1
    video_target_width: Optional[int] = None
    video_target_height: Optional[int] = None


class StreamResponse(BaseModel):
    queue_message_id: Optional[str] = None
    stream_id: str
    tenant_id: Optional[str] = None
    camera_id: Optional[str] = None
    rtsp_url: str
    status: str


@stream_router.post("/streams/submit", tags=["streams"], response_model=StreamResponse)
async def submit_stream(stream: StreamCreate) -> StreamResponse:
    stream_id = f"s_{uuid.uuid4().hex[:12]}"

    stream_data = StreamJobMessage(
        stream_id=stream_id,
        tenant_id=stream.tenant_id,
        camera_id=stream.camera_id,
        name=stream.name or f"stream_{stream_id}",
        rtsp_url=stream.rtsp_url,
        trigger_queries=[query.query for query in stream.trigger_queries],
        thresholds=[
            query.threshold if query.threshold is not None else 0.5
            for query in stream.trigger_queries
        ],
        webhook_urls=stream.webhook_urls,
        chunk_seconds=stream.chunk_seconds,
        fps=stream.fps,
        video_target_width=stream.video_target_width,
        video_target_height=stream.video_target_height,
        status="queued",
    )

    stream_data_dict = stream_data.model_dump()
    logger.info(f"Stream data: {stream_data_dict}")

    try:
        await create_stream_async(
            stream_id,
            stream.rtsp_url,
            "queued",
            camera_id=stream.camera_id,
        )
        queue_message_id = await stream_job_queue.enqueue(stream_data_dict)
    except Exception as exc:
        raise HTTPException(
            status_code=503, detail=f"Failed to queue stream job: {exc}"
        ) from exc

    logger.info(f"Queue message ID: {queue_message_id}")
    return StreamResponse(
        queue_message_id=queue_message_id,
        stream_id=stream_id,
        tenant_id=stream.tenant_id,
        camera_id=stream.camera_id,
        rtsp_url=stream.rtsp_url,
        status="queued",
    )


@stream_router.get(
    "/streams/{stream_id}", tags=["streams"], response_model=StreamResponse
)
async def get_stream(stream_id: str) -> StreamResponse:
    stream = await get_stream_async(stream_id)
    if not stream:
        raise HTTPException(status_code=404, detail=f"Stream not found: {stream_id}")
    return StreamResponse(
        stream_id=stream["id"],
        rtsp_url=stream["url"],
        status=stream["status"],
        camera_id=stream.get("camera_id"),
    )
