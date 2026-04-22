from pydantic import BaseModel
from typing import List, Optional


class LLMJobMessage(BaseModel):
    stream_id: str
    camera_id: Optional[str] = None
    chunk_paths: List[str]
    chunk_presigned_urls: List[str]
    clip_url: Optional[str] = None
    trigger_queries: List[str]
    thresholds: List[float]
    webhook_urls: Optional[List[str]] = None


class StreamJobMessage(BaseModel):
    stream_id: str
    tenant_id: Optional[str] = None
    camera_id: Optional[str] = None
    rtsp_url: str
    name: str
    chunk_seconds: int
    trigger_queries: List[str]
    thresholds: List[float]
    webhook_urls: Optional[List[str]] = None
    fps: int
    video_target_width: Optional[int] = None
    video_target_height: Optional[int] = None
    status: str


class SceneJobMessage(BaseModel):
    stream_id: str
    camera_id: Optional[str] = None
    clip_url: str
    trigger_queries: List[str]
    thresholds: List[float]
    screening_scores: List[float]
    webhook_urls: Optional[List[str]] = None
