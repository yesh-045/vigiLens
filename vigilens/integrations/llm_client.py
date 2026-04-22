import base64
from typing import List, Optional, Type
import os
from vigilens.core.config import settings
import asyncio
import json
import aiohttp
import httpx
from pydantic import BaseModel
import tenacity
from google import genai
from google.genai import types
from google.genai.errors import APIError
from vigilens.models.contracts.prompts import VideoAnalysisResultList
from datetime import datetime
from pathlib import Path
import logging
from typing import Any

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}


def _base_url() -> str:
    return (settings.llm_base_url or "").strip().rstrip("/")


def _use_openai_compatible_mode() -> bool:
    base = _base_url()
    if not base:
        return False
    return "generativelanguage.googleapis.com" not in base


def _chat_completions_url() -> str:
    base = _base_url()
    if base.endswith("/v1"):
        return f"{base}/chat/completions"
    if base.endswith("/api"):
        return f"{base}/v1/chat/completions"
    return f"{base}/chat/completions"


async def _openai_compatible_chat_completion(
    *,
    messages: list[dict[str, Any]],
    response_format: dict[str, Any] | None = None,
    temperature: float | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model": settings.llm_model,
        "messages": messages,
    }
    if response_format is not None:
        payload["response_format"] = response_format
    if temperature is not None:
        payload["temperature"] = temperature

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=settings.llm_timeout)
    ) as session:
        async with session.post(
            _chat_completions_url(),
            headers={
                "Authorization": f"Bearer {settings.llm_api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
        ) as response:
            response.raise_for_status()
            return await response.json()


async def _generate_content_with_transport_fallback(
    client: genai.Client,
    *,
    model: str,
    contents: list[types.Content],
    config: types.GenerateContentConfig,
):
    """Use async SDK path first; fall back to sync SDK on transport-layer errors.

    In some environments the async httpx transport intermittently fails to connect,
    while sync requests succeed. This fallback keeps behavior resilient and surfaces
    upstream API errors (for example quota/auth) more reliably.
    """
    try:
        return await client.aio.models.generate_content(
            model=model,
            contents=contents,
            config=config,
        )
    except (httpx.ConnectError, httpx.RemoteProtocolError) as exc:
        logger.warning(
            "Gemini async transport error (%s); retrying with sync transport",
            exc,
        )
        return await asyncio.to_thread(
            client.models.generate_content,
            model=model,
            contents=contents,
            config=config,
        )


def is_retryable_exception(exc: BaseException) -> bool:
    if isinstance(exc, (aiohttp.ClientConnectionError, asyncio.TimeoutError)):
        return True
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status in RETRYABLE_STATUS_CODES
    if isinstance(exc, APIError):
        return True
    return False


def video_to_data_url(path: str, mime="video/mp4") -> str:
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    return f"data:{mime};base64,{b64}"


def construct_payload(chunk_paths) -> list:
    """Legacy payload builder used by tests and OpenAI-compatible HTTP mode."""
    payload = []
    for chunk_path in chunk_paths:
        payload.append(
            {
                "type": "text",
                "text": "Chunk ID: " + os.path.basename(chunk_path).split(".")[0],
            }
        )
        if not os.path.exists(chunk_path) or chunk_path.startswith("http"):
            url = chunk_path
            payload.append({"type": "video_url", "video_url": {"url": url}})
        else:
            payload.append(
                {"type": "video_url", "video_url": {"url": video_to_data_url(chunk_path)}}
            )
    return payload


def _construct_gemini_parts(chunk_paths: List[str]) -> list[types.Part]:
    payload: list[types.Part] = []
    for chunk_path in chunk_paths:
        payload.append(
            types.Part.from_text(
                text="Chunk ID: " + os.path.basename(chunk_path).split(".")[0]
            )
        )
        if not os.path.exists(chunk_path) or chunk_path.startswith("http"):
            payload.append(types.Part.from_uri(file_uri=chunk_path, mime_type="video/mp4"))
        else:
            with open(chunk_path, "rb") as f:
                payload.append(types.Part.from_bytes(data=f.read(), mime_type="video/mp4"))
    return payload


def parse_chunk_timestamp(filepath: str) -> datetime:
    try:
        prefix, time_pattern = settings.chunk_output_pattern.split("%", 1)
        time_pattern = "%" + time_pattern.strip()
        time_pattern = time_pattern.split(".")[0]
        stem = Path(filepath).stem
        ts_part = stem.removeprefix(prefix)
        return datetime.strptime(ts_part, time_pattern)
    except ValueError as e:
        logger.error(f"Error parsing chunk timestamp: {e}")
        stem = Path(filepath).stem
        ts_part = stem.removeprefix(prefix)
        return ts_part


def construct_history_context(
    history: List[VideoAnalysisResultList], char_budget: int = 100000
) -> str:
    context = ""
    for result in history:
        is_action_detected = result.is_action_detected
        video_results = result.results
        if is_action_detected:
            for video_result in video_results:
                video_path = video_result.video_path
                timestamp = parse_chunk_timestamp(video_path)
                analysis = video_result.analysis
                _is_action_detected = video_result.is_action_detected
                context += "--------------------------------\n"
                context += f"Timestamp: {timestamp}\nAnalysis: {analysis}\nIs Action Detected: {_is_action_detected}\n"
    context = context[-char_budget:]
    return context


@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
    retry=tenacity.retry_if_exception(is_retryable_exception),
)
async def llm_analysis(
    prompt: str,
    query: str,
    analysis_model: Type[BaseModel],
    chunk_paths: List[str],
    history: List[VideoAnalysisResultList] = None,
) -> BaseModel:
    # OpenAI-compatible mode (e.g., llm-test or OpenRouter).
    if _use_openai_compatible_mode():
        payload = construct_payload(chunk_paths)
        analysis_schema = analysis_model.model_json_schema()
        history_context = ""
        if history:
            history_context = construct_history_context(history)

        return await _openai_compatible_chat_completion(
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"History Context:\n\n{history_context}"},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": f"query: {query}"},
                        *payload,
                    ],
                },
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "video_analysis_result",
                    "strict": True,
                    "schema": analysis_schema,
                },
            },
        )

    payload = _construct_gemini_parts(chunk_paths)
    history_context = ""
    
    if history:
        history_context = construct_history_context(history)
        
    client = genai.Client(api_key=settings.llm_api_key)
    
    contents = []
    if history_context:
        contents.append(types.Content(
            role="user",
            parts=[types.Part.from_text(text=f"History Context:\n\n{history_context}")]
        ))
        
    user_parts = [types.Part.from_text(text=f"query: {query}")] + payload
    contents.append(types.Content(role="user", parts=user_parts))
    
    config = types.GenerateContentConfig(
        system_instruction=prompt,
        response_mime_type="application/json",
        response_schema=analysis_model,
        temperature=0.0
    )
    
    response = await _generate_content_with_transport_fallback(
        client,
        model=settings.llm_model,
        contents=contents,
        config=config,
    )
    return response.parsed


@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=8),
    retry=tenacity.retry_if_exception(is_retryable_exception),
)
async def summarize_scene_clip(
    clip_url: str,
    camera_id: Optional[str] = None,
) -> str:
    if _use_openai_compatible_mode():
        raw = await _openai_compatible_chat_completion(
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                "Summarize this surveillance clip in one concise sentence. "
                                "Focus on activity and notable entities."
                                + (f" Camera ID: {camera_id}." if camera_id else "")
                            ),
                        },
                        {"type": "video_url", "video_url": {"url": clip_url}},
                    ],
                }
            ],
            temperature=0.1,
        )
        return (
            raw.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )

    client = genai.Client(api_key=settings.llm_api_key)
    prompt = (
        "Summarize this surveillance clip in one concise sentence. "
        "Focus on activity and notable entities."
    )
    if camera_id:
        prompt += f" Camera ID: {camera_id}."

    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text=prompt),
                types.Part.from_uri(file_uri=clip_url, mime_type="video/mp4"),
            ],
        )
    ]
    response = await _generate_content_with_transport_fallback(
        client,
        model=settings.llm_model,
        contents=contents,
        config=types.GenerateContentConfig(temperature=0.1),
    )
    return (response.text or "").strip()

if __name__ == "__main__":
    import asyncio
    from vigilens.models.contracts.prompts import (
        LLM_VIDEO_ANALYSIS_PROMPT,
        VideoAnalysisResultList,
    )
    from vigilens.integrations.storage import get_s3_client
    import time

    s3_client = get_s3_client()
    upload_result = s3_client.upload_chunks_to_s3(
        [
            "/Users/yeshw/Downloads/s_fc882e325a15_chunks_chunk_20260215_010047.mp4",
        ],
        "s_d30e03a329d0",
        generate_presigned_url=True,
    )
    print(f"Upload result: {upload_result}")
    presigned_urls = [chunk["presigned_url"] for chunk in upload_result]
    start_time = time.time()
    result = asyncio.run(
        llm_analysis(
            LLM_VIDEO_ANALYSIS_PROMPT,
            "Verify if the batsman has hit a boundary",
            VideoAnalysisResultList,
            presigned_urls,
        )
    )
    end_time = time.time()
    print(f"LLM analysis time: {end_time - start_time} seconds")
    print(f"LLM analysis result: {result}")
