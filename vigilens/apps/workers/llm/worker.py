import asyncio
import json
import hashlib
import uuid
from datetime import datetime, timezone
from typing import List

from vigilens.core.config import settings
from vigilens.integrations.redis_queue import AsyncRedisStreamQueue, StreamQueueMessage
from vigilens.integrations.llm_client import llm_analysis
from vigilens.models.contracts.messages import LLMJobMessage
from vigilens.models.contracts.prompts import (
    LLM_VIDEO_ANALYSIS_PROMPT as prompt,
    VideoAnalysisResultList,
)
from vigilens.apps.workers.llm.webhook import send_webhook_with_retry
import traceback
import logging
from collections import defaultdict
from vigilens.core.db import update_stream_status_async
from vigilens.core.db import save_event_async
from vigilens.services.events import save_verified_events

logger = logging.getLogger(__name__)

NUM_ACTIVE_STREAMS = settings.num_active_llm_streams_per_worker
INGESTION_TIMEOUT = settings.ingestion_timeout


class LLMAnalysisWorker:
    """Consumes stream jobs from Redis Streams and starts ffmpeg segmenters."""

    def __init__(self) -> None:
        redis_url = settings.redis_url
        llm_stream_name = settings.llm_job_stream
        llm_group_name = settings.llm_job_group
        consumer_name = f"{llm_group_name}-{uuid.uuid4().hex[:8]}"
        self.in_progress_tasks = set()
        self.stream_detection_history = defaultdict(list)

        self.queue = AsyncRedisStreamQueue(
            redis_url=redis_url,
            stream_name=llm_stream_name,
            group_name=llm_group_name,
            consumer_name=consumer_name,
            create_consumer_group=True,
            group_start_id="0-0",
            claim_stale_ms=60000,
        )

    async def run_forever(self) -> None:
        while True:
            print(f"In progress tasks: {self.in_progress_tasks}")
            if len(self.in_progress_tasks) >= NUM_ACTIVE_STREAMS:
                await asyncio.sleep(0.1)
                continue

            num_to_dequeue = min(NUM_ACTIVE_STREAMS - len(self.in_progress_tasks), 10)
            logger.info(f"Dequeuing {num_to_dequeue} messages from stream queue")
            messages = await self.queue.dequeue(count=num_to_dequeue, block_ms=5000)
            print(f"Dequeued messages: {messages}")
            if not messages:
                continue
            logger.info(f"Dequeued {len(messages)} messages from stream queue")
            for message in messages:
                if message.message_id in self.in_progress_tasks:
                    logger.debug(
                        "Skipping duplicate in-progress llm message %s",
                        message.message_id,
                    )
                    continue
                self.in_progress_tasks.add(message.message_id)
                asyncio.create_task(self._handle_message(message))

    async def send_webhook(
        self,
        webhook_urls: List[str],
        message: VideoAnalysisResultList,
        chunk_presigned_urls: List[str],
    ) -> None:
        tasks = [
            send_webhook_with_retry(url, message.model_dump(), chunk_presigned_urls)
            for url in webhook_urls
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        failures = 0
        for url, result in zip(webhook_urls, results):
            if isinstance(result, Exception):
                failures += 1
                logger.error("Webhook failed for %s: %s", url, result, exc_info=result)

        if failures:
            logger.error(
                "Webhook delivery failed for %d/%d URLs", failures, len(webhook_urls)
            )

    def parse_llm_analysis_result(self, raw: object) -> VideoAnalysisResultList:
        """Compatibility parser for legacy tests and fallback inputs."""
        if isinstance(raw, VideoAnalysisResultList):
            return raw
        if isinstance(raw, dict):
            if "results" in raw and "is_action_detected" in raw:
                return VideoAnalysisResultList.model_validate(raw)

            choices = raw.get("choices")
            if not choices:
                raise ValueError("Missing choices in LLM response")

            content = choices[0].get("message", {}).get("content")
            if not content:
                raise ValueError("Missing content in LLM response")

            if isinstance(content, str):
                return VideoAnalysisResultList.model_validate_json(content)
            if isinstance(content, dict):
                return VideoAnalysisResultList.model_validate(content)

            raise ValueError("Unsupported message content format")
        raise ValueError("Unsupported LLM response payload")

    async def _save_fallback_event(
        self,
        *,
        stream_id: str,
        camera_id: str | None,
        clip_url: str,
        query: str,
        reason: str,
    ) -> None:
        dedupe_base = f"fallback:{stream_id}:{camera_id or ''}:{clip_url}:{query}"
        dedupe_key = hashlib.sha256(dedupe_base.encode("utf-8")).hexdigest()
        description = (
            f"Screener-triggered alert for query '{query}'. "
            f"LLM verification unavailable: {reason[:180]}"
        )
        await save_event_async(
            event_id=f"evt_{uuid.uuid4().hex[:12]}",
            timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            camera_id=camera_id,
            event_type=f"Screener alert: {query}",
            confidence=0.35,
            description=description,
            clip_url=clip_url,
            stream_id=stream_id,
            dedupe_key=dedupe_key,
        )

    async def _handle_message(self, message: StreamQueueMessage) -> None:
        payload = LLMJobMessage.model_validate(message.payload)
        chunk_presigned_urls = payload.chunk_presigned_urls
        clip_url = payload.clip_url or (chunk_presigned_urls[0] if chunk_presigned_urls else "")
        trigger_queries = payload.trigger_queries
        webhook_urls = payload.webhook_urls
        stream_id = payload.stream_id
        camera_id = payload.camera_id
        try:
            for query in trigger_queries:
                try:
                    llm_raw = await llm_analysis(
                        prompt,
                        query,
                        VideoAnalysisResultList,
                        chunk_presigned_urls,
                        self.stream_detection_history.get(stream_id, []),
                    )
                    parsed_result = self.parse_llm_analysis_result(llm_raw)
                    self.stream_detection_history[stream_id].append(parsed_result)

                    if parsed_result.is_action_detected:
                        await save_verified_events(
                            parsed_result=parsed_result,
                            stream_id=stream_id,
                            camera_id=camera_id,
                            clip_url=clip_url,
                            query=query,
                        )

                        if webhook_urls:
                            asyncio.create_task(
                                self.send_webhook(
                                    webhook_urls, parsed_result, chunk_presigned_urls
                                )
                            )
                except Exception as exc:
                    logger.error(
                        "LLM analysis failed for stream=%s query=%s: %s\n%s",
                        stream_id,
                        query,
                        exc,
                        traceback.format_exc(),
                    )
                    await self._save_fallback_event(
                        stream_id=stream_id,
                        camera_id=camera_id,
                        clip_url=clip_url,
                        query=query,
                        reason=str(exc),
                    )
        except Exception:
            logger.error("Unexpected LLM worker error:\n" + traceback.format_exc())
            asyncio.create_task(update_stream_status_async(stream_id, "failed"))
        finally:
            await self.queue.ack(message.message_id)
            logger.info(f"Setting task done for message {message.message_id}")
            self.in_progress_tasks.discard(message.message_id)


if __name__ == "__main__":
    asyncio.run(LLMAnalysisWorker().run_forever())
