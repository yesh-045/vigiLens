import asyncio
import logging
import traceback
import uuid

from vigilens.core.config import settings
from vigilens.integrations.llm_client import summarize_scene_clip
from vigilens.integrations.redis_queue import AsyncRedisStreamQueue, StreamQueueMessage
from vigilens.models.contracts.messages import SceneJobMessage
from vigilens.services.events import run_scene_retention_compression, save_scene_summary

logger = logging.getLogger(__name__)


class SceneAnalysisWorker:
    def __init__(self) -> None:
        group_name = settings.scene_job_group
        consumer_name = f"{group_name}-{uuid.uuid4().hex[:8]}"
        self.queue = AsyncRedisStreamQueue(
            redis_url=settings.redis_url,
            stream_name=settings.scene_job_stream,
            group_name=group_name,
            consumer_name=consumer_name,
            create_consumer_group=True,
            group_start_id="0-0",
            claim_stale_ms=60000,
        )
        self.in_progress_tasks: set[str] = set()
        self._processed_since_compaction = 0

    async def run_forever(self) -> None:
        while True:
            if len(self.in_progress_tasks) >= settings.num_active_scene_streams_per_worker:
                await asyncio.sleep(0.1)
                continue

            num_to_dequeue = min(
                settings.num_active_scene_streams_per_worker - len(self.in_progress_tasks),
                10,
            )
            messages = await self.queue.dequeue(count=num_to_dequeue, block_ms=5000)
            if not messages:
                continue

            for message in messages:
                if message.message_id in self.in_progress_tasks:
                    logger.debug(
                        "Skipping duplicate in-progress scene message %s",
                        message.message_id,
                    )
                    continue
                self.in_progress_tasks.add(message.message_id)
                asyncio.create_task(self._handle_message(message))

    async def _handle_message(self, message: StreamQueueMessage) -> None:
        try:
            payload = SceneJobMessage.model_validate(message.payload)
            summary = await summarize_scene_clip(
                payload.clip_url,
                camera_id=payload.camera_id,
            )
            if summary:
                await save_scene_summary(
                    stream_id=payload.stream_id,
                    camera_id=payload.camera_id,
                    summary=summary,
                    clip_url=payload.clip_url,
                )

            self._processed_since_compaction += 1
            if self._processed_since_compaction >= 25:
                await run_scene_retention_compression(settings.scene_retention_hours)
                self._processed_since_compaction = 0

        except Exception:
            logger.error("Scene analysis error:\n" + traceback.format_exc())
        finally:
            await self.queue.ack(message.message_id)
            self.in_progress_tasks.discard(message.message_id)


if __name__ == "__main__":
    asyncio.run(SceneAnalysisWorker().run_forever())
