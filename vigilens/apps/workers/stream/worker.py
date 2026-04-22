import os
import time
import asyncio
from typing import List
from typing import Optional
from vigilens.core.config import settings
from vigilens.integrations.redis_queue import AsyncRedisStreamQueue, StreamQueueMessage
from vigilens.integrations.storage import s3_client
from vigilens.apps.workers.stream.stream import start_rtsp_segmenter, wait_until_video_ready
from vigilens.services.screening import screen_chunk
from vigilens.models.contracts.messages import (
    LLMJobMessage,
    SceneJobMessage,
    StreamJobMessage,
)
from vigilens.services.clip_builder import (
    build_clip,
    sample_frames_from_video,
    upload_clip_to_minio,
)
from vigilens.core.db import update_stream_status_async
from functools import partial

import traceback
import logging
import uuid

logger = logging.getLogger(__name__)

NUM_ACTIVE_STREAMS = settings.num_active_streams_per_worker
INGESTION_TIMEOUT = settings.ingestion_timeout
SEGMENTER_STARTUP_GRACE_SECONDS = 2.0


class StreamProcessWorker:
    """Consumes stream jobs from Redis Streams and starts ffmpeg segmenters."""

    def __init__(self) -> None:
        redis_url = settings.redis_url
        stream_name = settings.stream_job_stream
        llm_stream_name = settings.llm_job_stream
        scene_stream_name = settings.scene_job_stream
        group_name = settings.stream_job_group
        llm_group_name = settings.llm_job_group
        scene_group_name = settings.scene_job_group
        consumer_name = f"{group_name}-{uuid.uuid4().hex[:8]}"
        self.in_progress_tasks = set()

        self.stream_tmp_dir = settings.stream_tmp_dir
        self.queue = AsyncRedisStreamQueue(
            redis_url=redis_url,
            stream_name=stream_name,
            group_name=group_name,
            consumer_name=consumer_name,
            create_consumer_group=True,
            group_start_id="0-0",
            claim_stale_ms=None,
        )
        self.scene_queue = AsyncRedisStreamQueue(
            redis_url=redis_url,
            stream_name=scene_stream_name,
            group_name=scene_group_name,
            create_consumer_group=True,
            group_start_id="0-0",
            maxlen=settings.scene_job_maxlen,
            claim_stale_ms=60000,
        )
        self.llm_queue = AsyncRedisStreamQueue(
            redis_url=redis_url,
            stream_name=llm_stream_name,
            group_name=llm_group_name,
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
            print(f"Dequeued {len(messages)} messages from stream queue")
            if not messages:
                continue
            logger.info(f"Dequeued {len(messages)} messages from stream queue")
            for message in messages:
                try:
                    if message.message_id in self.in_progress_tasks:
                        logger.debug(
                            "Skipping duplicate in-progress stream message %s",
                            message.message_id,
                        )
                        continue
                    self.in_progress_tasks.add(message.message_id)
                    asyncio.create_task(self._handle_message(message))
                except Exception:
                    logger.error("Error handling message:\n" + traceback.format_exc())
                    pass

            await asyncio.sleep(0.1)

    def _delete_if_exists(self, path: str) -> None:
        # Only clean up worker staging files to avoid deleting arbitrary temp test files.
        if not os.path.abspath(path).startswith(os.path.abspath(self.stream_tmp_dir)):
            return
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    # TODO: upload neighboring chunks to s3
    async def handle_selected_chunks(
        self,
        message_id: str,
        stream_id: str,
        chunk_path: str,
        context_chunks: List[str],
        selected_queries: List[str],
        selected_thresholds: List[float],
        webhook_urls: Optional[List[str]] = None,
        camera_id: Optional[str] = None,
    ) -> None:

        try:
            print(f"Handling selected chunks for message {message_id}")
            candidate_paths = [*context_chunks, chunk_path]
            existing_paths: List[str] = []
            seen: set[str] = set()
            for path in candidate_paths:
                if path in seen:
                    continue
                seen.add(path)
                if os.path.exists(path):
                    existing_paths.append(path)

            if not existing_paths:
                logger.warning(
                    "No existing chunks left to upload for message %s (stream=%s)",
                    message_id,
                    stream_id,
                )
                return

            upload_result = await asyncio.to_thread(
                s3_client.upload_chunks_to_s3,
                existing_paths,
                stream_id,
                generate_presigned_url=True,
            )
            # enqueue the message for llm analysis
            message = LLMJobMessage(
                stream_id=stream_id,
                camera_id=camera_id,
                chunk_paths=[chunk["url"] for chunk in upload_result],
                chunk_presigned_urls=[
                    chunk["presigned_url"] for chunk in upload_result
                ],
                clip_url=upload_result[-1]["presigned_url"],
                trigger_queries=selected_queries,
                thresholds=selected_thresholds,
                webhook_urls=webhook_urls,
            )
            message_dict = message.model_dump()

            await self.llm_queue.enqueue(message_dict)
            print(f"Enqueued message for llm analysis: {message_dict}")
            print(f"Deleting {len(existing_paths)} chunks: {existing_paths}")
            await asyncio.gather(
                *(asyncio.to_thread(self._delete_if_exists, p) for p in existing_paths)
            )

        except Exception:
            logger.error("Error handling selected chunks:\n" + traceback.format_exc())

    async def enqueue_scene_job(
        self,
        *,
        stream_id: str,
        camera_id: Optional[str],
        chunk_path: str,
        trigger_queries: List[str],
        thresholds: List[float],
        screening_scores: List[float],
        webhook_urls: Optional[List[str]] = None,
    ) -> None:
        """Build a short clip from in-memory sampled frames and enqueue a scene job."""
        clip_path: str | None = None
        try:
            frames = await asyncio.to_thread(
                sample_frames_from_video,
                chunk_path,
                settings.sample_frame_every_n_seconds,
                15,
            )
            if not frames:
                return

            clip_path = await asyncio.to_thread(
                build_clip,
                frames,
                15,
                settings.clip_builder_duration_seconds,
            )
            if not clip_path:
                return

            clip_url = await asyncio.to_thread(upload_clip_to_minio, clip_path, stream_id)
            scene_job = SceneJobMessage(
                stream_id=stream_id,
                camera_id=camera_id,
                clip_url=clip_url,
                trigger_queries=trigger_queries,
                thresholds=thresholds,
                screening_scores=screening_scores,
                webhook_urls=webhook_urls,
            )
            await self.scene_queue.enqueue(scene_job.model_dump())
        except Exception:
            logger.error("Error enqueueing scene job:\n" + traceback.format_exc())
        finally:
            if clip_path:
                await asyncio.to_thread(self._delete_if_exists, clip_path)

    def _on_done_screening_loop_cb(self, task: asyncio.Task, message_id: str, stream_id: str):
        try:
            exc = (
                task.exception()
            )  # reads exception if any (prevents "never retrieved")
        except Exception as e:
            logger.error(
                f"[StreamProcessWorker] task {task.get_name()}: error retrieving exception: {e}"
            )
            exc = e

        if exc:
            logger.exception(
                f"[StreamProcessWorker] task {task.get_name()} crashed", exc_info=exc
            )
            asyncio.create_task(update_stream_status_async(stream_id, "failed"))
            asyncio.create_task(self.queue.ack(message_id))
            self.in_progress_tasks.discard(message_id)

        else:
            # if you care about return value:
            try:
                result = task.result()
                logger.info(
                    f"[StreamProcessWorker] task {task.get_name()} finished: {result}"
                )
                asyncio.create_task(update_stream_status_async(stream_id, "completed"))
                asyncio.create_task(self.queue.ack(message_id))
            except Exception:
                logger.exception(
                    f"[StreamProcessWorker] task {task.get_name()}: error retrieving result"
                )
                asyncio.create_task(update_stream_status_async(stream_id, "failed"))
                asyncio.create_task(self.queue.ack(message_id))
            finally:
                self.in_progress_tasks.discard(message_id)

    async def _action_screening_loop(
        self,
        message_id: str,
        stream_id: str,
        out_dir: str,
        trigger_queries: List[str],
        thresholds: List[float],
        webhook_urls: Optional[List[str]] = None,
        camera_id: Optional[str] = None,
    ) -> None:

        last_heartbeat = time.time()
        previous_chunks_lookback = settings.llm_context_chunk_lookback or 0
        seen_chunks: List[str] = []
        selected_chunks: List[str] = []

        while True:
            now = time.time()
            if now - last_heartbeat > INGESTION_TIMEOUT:
                logger.info(
                    f"[StreamProcessWorker._action_screening_loop] Ingestion stopped for stream {stream_id} due to inactivity"
                )
                self.in_progress_tasks.discard(message_id)
                break

            files = sorted(os.listdir(out_dir))

            if not files:
                await asyncio.sleep(0.1)
                continue

            for name in files:
                if not name.endswith(".mp4"):
                    await asyncio.sleep(0.1)
                    continue

                path = os.path.join(out_dir, name)
                if path in seen_chunks:
                    continue

                try:
                    # ensure ffmpeg is done writing the file (avoid partial MP4 reads)
                    try:
                        wait_until_video_ready(path)
                    except TimeoutError:
                        # try again on the next poll cycle
                        continue

                    last_heartbeat = now
                    print(f"Screening chunk {path} with trigger queries")
                    results = await screen_chunk(path, trigger_queries)
                    seen_chunks.append(path)
                    screening_scores = [
                        float(item.get("score", 0.0))
                        for item in results.get("data", [])
                    ]

                    asyncio.create_task(
                        self.enqueue_scene_job(
                            stream_id=stream_id,
                            camera_id=camera_id,
                            chunk_path=path,
                            trigger_queries=trigger_queries,
                            thresholds=thresholds,
                            screening_scores=screening_scores,
                            webhook_urls=webhook_urls,
                        )
                    )

                    selected_queries = []
                    selected_thresholds = []
                    context_chunks = []
                    for i, result in enumerate(results.get("data", [])):
                        print(f"Result {i}: {result.get('score')} > {thresholds[i]}")
                        if result.get("score") > thresholds[i]:
                            selected_queries.append(trigger_queries[i])
                            selected_thresholds.append(thresholds[i])
                            if previous_chunks_lookback > 0:
                                context_chunks.extend(
                                    seen_chunks[-(previous_chunks_lookback + 1) : -1]
                                )
                            print(f"LLM Context chunks: {[path] + context_chunks}")
                            selected_chunks.extend([path] + context_chunks)

                    if selected_queries:
                        print(
                            f"Selected chunks for llm analysis: {[path] + context_chunks}"
                        )
                        asyncio.create_task(
                            self.handle_selected_chunks(
                                message_id,
                                stream_id,
                                path,
                                context_chunks,
                                selected_queries,
                                selected_thresholds,
                                webhook_urls,
                                camera_id,
                            )
                        )
                        logger.info(
                            f"Chunk {path} is relevant to queries {selected_queries}, score: {selected_thresholds}. Enqueued message with ID: {message_id}"
                        )

                    beyond_context_chunks = seen_chunks[
                        : -(previous_chunks_lookback + 1)
                    ]  # remaining chunks before the current chunk + context chunks
                    beyond_context_unselected_chunks = set(
                        beyond_context_chunks
                    ).difference(
                        selected_chunks
                    )  # chunks that are not selected thus far and not in the context
                    await asyncio.gather(
                        *(
                            asyncio.to_thread(self._delete_if_exists, p)
                            for p in beyond_context_unselected_chunks
                        )
                    )
                    for chunk in beyond_context_unselected_chunks:
                        seen_chunks.remove(chunk)

                except Exception:
                    logger.error("Ingest loop error:\n" + traceback.format_exc())

    async def _handle_message(self, message: StreamQueueMessage) -> None:

        message_id = message.message_id

        payload = StreamJobMessage.model_validate(message.payload)
        stream_id = payload.stream_id
        camera_id = payload.camera_id
        out_dir = os.path.join(self.stream_tmp_dir, stream_id)
        os.makedirs(out_dir, exist_ok=True)
        trigger_queries = payload.trigger_queries
        thresholds = payload.thresholds
        webhook_urls = payload.webhook_urls
        video_target_width = payload.video_target_width
        video_target_height = payload.video_target_height
        target_fps = max(1, int(payload.fps or 1))
        num_retries = 0
        MAX_RETRIES = settings.stream_segmenter_max_retries
        proc = None

        try:
            while num_retries < MAX_RETRIES:
                try:
                    proc = start_rtsp_segmenter(
                        rtsp_url=payload.rtsp_url,
                        out_dir=out_dir,
                        chunk_seconds=int(payload.chunk_seconds),
                        fps=target_fps,
                        target_width=video_target_width
                        or settings.stream_chunk_target_width,
                        target_height=video_target_height
                        or settings.stream_chunk_target_height,
                    )
                    logger.info(
                        f"[StreamProcessWorker] Started segmenter for stream {stream_id} on process {proc.pid}"
                    )

                    # ffmpeg can return a process immediately and then exit right away
                    # (e.g., RTSP DESCRIBE 404). Treat quick exits as startup failures.
                    await asyncio.sleep(SEGMENTER_STARTUP_GRACE_SECONDS)
                    return_code = proc.poll()
                    if return_code is not None:
                        num_retries += 1
                        logger.warning(
                            f"[StreamProcessWorker] Segmenter exited during startup for stream {stream_id} "
                            f"(attempt {num_retries}/{MAX_RETRIES}, rc={return_code})"
                        )
                        proc = None
                        await asyncio.sleep(1)
                        continue
                    break
                except Exception as e:
                    num_retries += 1
                    await asyncio.sleep(1)
                    logger.debug(
                        f"[StreamProcessWorker] Error starting segmenter for stream {stream_id} after {num_retries} retries: {e}"
                    )
                    continue
            if num_retries >= MAX_RETRIES:
                asyncio.create_task(update_stream_status_async(stream_id, "failed"))
                logger.error(
                    f"[StreamProcessWorker] Error starting segmenter for stream {stream_id} after {MAX_RETRIES} retries"
                )
                try:
                    await self.queue.ack(message_id)
                except Exception:
                    logger.error(
                        "[StreamProcessWorker] Failed to ack message %s after max retries:\n%s",
                        message_id,
                        traceback.format_exc(),
                    )
                self.in_progress_tasks.discard(message_id)
                if proc:
                    proc.terminate()
                    proc.wait()
                    proc = None
                return None

            if proc:
                asyncio.create_task(update_stream_status_async(stream_id, "processing"))
                screening_task = asyncio.create_task(
                    self._action_screening_loop(
                        message_id,
                        stream_id,
                        out_dir,
                        trigger_queries,
                        thresholds,
                        webhook_urls,
                        camera_id,
                    )
                )
                screening_task.add_done_callback(
                    partial(self._on_done_screening_loop_cb, message_id=message_id, stream_id=stream_id)
                )
        except Exception as e:
            logger.error(
                f"[StreamProcessWorker] Error handling message {message_id}: {e}"
            )
            try:
                await self.queue.ack(message_id)
            except Exception:
                logger.error(
                    "[StreamProcessWorker] Failed to ack errored message %s:\n%s",
                    message_id,
                    traceback.format_exc(),
                )
            self.in_progress_tasks.discard(message_id)
            if proc:
                proc.terminate()
                proc.wait()
            return None
        finally:
            logger.debug(f"[StreamProcessWorker] Acknowledging message {message_id}")
        return proc


if __name__ == "__main__":
    asyncio.run(StreamProcessWorker().run_forever())
