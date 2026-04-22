"""Integration tests for StreamProcessWorker."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from vigilens.integrations.redis_queue import StreamQueueMessage


def _make_queue_message(
    payload_dict: dict, message_id: str = "msg-001"
) -> StreamQueueMessage:
    return StreamQueueMessage(
        stream="stream.jobs",
        message_id=message_id,
        payload=payload_dict,
    )


@pytest.fixture()
def worker():
    """Create a StreamProcessWorker with mocked queues and dependencies."""
    with (
        patch("vigilens.apps.workers.stream.worker.AsyncRedisStreamQueue") as MockQueue,
        patch("vigilens.apps.workers.stream.worker.s3_client") as mock_s3,
    ):
        mock_q = MagicMock()
        MockQueue.return_value = mock_q
        mock_s3.upload_chunks_to_s3.return_value = [
            {
                "url": "s3://bucket/chunk.mp4",
                "presigned_url": "http://presigned.url/chunk.mp4",
            }
        ]

        from vigilens.apps.workers.stream.worker import StreamProcessWorker

        w = StreamProcessWorker()
        w.queue = AsyncMock()
        w.llm_queue = AsyncMock()
        w.llm_queue.enqueue = AsyncMock(return_value="llm-msg-001")
        w._mock_s3 = mock_s3
        return w


class TestHandleSelectedChunks:
    async def test_uploads_and_enqueues(self, worker, make_stub_mp4):
        path = make_stub_mp4("chunk_test.mp4")
        worker.in_progress_tasks.add("msg-001")

        with patch("vigilens.apps.workers.stream.worker.s3_client") as mock_s3:
            mock_s3.upload_chunks_to_s3.return_value = [
                {
                    "url": "s3://bucket/chunk_test.mp4",
                    "presigned_url": "http://presigned/chunk_test.mp4",
                }
            ]
            await worker.handle_selected_chunks(
                "msg-001",
                "s_abc",
                path,
                [],
                ["person falling"],
                [0.5],
                ["http://hook.example.com"],
            )

        # LLM queue should have been called
        worker.llm_queue.enqueue.assert_called_once()
        enqueued = worker.llm_queue.enqueue.call_args[0][0]
        assert enqueued["stream_id"] == "s_abc"
        assert enqueued["trigger_queries"] == ["person falling"]
        assert enqueued["chunk_paths"] == ["s3://bucket/chunk_test.mp4"]

    async def test_preserves_file_after_upload(self, worker, make_stub_mp4):
        path = make_stub_mp4("to_delete.mp4")
        assert os.path.exists(path)

        with patch("vigilens.apps.workers.stream.worker.s3_client") as mock_s3:
            mock_s3.upload_chunks_to_s3.return_value = [
                {
                    "url": "s3://bucket/to_delete.mp4",
                    "presigned_url": "http://presigned/to_delete.mp4",
                }
            ]
            await worker.handle_selected_chunks(
                "msg-001",
                "s_abc",
                path,
                [],
                ["q"],
                [0.5],
            )

        # Current behavior: worker does not delete chunks after upload.
        assert os.path.exists(path)


class TestHandleMessage:
    async def test_starts_segmenter_and_creates_screening_task(
        self, worker, stream_job_factory
    ):
        payload = stream_job_factory()
        msg = _make_queue_message(payload)
        worker.in_progress_tasks.add("msg-001")

        mock_proc = MagicMock(pid=999)
        mock_proc.poll.return_value = None

        with (
            patch(
                "vigilens.apps.workers.stream.worker.start_rtsp_segmenter",
                return_value=mock_proc,
            ),
            patch("vigilens.apps.workers.stream.worker.SEGMENTER_STARTUP_GRACE_SECONDS", 0),
            patch("vigilens.apps.workers.stream.worker.asyncio.sleep", new=AsyncMock()),
            patch.object(
                worker,
                "_action_screening_loop",
                new_callable=AsyncMock,
            ),
        ):
            result = await worker._handle_message(msg)

        assert result is mock_proc

    async def test_retries_segmenter_on_failure(self, worker, stream_job_factory):
        """When segmenter fails on first attempts but succeeds later."""
        payload = stream_job_factory()
        msg = _make_queue_message(payload)
        worker.in_progress_tasks.add("msg-001")

        call_count = 0
        mock_proc = MagicMock(pid=999)
        mock_proc.poll.return_value = None

        def failing_segmenter(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("ffmpeg failed")
            return mock_proc

        with (
            patch(
                "vigilens.apps.workers.stream.worker.start_rtsp_segmenter",
                side_effect=failing_segmenter,
            ),
            patch("vigilens.apps.workers.stream.worker.SEGMENTER_STARTUP_GRACE_SECONDS", 0),
            patch("vigilens.apps.workers.stream.worker.asyncio.sleep", new=AsyncMock()),
            patch.object(
                worker,
                "_action_screening_loop",
                new_callable=AsyncMock,
            ),
        ):
            result = await worker._handle_message(msg)

        assert result is mock_proc
        assert call_count == 3

    async def test_gives_up_after_max_retries(self, worker, stream_job_factory):
        """When all retry attempts fail, message is cleaned up and returns None."""
        payload = stream_job_factory()
        msg = _make_queue_message(payload)
        worker.in_progress_tasks.add("msg-001")

        with (
            patch(
                "vigilens.apps.workers.stream.worker.start_rtsp_segmenter",
                side_effect=RuntimeError("always fails"),
            ),
            patch("vigilens.apps.workers.stream.worker.asyncio.sleep", new=AsyncMock()),
            patch("vigilens.apps.workers.stream.worker.settings.stream_segmenter_max_retries", 1),
        ):
            result = await worker._handle_message(msg)

        assert result is None
        assert "msg-001" not in worker.in_progress_tasks

    async def test_wrong_rtsp_removes_message_from_in_progress(
        self, worker, stream_job_factory
    ):
        payload = stream_job_factory(
            overrides={"rtsp_url": "rtsp://localhost:8554/missing"}
        )
        msg = _make_queue_message(payload, message_id="msg-bad")
        worker.in_progress_tasks.add("msg-bad")

        dead_proc = MagicMock(pid=1001)
        dead_proc.poll.return_value = 1

        with (
            patch(
                "vigilens.apps.workers.stream.worker.start_rtsp_segmenter",
                return_value=dead_proc,
            ),
            patch("vigilens.apps.workers.stream.worker.settings.stream_segmenter_max_retries", 1),
            patch("vigilens.apps.workers.stream.worker.SEGMENTER_STARTUP_GRACE_SECONDS", 0),
            patch("vigilens.apps.workers.stream.worker.asyncio.sleep", new=AsyncMock()),
        ):
            result = await worker._handle_message(msg)

        assert result is None
        assert "msg-bad" not in worker.in_progress_tasks

    async def test_failed_stream_does_not_block_next_stream(
        self, worker, stream_job_factory
    ):
        bad = _make_queue_message(
            stream_job_factory(overrides={"rtsp_url": "rtsp://localhost:8554/missing"}),
            message_id="msg-bad",
        )
        good = _make_queue_message(
            stream_job_factory(overrides={"rtsp_url": "rtsp://localhost:8554/test"}),
            message_id="msg-good",
        )

        dead_proc = MagicMock(pid=2001)
        dead_proc.poll.return_value = 1
        alive_proc = MagicMock(pid=2002)
        alive_proc.poll.return_value = None

        def segmenter_for_url(**kwargs):
            if kwargs["rtsp_url"].endswith("/missing"):
                return dead_proc
            return alive_proc

        with (
            patch("vigilens.apps.workers.stream.worker.settings.stream_segmenter_max_retries", 1),
            patch("vigilens.apps.workers.stream.worker.SEGMENTER_STARTUP_GRACE_SECONDS", 0),
            patch(
                "vigilens.apps.workers.stream.worker.start_rtsp_segmenter",
                side_effect=segmenter_for_url,
            ),
            patch.object(worker, "_action_screening_loop", new_callable=AsyncMock),
            patch("vigilens.apps.workers.stream.worker.asyncio.sleep", new=AsyncMock()),
        ):
            worker.in_progress_tasks.add("msg-bad")
            bad_result = await worker._handle_message(bad)

            assert bad_result is None
            assert "msg-bad" not in worker.in_progress_tasks

            worker.in_progress_tasks.add("msg-good")
            good_result = await worker._handle_message(good)

        assert good_result is alive_proc
        assert "msg-bad" not in worker.in_progress_tasks


class TestActionScreeningLoop:
    async def test_screens_mp4_files_in_directory(self, worker, tmp_path):
        """Verify the loop finds .mp4 files and calls screen_chunk."""
        # Create a fake chunk
        chunk = tmp_path / "chunk_001.mp4"
        chunk.write_bytes(b"\x00" * 100)

        screen_result = {"data": [{"score": 0.9}]}

        with (
            patch("vigilens.apps.workers.stream.worker.wait_until_video_ready"),
            patch(
                "vigilens.apps.workers.stream.worker.screen_chunk",
                new_callable=AsyncMock,
                return_value=screen_result,
            ) as mock_screen,
            patch.object(
                worker,
                "handle_selected_chunks",
                new_callable=AsyncMock,
            ) as _,
            patch("vigilens.apps.workers.stream.worker.time.time", side_effect=[0, 0, 100]),
            patch("vigilens.apps.workers.stream.worker.INGESTION_TIMEOUT", 10),
        ):
            worker.in_progress_tasks.add("msg-001")
            await worker._action_screening_loop(
                "msg-001",
                "s_abc",
                str(tmp_path),
                ["person falling"],
                [0.5],
                ["http://hook.example.com"],
            )

            mock_screen.assert_called_once()

    async def test_skips_non_mp4_files(self, worker, tmp_path):
        """Non-.mp4 files should be ignored."""
        (tmp_path / "readme.txt").write_text("hello")
        (tmp_path / "data.json").write_text("{}")

        with (
            patch(
                "vigilens.apps.workers.stream.worker.screen_chunk",
                new_callable=AsyncMock,
            ) as mock_screen,
            patch("vigilens.apps.workers.stream.worker.time.time", side_effect=[0, 100]),
            patch("vigilens.apps.workers.stream.worker.INGESTION_TIMEOUT", 10),
        ):
            worker.in_progress_tasks.add("msg-001")
            await worker._action_screening_loop(
                "msg-001", "s_abc", str(tmp_path), ["q"], [0.5]
            )

        mock_screen.assert_not_called()

    async def test_below_threshold_does_not_enqueue_selection(self, worker, tmp_path):
        """Chunks below threshold should not trigger upload/enqueue."""
        chunk = tmp_path / "chunk_002.mp4"
        chunk.write_bytes(b"\x00" * 100)

        screen_result = {"data": [{"score": 0.1}]}  # below threshold 0.5

        with (
            patch("vigilens.apps.workers.stream.worker.wait_until_video_ready"),
            patch(
                "vigilens.apps.workers.stream.worker.screen_chunk",
                new_callable=AsyncMock,
                return_value=screen_result,
            ),
            patch.object(
                worker, "handle_selected_chunks", new_callable=AsyncMock
            ) as mock_handle,
            patch("vigilens.apps.workers.stream.worker.time.time", side_effect=[0, 0, 100]),
            patch("vigilens.apps.workers.stream.worker.INGESTION_TIMEOUT", 10),
        ):
            worker.in_progress_tasks.add("msg-001")
            await worker._action_screening_loop(
                "msg-001", "s_abc", str(tmp_path), ["query"], [0.5]
            )

        mock_handle.assert_not_called()
        assert chunk.exists()

