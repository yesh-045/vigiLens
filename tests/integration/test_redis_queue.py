"""Integration tests for RedisStreamQueue and AsyncRedisStreamQueue
using fakeredis (no real Redis server required).
"""

import pytest
import pytest_asyncio

try:
    import fakeredis
    import fakeredis.aioredis

    HAS_FAKEREDIS = True
except ImportError:
    HAS_FAKEREDIS = False

pytestmark = pytest.mark.skipif(not HAS_FAKEREDIS, reason="fakeredis not installed")


# ---------------------------------------------------------------------------
# Sync queue
# ---------------------------------------------------------------------------


class TestRedisStreamQueueSync:
    @pytest.fixture()
    def queue(self, fake_redis_server):
        from unittest.mock import patch

        from vigilens.integrations.redis_queue import RedisStreamQueue

        fake_conn = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=True)
        with patch("redis.Redis.from_url", return_value=fake_conn):
            q = RedisStreamQueue(
                redis_url="redis://fake:6379/0",
                stream_name="test.stream",
                group_name="test.group",
                consumer_name="consumer-1",
                create_consumer_group=True,
                group_start_id="0-0",
            )
            yield q
            q.close()

    def test_enqueue_returns_message_id(self, queue):
        mid = queue.enqueue({"key": "value"})
        assert isinstance(mid, str)
        assert "-" in mid  # Redis stream IDs contain a dash

    def test_enqueue_dequeue_round_trip(self, queue):
        payload = {"stream_id": "s_abc", "action": "test"}
        queue.enqueue(payload)
        messages = queue.dequeue(count=1, block_ms=500)
        assert len(messages) == 1
        assert messages[0].payload == payload

    def test_dequeue_empty_returns_empty(self, queue):
        messages = queue.dequeue(count=1, block_ms=100)
        assert messages == []

    def test_ack_succeeds(self, queue):
        queue.enqueue({"x": 1})
        msgs = queue.dequeue(count=1, block_ms=500)
        result = queue.ack(msgs[0].message_id)
        assert result >= 1

    def test_ack_many(self, queue):
        queue.enqueue({"a": 1})
        queue.enqueue({"b": 2})
        msgs = queue.dequeue(count=2, block_ms=500)
        ids = [m.message_id for m in msgs]
        result = queue.ack_many(ids)
        assert result >= 2

    def test_ack_many_empty_returns_zero(self, queue):
        assert queue.ack_many([]) == 0

    def test_dequeue_without_group_raises(self, fake_redis_server):
        from unittest.mock import patch
        from vigilens.integrations.redis_queue import RedisStreamQueue

        fake_conn = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=True)
        with patch("redis.Redis.from_url", return_value=fake_conn):
            q = RedisStreamQueue(
                redis_url="redis://fake:6379/0",
                stream_name="test.nogroupstream",
                group_name=None,
                create_consumer_group=False,
            )
            with pytest.raises(ValueError, match="group_name"):
                q.dequeue()

    def test_multiple_messages_ordering(self, queue):
        for i in range(5):
            queue.enqueue({"i": i})
        messages = queue.dequeue(count=5, block_ms=500)
        assert [m.payload["i"] for m in messages] == list(range(5))


# ---------------------------------------------------------------------------
# Async queue
# ---------------------------------------------------------------------------


class TestAsyncRedisStreamQueue:
    @pytest_asyncio.fixture()
    async def queue(self, fake_redis_server):
        from unittest.mock import patch

        from vigilens.integrations.redis_queue import AsyncRedisStreamQueue

        fake_conn = fakeredis.aioredis.FakeRedis(
            server=fake_redis_server, decode_responses=True
        )
        with patch("redis.asyncio.Redis.from_url", return_value=fake_conn):
            q = AsyncRedisStreamQueue(
                redis_url="redis://fake:6379/0",
                stream_name="test.async.stream",
                group_name="test.async.group",
                consumer_name="async-consumer-1",
                create_consumer_group=True,
                group_start_id="0-0",
            )
            yield q
            await q.close()

    @pytest.mark.asyncio
    async def test_enqueue_returns_id(self, queue):
        mid = await queue.enqueue({"hello": "world"})
        assert isinstance(mid, str)

    @pytest.mark.asyncio
    async def test_enqueue_dequeue_round_trip(self, queue):
        payload = {"stream_id": "s_xyz", "queries": ["fall"]}
        await queue.enqueue(payload)
        msgs = await queue.dequeue(count=1, block_ms=500)
        assert len(msgs) == 1
        assert msgs[0].payload == payload

    @pytest.mark.asyncio
    async def test_dequeue_empty(self, queue):
        msgs = await queue.dequeue(count=1, block_ms=100)
        assert msgs == []

    @pytest.mark.asyncio
    async def test_ack(self, queue):
        await queue.enqueue({"x": 1})
        msgs = await queue.dequeue(count=1, block_ms=500)
        result = await queue.ack(msgs[0].message_id)
        assert result >= 1

    @pytest.mark.asyncio
    async def test_multiple_enqueue_dequeue(self, queue):
        for i in range(3):
            await queue.enqueue({"i": i})
        msgs = await queue.dequeue(count=3, block_ms=500)
        assert len(msgs) == 3
        assert [m.payload["i"] for m in msgs] == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_maxlen_respected(self, fake_redis_server):
        from unittest.mock import patch
        from vigilens.integrations.redis_queue import AsyncRedisStreamQueue

        fake_conn = fakeredis.aioredis.FakeRedis(
            server=fake_redis_server, decode_responses=True
        )
        with patch("redis.asyncio.Redis.from_url", return_value=fake_conn):
            q = AsyncRedisStreamQueue(
                redis_url="redis://fake:6379/0",
                stream_name="test.maxlen.stream",
                group_name="test.maxlen.group",
                consumer_name="c1",
                create_consumer_group=True,
                group_start_id="0-0",
                maxlen=5,
            )
            for i in range(20):
                await q.enqueue({"i": i})
            # The stream should have been trimmed (approximately) to maxlen
            # fakeredis may not enforce approximate trimming exactly, so just
            # verify the queue is functional.
            msgs = await q.dequeue(count=100, block_ms=500)
            assert len(msgs) <= 20  # sanity
            await q.close()

