import json
import os
import socket
from dataclasses import dataclass
import inspect
from typing import Any, Iterable, Mapping

import redis
from redis import asyncio as redis_async
from redis.exceptions import ResponseError


@dataclass(frozen=True)
class StreamQueueMessage:
    stream: str
    message_id: str
    payload: dict[str, Any]


class RedisStreamQueue:
    """Small Redis Streams wrapper for enqueue/dequeue with consumer groups."""

    def __init__(
        self,
        *,
        redis_url: str,
        stream_name: str,
        group_name: str | None = None,
        consumer_name: str | None = None,
        create_consumer_group: bool = True,
        group_start_id: str = "$",
        maxlen: int | None = None,
    ) -> None:
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name or f"{socket.gethostname()}-{os.getpid()}"
        self.maxlen = maxlen
        self._redis = redis.Redis.from_url(redis_url, decode_responses=True)

        if create_consumer_group and self.group_name:
            self.ensure_consumer_group(start_id=group_start_id)

    def ensure_consumer_group(self, start_id: str = "$") -> None:
        try:
            self._redis.xgroup_create(
                name=self.stream_name,
                groupname=self.group_name,
                id=start_id,
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def enqueue(self, payload: Mapping[str, Any]) -> str:
        fields = {"payload": json.dumps(payload, separators=(",", ":"))}
        kwargs: dict[str, Any] = {}
        if self.maxlen is not None:
            kwargs["maxlen"] = self.maxlen
            kwargs["approximate"] = True
        return self._redis.xadd(self.stream_name, fields, **kwargs)

    def dequeue(
        self, *, count: int = 1, block_ms: int = 5000
    ) -> list[StreamQueueMessage]:
        if not self.group_name:
            raise ValueError("group_name is required for dequeue()")

        response = self._redis.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_name,
            streams={self.stream_name: ">"},
            count=count,
            block=block_ms,
        )
        return self._parse_messages(response)

    def reclaim_stale(
        self,
        *,
        min_idle_ms: int,
        count: int = 50,
        start_id: str = "0-0",
    ) -> list[StreamQueueMessage]:
        if not self.group_name:
            raise ValueError("group_name is required for reclaim_stale()")

        result = self._redis.xautoclaim(
            name=self.stream_name,
            groupname=self.group_name,
            consumername=self.consumer_name,
            min_idle_time=min_idle_ms,
            start_id=start_id,
            count=count,
        )
        claimed = (
            result[1] if isinstance(result, (list, tuple)) and len(result) >= 2 else []
        )
        wrapped = [(self.stream_name, claimed)]
        return self._parse_messages(wrapped)

    def ack(self, message_id: str) -> int:
        if not self.group_name:
            raise ValueError("group_name is required for ack()")
        return self._redis.xack(self.stream_name, self.group_name, message_id)

    def ack_many(self, message_ids: Iterable[str]) -> int:
        if not self.group_name:
            raise ValueError("group_name is required for ack_many()")
        ids = list(message_ids)
        if not ids:
            return 0
        return self._redis.xack(self.stream_name, self.group_name, *ids)

    def close(self) -> None:
        self._redis.close()

    def _parse_messages(self, response: list[Any]) -> list[StreamQueueMessage]:
        messages: list[StreamQueueMessage] = []
        for stream, entries in response:
            for message_id, fields in entries:
                payload_raw = fields.get("payload", "{}")
                payload = json.loads(payload_raw)
                messages.append(
                    StreamQueueMessage(
                        stream=stream,
                        message_id=message_id,
                        payload=payload,
                    )
                )
        return messages


class AsyncRedisStreamQueue:
    """Async Redis Streams wrapper for enqueue/dequeue with consumer groups."""

    def __init__(
        self,
        *,
        redis_url: str,
        stream_name: str,
        group_name: str | None = None,
        consumer_name: str | None = None,
        create_consumer_group: bool = True,
        group_start_id: str = "$",
        maxlen: int | None = None,
        claim_stale_ms: int | None = None,
    ) -> None:
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name or f"{socket.gethostname()}-{os.getpid()}"
        self.maxlen = maxlen
        self.claim_stale_ms = claim_stale_ms
        self._redis = redis_async.Redis.from_url(redis_url, decode_responses=True)
        self._group_start_id = group_start_id
        self._auto_create_group = bool(create_consumer_group and group_name)
        self._group_ready = not self._auto_create_group

    async def ensure_consumer_group(self, start_id: str = "$") -> None:
        try:
            await self._redis.xgroup_create(
                name=self.stream_name,
                groupname=self.group_name,
                id=start_id,
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._group_ready = True

    async def enqueue(self, payload: Mapping[str, Any]) -> str:
        fields = {"payload": json.dumps(payload, separators=(",", ":"))}
        kwargs: dict[str, Any] = {}
        if self.maxlen is not None:
            kwargs["maxlen"] = self.maxlen
            kwargs["approximate"] = True
        return await self._redis.xadd(self.stream_name, fields, **kwargs)

    async def dequeue(
        self, *, count: int = 1, block_ms: int = 5000
    ) -> list[StreamQueueMessage]:
        if not self.group_name:
            raise ValueError("group_name is required for dequeue()")
        await self._ensure_group_if_needed()

        # 1. Read new messages first.
        response = await self._redis.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_name,
            streams={self.stream_name: ">"},
            count=count,
            block=block_ms,
        )
        messages = self._parse_messages(response)
        if messages:
            return messages

        # 2. If no new work arrived, attempt stale claim recovery.
        if self.claim_stale_ms is not None:
            return await self.reclaim_stale(min_idle_ms=self.claim_stale_ms, count=count)

        return []

    async def reclaim_stale(
        self,
        *,
        min_idle_ms: int,
        count: int = 50,
        start_id: str = "0-0",
    ) -> list[StreamQueueMessage]:
        if not self.group_name:
            raise ValueError("group_name is required for reclaim_stale()")
        await self._ensure_group_if_needed()

        result = await self._redis.xautoclaim(
            name=self.stream_name,
            groupname=self.group_name,
            consumername=self.consumer_name,
            min_idle_time=min_idle_ms,
            start_id=start_id,
            count=count,
        )
        claimed = (
            result[1] if isinstance(result, (list, tuple)) and len(result) >= 2 else []
        )
        wrapped = [(self.stream_name, claimed)]
        return self._parse_messages(wrapped)

    async def ack(self, message_id: str) -> int:
        """Ack a message. Returns the number of messages acknowledged."""
        if not self.group_name:
            raise ValueError("group_name is required for ack()")
        await self._ensure_group_if_needed()
        return await self._redis.xack(self.stream_name, self.group_name, message_id)

    async def ack_many(self, message_ids: Iterable[str]) -> int:
        if not self.group_name:
            raise ValueError("group_name is required for ack_many()")
        await self._ensure_group_if_needed()
        ids = list(message_ids)
        if not ids:
            return 0
        return await self._redis.xack(self.stream_name, self.group_name, *ids)

    async def close(self) -> None:
        close_result = self._redis.close()
        if inspect.isawaitable(close_result):
            await close_result

    async def _ensure_group_if_needed(self) -> None:
        if self._group_ready:
            return
        await self.ensure_consumer_group(start_id=self._group_start_id)

    def _parse_messages(self, response: list[Any]) -> list[StreamQueueMessage]:
        messages: list[StreamQueueMessage] = []
        for stream, entries in response:
            for message_id, fields in entries:
                payload_raw = fields.get("payload", "{}")
                payload = json.loads(payload_raw)
                messages.append(
                    StreamQueueMessage(
                        stream=stream,
                        message_id=message_id,
                        payload=payload,
                    )
                )
        return messages
