"""Tests for redis_queue._parse_messages — the shared parsing logic in both
RedisStreamQueue and AsyncRedisStreamQueue.
"""

import json


from vigilens.integrations.redis_queue import (
    AsyncRedisStreamQueue,
    RedisStreamQueue,
    StreamQueueMessage,
)


def _build_raw_response(messages_per_stream: dict[str, list[tuple[str, dict]]]):
    """Build the raw response structure that Redis returns from XREADGROUP.

    Format: [(stream_name, [(msg_id, {field: value}), ...]), ...]
    """
    response = []
    for stream, entries in messages_per_stream.items():
        formatted = []
        for msg_id, payload_dict in entries:
            formatted.append((msg_id, {"payload": json.dumps(payload_dict)}))
        response.append((stream, formatted))
    return response


class TestParseMessagesSyncQueue:
    """Test RedisStreamQueue._parse_messages (instance method, but pure logic)."""

    def _get_parser(self):
        """Get access to _parse_messages without connecting to Redis."""
        # We create a subclass that skips __init__'s Redis connection
        obj = object.__new__(RedisStreamQueue)
        return obj._parse_messages

    def test_empty_response(self):
        parse = self._get_parser()
        assert parse([]) == []

    def test_single_message(self):
        parse = self._get_parser()
        raw = _build_raw_response({"mystream": [("1-0", {"action": "test"})]})
        result = parse(raw)
        assert len(result) == 1
        assert isinstance(result[0], StreamQueueMessage)
        assert result[0].stream == "mystream"
        assert result[0].message_id == "1-0"
        assert result[0].payload == {"action": "test"}

    def test_multiple_messages_single_stream(self):
        parse = self._get_parser()
        raw = _build_raw_response(
            {
                "s1": [
                    ("1-0", {"a": 1}),
                    ("2-0", {"b": 2}),
                    ("3-0", {"c": 3}),
                ]
            }
        )
        result = parse(raw)
        assert len(result) == 3
        assert [m.message_id for m in result] == ["1-0", "2-0", "3-0"]

    def test_multiple_streams(self):
        parse = self._get_parser()
        raw = _build_raw_response(
            {
                "stream.a": [("1-0", {"x": 1})],
                "stream.b": [("2-0", {"y": 2})],
            }
        )
        result = parse(raw)
        assert len(result) == 2
        assert result[0].stream == "stream.a"
        assert result[1].stream == "stream.b"

    def test_empty_payload_defaults_to_empty_dict(self):
        parse = self._get_parser()
        # Simulate missing payload field
        raw = [("mystream", [("1-0", {})])]
        result = parse(raw)
        assert result[0].payload == {}

    def test_nested_payload(self):
        parse = self._get_parser()
        nested = {
            "stream_id": "s_abc",
            "trigger_queries": ["fall", "fire"],
            "thresholds": [0.3, 0.5],
        }
        raw = _build_raw_response({"s": [("1-0", nested)]})
        result = parse(raw)
        assert result[0].payload == nested


class TestParseMessagesAsyncQueue:
    """Test AsyncRedisStreamQueue._parse_messages — same logic, different class."""

    def _get_parser(self):
        obj = object.__new__(AsyncRedisStreamQueue)
        return obj._parse_messages

    def test_empty_response(self):
        assert self._get_parser()([]) == []

    def test_single_message(self):
        parse = self._get_parser()
        raw = _build_raw_response({"s": [("10-0", {"k": "v"})]})
        result = parse(raw)
        assert len(result) == 1
        assert result[0].payload == {"k": "v"}

    def test_preserves_message_order(self):
        parse = self._get_parser()
        raw = _build_raw_response({"s": [(f"{i}-0", {"i": i}) for i in range(10)]})
        result = parse(raw)
        assert [m.payload["i"] for m in result] == list(range(10))

