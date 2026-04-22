import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import aiohttp
import tenacity

from vigilens.core.config import settings

logger = logging.getLogger(__name__)


def _is_discord_webhook(url: str) -> bool:
    return (
        "discord.com/api/webhooks/" in url
        or "discordapp.com/api/webhooks/" in url
    )


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _build_discord_payload(message: Any, chunk_presigned_urls: list[str]) -> dict[str, Any]:
    if hasattr(message, "model_dump"):
        message = message.model_dump()

    if not isinstance(message, dict):
        message = {"raw_message": str(message)}

    results = message.get("results") or []
    first = results[0] if results else {}
    title = first.get("title") or "Event detected"
    analysis = first.get("analysis") or "Vigilens verified an alert from stream analysis."
    key_identifiers = first.get("key_identifiers") or []

    field_rows = [
        {
            "name": "Action Detected",
            "value": "yes" if message.get("is_action_detected") else "no",
            "inline": True,
        },
        {
            "name": "Chunk URLs",
            "value": str(len(chunk_presigned_urls)),
            "inline": True,
        },
    ]
    if key_identifiers:
        field_rows.append(
            {
                "name": "Key Identifiers",
                "value": _truncate(", ".join(str(v) for v in key_identifiers), 250),
                "inline": False,
            }
        )

    if chunk_presigned_urls:
        field_rows.append(
            {
                "name": "Top Clip",
                "value": chunk_presigned_urls[0],
                "inline": False,
            }
        )

    return {
        "username": "Vigilens Alerts",
        "content": "Vigilens verified a detection.",
        "embeds": [
            {
                "title": _truncate(f"Vigilens Alert - {title}", 240),
                "description": _truncate(analysis, 1500),
                "color": 16751677,
                "fields": field_rows,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ],
    }


@tenacity.retry(
    stop=tenacity.stop_after_attempt(settings.webhook_max_retries),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    retry=tenacity.retry_if_exception_type(
        (aiohttp.ClientError, asyncio.TimeoutError)
    ),
)
async def send_webhook_with_retry(
    url: str,
    message: Any,
    chunk_presigned_urls: list[str],
) -> None:
    if _is_discord_webhook(url):
        payload = _build_discord_payload(message, chunk_presigned_urls)
    else:
        if hasattr(message, "model_dump"):
            message = message.model_dump()
        payload = {
            "message": message,
            "chunk_presigned_urls": chunk_presigned_urls,
        }

    timeout = aiohttp.ClientTimeout(total=settings.webhook_timeout)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=payload) as response:
            response.raise_for_status()
            logger.info("Webhook delivered to %s with status %s", url, response.status)
