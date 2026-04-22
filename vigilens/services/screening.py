from typing import List
import aiohttp
import json
import tenacity
import logging
import base64
from vigilens.integrations.llm_client import is_retryable_exception

from vigilens.core.config import settings

logger = logging.getLogger(__name__)


@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
    retry=tenacity.retry_if_exception(is_retryable_exception),
)
async def screen_chunk(chunk_path: str, trigger_queries: List[str]) -> bool:
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=settings.screener_timeout)
    ) as session:
        logger.debug(
            f"Screening chunk {chunk_path} with trigger queries {trigger_queries}"
        )
        async with session.post(
            f"{settings.screener_base_url}/v1/score",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {settings.screener_api_key}",
            },
            data=json.dumps(
                {
                    "model": settings.screener_model,
                    "queries": trigger_queries,
                    "documents": {
                        "content": [
                            {
                                "type": "video_url",
                                "video_url": {"url": video_to_data_url(chunk_path)},
                            }
                            for _ in trigger_queries
                        ]
                    },
                    "mm_processor_kwargs": {},
                }
            ),
        ) as response:
            logger.debug(
                f"Screening chunk {chunk_path} with trigger queries {trigger_queries} response: {response}"
            )
            response.raise_for_status()
            return await response.json()


def video_to_data_url(path: str, mime="video/mp4") -> str:
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    return f"data:{mime};base64,{b64}"
