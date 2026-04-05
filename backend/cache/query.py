# backend/cache/query.py
import json
import logging
import os

import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

_redis_client = None


def get_redis():
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(
            os.getenv("REDIS_URL"),
            decode_responses=True
        )
    return _redis_client


async def get_cached(key: str):
    try:
        r = get_redis()
        raw = await r.get(key)
        return json.loads(raw) if raw else None
    except Exception as e:
        log.warning(f"Redis GET failed for key={key}: {e}")
        return None


async def set_cached(key: str, value, ttl: int = 900):
    try:
        r = get_redis()
        await r.set(key, json.dumps(value), ex=ttl)
    except Exception as e:
        log.warning(f"Redis SET failed for key={key}: {e}")


async def delete_cached(key: str):
    try:
        r = get_redis()
        await r.delete(key)
    except Exception as e:
        log.warning(f"Redis DELETE failed for key={key}: {e}")