# backend/cache/ancestry.py
import json
import logging
import os

import redis.asyncio as redis
from dotenv import load_dotenv

from backend.cache.keys import key_ancestry

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


async def get_ancestry_cache(person_id: str):
    try:
        r = get_redis()
        raw = await r.get(key_ancestry(person_id))
        return json.loads(raw) if raw else None
    except Exception as e:
        log.warning(f"Redis ancestry GET failed for person_id={person_id}: {e}")
        return None


async def set_ancestry_cache(person_id: str, value):
    try:
        r = get_redis()
        await r.set(key_ancestry(person_id), json.dumps(value), ex=1800)
    except Exception as e:
        log.warning(f"Redis ancestry SET failed for person_id={person_id}: {e}")