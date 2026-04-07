import json
import logging
from backend.cache.client import get_redis

log = logging.getLogger(__name__)


async def get_cached(key: str):
    try:
        r = await get_redis()
        raw = await r.get(key)
        return json.loads(raw) if raw else None
    except Exception as e:
        log.warning(f"Redis GET failed for key={key}: {e}")
        return None


async def set_cached(key: str, value, ttl: int = 900):
    try:
        r = await get_redis()
        await r.set(key, json.dumps(value), ex=ttl)
    except Exception as e:
        log.warning(f"Redis SET failed for key={key}: {e}")


async def delete_cached(key: str):
    try:
        r = await get_redis()
        await r.delete(key)
    except Exception as e:
        log.warning(f"Redis DELETE failed for key={key}: {e}")