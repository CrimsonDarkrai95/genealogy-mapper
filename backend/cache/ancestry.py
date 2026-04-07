import json
import logging
from backend.cache.client import get_redis
from backend.cache.keys import key_ancestry

log = logging.getLogger(__name__)


async def get_ancestry_cache(person_id: str):
    try:
        r = await get_redis()
        raw = await r.get(key_ancestry(person_id))
        return json.loads(raw) if raw else None
    except Exception as e:
        log.warning(f"Redis ancestry GET failed for person_id={person_id}: {e}")
        return None


async def set_ancestry_cache(person_id: str, value):
    try:
        r = await get_redis()
        await r.set(key_ancestry(person_id), json.dumps(value), ex=1800)
    except Exception as e:
        log.warning(f"Redis ancestry SET failed for person_id={person_id}: {e}")