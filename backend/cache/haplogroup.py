import json
from backend.cache.client import get_redis
from backend.cache.keys import key_haplogroup
from backend.cache.ttl import TTL


async def get_haplogroup_cache(code: str) -> dict | None:
    r = await get_redis()
    raw = await r.get(key_haplogroup(code))
    return json.loads(raw) if raw else None


async def set_haplogroup_cache(code: str, payload: dict) -> None:
    r = await get_redis()
    await r.set(key_haplogroup(code), json.dumps(payload, default=str), ex=TTL.HAPLOGROUP)


async def invalidate_haplogroup(code: str) -> None:
    r = await get_redis()
    await r.delete(key_haplogroup(code))