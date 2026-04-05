import json
from backend.cache.client import get_redis
from backend.cache.keys import key_cluster
from backend.cache.ttl import TTL


async def get_cluster_cache(cluster_id: str) -> dict | None:
    r = await get_redis()
    raw = await r.get(key_cluster(cluster_id))
    return json.loads(raw) if raw else None


async def set_cluster_cache(cluster_id: str, payload: dict) -> None:
    r = await get_redis()
    await r.set(key_cluster(cluster_id), json.dumps(payload, default=str), ex=TTL.CLUSTER)


async def invalidate_cluster(cluster_id: str) -> None:
    r = await get_redis()
    await r.delete(key_cluster(cluster_id))