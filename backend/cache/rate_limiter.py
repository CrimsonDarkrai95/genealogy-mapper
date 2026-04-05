from datetime import datetime, timezone
from backend.cache.client import get_redis
from backend.cache.keys import key_rate_limit
from backend.cache.ttl import TTL

RATE_LIMIT_MAX = 10  # matches nl_query.py


async def check_rate_limit(api_key: str) -> tuple[bool, int]:
    r = await get_redis()
    window_minute = int(datetime.now(timezone.utc).timestamp() // 60)
    key = key_rate_limit(api_key, window_minute)

    count = await r.incr(key)
    if count == 1:
        await r.expire(key, TTL.RATE_LIMIT)

    allowed = count <= RATE_LIMIT_MAX
    return allowed, count