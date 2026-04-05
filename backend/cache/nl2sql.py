from backend.cache.client import get_redis
from backend.cache.keys import key_nl2sql
from backend.cache.ttl import TTL


async def get_nl2sql_cache(query_hash: str) -> str | None:
    r = await get_redis()
    return await r.get(key_nl2sql(query_hash))


async def set_nl2sql_cache(query_hash: str, validated_sql: str) -> None:
    r = await get_redis()
    await r.set(key_nl2sql(query_hash), validated_sql, ex=TTL.NL2SQL)