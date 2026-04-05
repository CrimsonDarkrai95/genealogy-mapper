import os
import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")

_pool: redis.Redis | None = None


async def init_redis() -> redis.Redis:
    """
    Initialise Redis connection (called at app startup)
    """
    global _pool

    if _pool is None:
        if REDIS_URL is None:
            raise RuntimeError("REDIS_URL is not set in .env")

        _pool = redis.from_url(
            REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            max_connections=10,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )

        # Test connection
        await _pool.ping()

    return _pool


async def get_redis() -> redis.Redis:
    if _pool is None:
        raise RuntimeError("Redis not initialised. Call init_redis() first.")
    return _pool


async def close_redis():
    global _pool
    if _pool:
        await _pool.aclose()
        _pool = None