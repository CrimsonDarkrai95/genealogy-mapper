import asyncpg
import ssl
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    global _pool

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    _pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=2,
        max_size=10,
        ssl=ssl_ctx,
        statement_cache_size=0,
        timeout=30,
    )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database pool not initialised. Call init_pool() first.")
    return _pool