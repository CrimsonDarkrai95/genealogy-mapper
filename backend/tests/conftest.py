import pytest
import pytest_asyncio
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def app():
    from backend.main import app as fastapi_app
    return fastapi_app


@pytest.fixture
def mock_pool():
    mock_conn = MagicMock()
    mock_conn.fetch = AsyncMock(return_value=[])
    mock_conn.fetchrow = AsyncMock(return_value=None)
    mock_conn.fetchval = AsyncMock(return_value=0)
    mock_conn.execute = AsyncMock(return_value="OK")

    mock_acquire = MagicMock()
    mock_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_acquire.__aexit__ = AsyncMock(return_value=False)

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=mock_acquire)
    pool._mock_conn = mock_conn
    return pool


@pytest.fixture
def mock_redis():
    redis = MagicMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    redis.delete = AsyncMock(return_value=1)
    redis.incr = AsyncMock(return_value=1)
    redis.expire = AsyncMock(return_value=True)
    redis.ping = AsyncMock(return_value=True)
    return redis


@pytest_asyncio.fixture
async def client(app, mock_pool, mock_redis):
    """
    Patches:
      - backend.db.pool.get_pool        (sync, returns pool)
      - backend.cache.client.get_redis  (async, returns redis client)
    Also patches _pool directly in cache.client so the lifespan
    does not attempt a real Redis connection during test startup.
    """
    with patch("backend.db.pool.get_pool", return_value=mock_pool), \
         patch("backend.db.pool._pool", mock_pool), \
         patch("backend.cache.client._pool", mock_redis), \
         patch("backend.cache.client.get_redis", new=AsyncMock(return_value=mock_redis)):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as ac:
            yield ac, mock_pool._mock_conn, mock_redis


@pytest_asyncio.fixture
async def live_client():
    from backend.main import app as fastapi_app
    from asgi_lifespan import LifespanManager

    async with LifespanManager(fastapi_app) as manager:
        async with AsyncClient(
            transport=ASGITransport(app=manager.app),
            base_url="http://test"
        ) as ac:
            yield ac