from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.db.pool import init_pool, close_pool
from backend.cache.client import init_redis, close_redis
from backend.routers import person
from backend.routers import match
from backend.routers import relationship
from backend.routers import haplogroup
from backend.routers import cluster
from backend.routers import historical
from backend.routers import search
from backend.routers import timeline
from backend.routers import nl_query


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = await init_pool()
    try:
        app.state.redis = await init_redis()
    except Exception as e:
        print(f"WARNING: Redis unavailable at startup: {e}")
        app.state.redis = None
    yield
    await close_pool()
    await close_redis()


app = FastAPI(
    title="Historical Figure Genetic Genealogy & Ancestry Mapper",
    version="1.0.0",
    description="Probabilistic genetic ancestry inference between modern individuals and historical figures.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(person.router, prefix="/person", tags=["Person"])
app.include_router(match.router, prefix="/match", tags=["Match"])
app.include_router(relationship.router, prefix="/relationship", tags=["Relationship"])
app.include_router(haplogroup.router, prefix="/haplogroup", tags=["Haplogroup"])
app.include_router(cluster.router, prefix="/cluster", tags=["Cluster"])
app.include_router(historical.router, tags=["Historical"])
app.include_router(search.router, tags=["Search"])
app.include_router(timeline.router, tags=["Timeline"])
app.include_router(nl_query.router, prefix="/query", tags=["NL Query"])


@app.get("/health", tags=["Health"])
async def health():
    from backend.db.pool import get_pool
    from backend.cache.client import get_redis

    db_status = "ok"
    redis_status = "ok"

    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception:
        db_status = "error"

    try:
        r = await get_redis()
        await r.ping()
    except Exception:
        redis_status = "error"

    return {
        "status": "ok" if db_status == "ok" and redis_status == "ok" else "degraded",
        "database": db_status,
        "redis": redis_status,
    }