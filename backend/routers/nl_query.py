from fastapi import APIRouter, HTTPException, Request
from backend.db.pool import get_pool
from backend.models.responses import NLQueryRequest, NLQueryOut
from backend.cache.rate_limiter import check_rate_limit
from backend.nl.pipeline import run_nl_pipeline

router = APIRouter()

RATE_LIMIT_PER_MINUTE = 10


@router.post("/nl", response_model=NLQueryOut)
async def nl_query(body: NLQueryRequest, request: Request):
    # 1. Rate limit check
    allowed, current_count = await check_rate_limit(body.api_key)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded: {current_count}/{RATE_LIMIT_PER_MINUTE} requests per minute",
        )

    # 2. Run NL pipeline
    pipeline_result = await run_nl_pipeline(body.query)

    if not pipeline_result["valid"]:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "SQL generation failed validation",
                "reason": pipeline_result["error"],
            },
        )

    sql = pipeline_result["sql"]
    cached = pipeline_result["cached"]

    # 3. Execute validated SQL
    pool = get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "Query execution failed", "reason": str(e)},
        )

    # 4. Serialise asyncpg Records to plain dicts
    def serialise_row(row) -> dict:
        result = {}
        for key, val in dict(row).items():
            if hasattr(val, "isoformat"):
                result[key] = val.isoformat()
            else:
                result[key] = val
        return result

    results = [serialise_row(r) for r in rows]

    return NLQueryOut(
        query=body.query,
        generated_sql=sql,
        cached=cached,
        results=results,
        row_count=len(results),
    )