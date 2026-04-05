import hashlib
import os
from dotenv import load_dotenv
from google import genai
from fastapi import HTTPException

load_dotenv()

from backend.db.pool import get_pool
from backend.cache.nl2sql import get_nl2sql_cache, set_nl2sql_cache
from backend.cache.rate_limiter import check_rate_limit
from backend.nl.prompt import build_prompt
from backend.nl.validator import validate_sql, ValidationError

_client = None

def _get_client():
    global _client
    if _client is None:
        _client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    return _client


def _hash_query(nl_query: str) -> str:
    normalised = nl_query.strip().lower()
    return hashlib.sha256(normalised.encode()).hexdigest()


async def run_nl_pipeline(nl_query: str, api_key: str) -> dict:
    # 1. Rate limit check
    allowed = await check_rate_limit(api_key)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Maximum 60 requests per minute."
        )

    query_hash = _hash_query(nl_query)

    # 2. Check NL-to-SQL cache
    cached_sql = await get_nl2sql_cache(query_hash)
    cache_hit = cached_sql is not None

    if cache_hit:
        sql = cached_sql
    else:
        # 3. Build prompt and call Gemini Flash
        prompt = build_prompt(nl_query)
        try:
            response = _get_client().models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt
                )
            raw_sql = response.text.strip()
        except ValueError:
            # Safety filter or empty response from Gemini
            raise HTTPException(
                status_code=422,
                detail="Gemini returned no usable SQL. The query may have triggered a safety filter."
            )
        except Exception as e:
            raise HTTPException(
                status_code=502,
                detail=f"Gemini API error: {str(e)}"
            )

        # 4. Validate the generated SQL
        try:
            sql = validate_sql(raw_sql)
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=str(e))

        # 5. Cache the validated SQL
        await set_nl2sql_cache(query_hash, sql)

    # 6. Execute against PostgreSQL
    pool = get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"SQL execution error: {str(e)}"
        )

    results = [dict(row) for row in rows]

    return {
        "query": nl_query,
        "generated_sql": sql,
        "cached": cache_hit,
        "row_count": len(results),
        "results": results,
    }