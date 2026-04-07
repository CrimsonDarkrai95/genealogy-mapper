import hashlib
import os
from dotenv import load_dotenv
from groq import Groq
from fastapi import HTTPException

load_dotenv()

from backend.db.pool import get_pool
from backend.cache.nl2sql import get_nl2sql_cache, set_nl2sql_cache
from backend.cache.rate_limiter import check_rate_limit
from backend.nl.prompt import build_prompt
from backend.nl.validator import validate_sql, ValidationError

_client = None

def _get_client() -> Groq:
    global _client
    if _client is None:
        _client = Groq(api_key=os.getenv("GROQ_API_KEY"))
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

    if not cache_hit:
        # 3. Build prompt and call Groq
        prompt = build_prompt(nl_query)
        try:
            response = _get_client().chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=1024,
            )
            raw_sql = (response.choices[0].message.content or "").strip()
            if not raw_sql:
                raise HTTPException(status_code=422, detail="Groq returned an empty response.")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Groq API error: {str(e)}")

        # 4. Validate the generated SQL
        try:
            sql = validate_sql(raw_sql)
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=str(e))

        # 5. Cache the validated SQL
        await set_nl2sql_cache(query_hash, sql)
    else:
        sql = cached_sql

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