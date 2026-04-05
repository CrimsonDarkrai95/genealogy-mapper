import hashlib
from fastapi import APIRouter, Request, Query, HTTPException
from backend.db.pool import get_pool
from backend.models.responses import SearchOut, SearchResultOut
from backend.cache.query import get_cached, set_cached
from backend.cache.keys import search_key

router = APIRouter()


def _str(val) -> str | None:
    return str(val) if val is not None else None

@router.get("/search", response_model=SearchOut)
async def search(
    request: Request,
    q: str = Query(..., min_length=2),
    type: str | None = Query(default=None, pattern="^(person|dataset|region)$"),
):
    if not q.strip():
        raise HTTPException(status_code=400, detail="Query string cannot be empty")

    params_hash = hashlib.sha256(f"{q}{type}".encode()).hexdigest()[:12]
    cache_key = search_key(params_hash)
    cached = await get_cached(cache_key)
    if cached:
        return cached

    pool = get_pool()
    async with pool.acquire() as conn:

        # Try FTS first
        rows = await conn.fetch(
            """
            SELECT result_type, id, label, rank_score, snippet
            FROM (
                SELECT
                    'person' AS result_type,
                    person_id::text AS id,
                    full_name AS label,
                    ts_rank(bio_tsv, plainto_tsquery('english', $1)) AS rank_score,
                    left(bio_text, 200) AS snippet
                FROM person
                WHERE bio_tsv @@ plainto_tsquery('english', $1)
                  AND ($2::text IS NULL OR $2 = 'person')

                UNION ALL

                SELECT
                    'dataset' AS result_type,
                    dataset_id::text AS id,
                    dataset_name AS label,
                    ts_rank(description_tsv, plainto_tsquery('english', $1)) AS rank_score,
                    left(description, 200) AS snippet
                FROM dataset_source
                WHERE description_tsv @@ plainto_tsquery('english', $1)
                  AND ($2::text IS NULL OR $2 = 'dataset')

                UNION ALL

                SELECT
                    'dynasty' AS result_type,
                    dynasty_id::text AS id,
                    dynasty_name AS label,
                    ts_rank(description_tsv, plainto_tsquery('english', $1)) AS rank_score,
                    left(description, 200) AS snippet
                FROM dynasty
                WHERE description_tsv @@ plainto_tsquery('english', $1)
                  AND ($2::text IS NULL OR $2 = 'dynasty')
            ) combined
            ORDER BY rank_score DESC
            LIMIT 30
            """,
            q, type,
        )

        # FTS returned nothing — fall back to ILIKE on name columns
        if not rows:
            like_q = f"%{q}%"
            rows = await conn.fetch(
                """
                SELECT result_type, id, label,
                       0.0::float AS rank_score,
                       snippet
                FROM (
                    SELECT
                        'person' AS result_type,
                        person_id::text AS id,
                        full_name AS label,
                        left(bio_text, 200) AS snippet
                    FROM person
                    WHERE full_name ILIKE $1
                      AND ($2::text IS NULL OR $2 = 'person')

                    UNION ALL

                    SELECT
                        'dataset' AS result_type,
                        dataset_id::text AS id,
                        dataset_name AS label,
                        left(description, 200) AS snippet
                    FROM dataset_source
                    WHERE dataset_name ILIKE $1
                      AND ($2::text IS NULL OR $2 = 'dataset')

                    UNION ALL

                    SELECT
                        'dynasty' AS result_type,
                        dynasty_id::text AS id,
                        dynasty_name AS label,
                        left(description, 200) AS snippet
                    FROM dynasty
                    WHERE dynasty_name ILIKE $1
                      AND ($2::text IS NULL OR $2 = 'dynasty')
                ) combined
                ORDER BY label
                LIMIT 30
                """,
                like_q, type,
            )

    results = [
        SearchResultOut(
            result_type=r["result_type"],
            id=r["id"],
            label=r["label"],
            rank_score=float(r["rank_score"]) if r["rank_score"] else None,
            snippet=r["snippet"],
        )
        for r in rows
    ]

    result = SearchOut(query=q, results=results)
    await set_cached(cache_key, result.model_dump(), ttl=600)
    return result