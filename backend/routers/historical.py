import hashlib
from fastapi import APIRouter, Request, Query
from backend.db.pool import get_pool
from backend.models.responses import HistoricalListOut, HistoricalPersonOut
from backend.cache.query import get_cached, set_cached
from backend.cache.keys import historical_key

router = APIRouter()


def _str(val) -> str | None:
    return str(val) if val is not None else None


@router.get("/historical", response_model=HistoricalListOut)
async def get_historical(
    request: Request,
    dynasty_id: str | None = Query(default=None),
    region_id: int | None = Query(default=None),
    from_year: int | None = Query(default=None),
    to_year: int | None = Query(default=None),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0),
):
    params_str = f"{dynasty_id}{region_id}{from_year}{to_year}{limit}{offset}"
    params_hash = hashlib.sha256(params_str.encode()).hexdigest()[:12]
    cache_key = historical_key(params_hash)
    cached = await get_cached(cache_key)
    if cached:
        return cached

    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                p.person_id,
                p.full_name,
                p.birth_year,
                p.death_year,
                p.wikidata_qid,
                r.region_name AS birth_region,
                COUNT(*) OVER() AS total_count
            FROM person p
            LEFT JOIN region r ON p.birth_region_id = r.region_id
            LEFT JOIN dynasty_membership dm ON dm.person_id = p.person_id
            WHERE p.is_historical = true
              AND ($1::uuid IS NULL OR dm.dynasty_id = $1::uuid)
              AND ($2::int IS NULL OR p.birth_region_id = $2)
              AND ($3::int IS NULL OR p.birth_year >= $3)
              AND ($4::int IS NULL OR p.death_year <= $4)
            GROUP BY p.person_id, p.full_name, p.birth_year, p.death_year,
                     p.wikidata_qid, r.region_name
            ORDER BY p.birth_year ASC NULLS LAST
            LIMIT $5 OFFSET $6
            """,
            dynasty_id, region_id, from_year, to_year, limit, offset,
        )

    total = rows[0]["total_count"] if rows else 0
    page = (offset // limit) + 1

    results = [
        HistoricalPersonOut(
            person_id=_str(r["person_id"]),
            full_name=r["full_name"],
            birth_year=r["birth_year"],
            death_year=r["death_year"],
            birth_region=r["birth_region"],
            wikidata_qid=r["wikidata_qid"],
        )
        for r in rows
    ]

    result = HistoricalListOut(total_count=total, page=page, results=results)
    await set_cached(cache_key, result.model_dump())
    return result