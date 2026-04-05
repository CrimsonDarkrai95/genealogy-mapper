from fastapi import APIRouter, Request, Query
from backend.db.pool import get_pool
from backend.models.responses import TimelineOut, TimelineEventOut
from backend.cache.query import get_cached, set_cached
from backend.cache.keys import timeline_key

router = APIRouter()


@router.get("/timeline", response_model=TimelineOut)
async def get_timeline(
    request: Request,
    person_id: str = Query(...),
    from_year: int | None = Query(default=None, alias="from"),
    to_year: int | None = Query(default=None, alias="to"),
):
    cache_key = timeline_key(person_id, from_year, to_year)
    cached = await get_cached(cache_key)
    if cached:
        return cached

    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                plt.event_type,
                plt.event_year,
                plt.event_year_end,
                plt.certainty,
                r.region_name
            FROM person_location_timeline plt
            LEFT JOIN region r ON plt.region_id = r.region_id
            WHERE plt.person_id = $1::uuid
              AND ($2::int IS NULL OR plt.event_year >= $2)
              AND ($3::int IS NULL OR plt.event_year <= $3)
            ORDER BY plt.event_year ASC NULLS LAST
            """,
            person_id, from_year, to_year,
        )

    events = [
        TimelineEventOut(
            event_type=r["event_type"],
            event_year=r["event_year"],
            event_year_end=r["event_year_end"],
            region_name=r["region_name"],
            certainty=r["certainty"],
        )
        for r in rows
    ]

    result = TimelineOut(person_id=person_id, events=events)
    await set_cached(cache_key, result.model_dump())
    return result