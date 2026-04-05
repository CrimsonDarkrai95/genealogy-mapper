from fastapi import APIRouter, HTTPException, Request
from backend.db.pool import get_pool
from backend.models.responses import HaplogroupMembersOut, HaplogroupMemberOut
from backend.cache.query import get_cached, set_cached
from backend.cache.keys import haplogroup_key

router = APIRouter()


def _str(val) -> str | None:
    return str(val) if val is not None else None


@router.get("/{code}/members", response_model=HaplogroupMembersOut)
async def get_haplogroup_members(code: str, request: Request):
    cache_key = haplogroup_key(code)
    cached = await get_cached(cache_key)
    if cached:
        return cached

    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH RECURSIVE descendants AS (
                SELECT haplogroup_id, haplogroup_code
                FROM haplogroup_reference
                WHERE haplogroup_code = $1

                UNION ALL

                SELECT h.haplogroup_id, h.haplogroup_code
                FROM haplogroup_reference h
                JOIN descendants d ON h.parent_haplogroup_id = d.haplogroup_id
            )
            SELECT
                p.person_id,
                p.full_name,
                p.is_historical,
                g.y_haplogroup,
                g.mt_haplogroup
            FROM genome g
            JOIN person p ON g.person_id = p.person_id
            WHERE g.y_haplogroup IN (SELECT haplogroup_code FROM descendants)
               OR g.mt_haplogroup IN (SELECT haplogroup_code FROM descendants)
            ORDER BY p.full_name
            LIMIT 200
            """,
            code,
        )

    if not rows:
        raise HTTPException(status_code=404, detail=f"Haplogroup '{code}' not found or has no members")

    members = [
        HaplogroupMemberOut(
            person_id=_str(r["person_id"]),
            full_name=r["full_name"],
            is_historical=r["is_historical"],
            haplogroup_exact_code=r["y_haplogroup"] or r["mt_haplogroup"],
        )
        for r in rows
    ]

    result = HaplogroupMembersOut(
        haplogroup_code=code,
        member_count=len(members),
        members=members,
    )

    await set_cached(cache_key, result.model_dump(), ttl=3600)
    return result