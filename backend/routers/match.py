from fastapi import APIRouter, HTTPException, Request
from backend.db.pool import get_pool
from backend.models.responses import MatchDetailOut
from backend.cache.query import get_cached, set_cached
from backend.cache.keys import match_key

router = APIRouter()


def _str(val) -> str | None:
    return str(val) if val is not None else None


@router.get("/{id}", response_model=MatchDetailOut)
async def get_match(id: str, request: Request):
    cache_key = match_key(id)
    cached = await get_cached(cache_key)
    if cached:
        return cached

    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                gm.match_id,
                pa.person_id AS person_a_id,
                pa.full_name AS person_a_name,
                pb.person_id AS person_b_id,
                pb.full_name AS person_b_name,
                gm.similarity_score,
                gm.shared_segment_count,
                gm.total_shared_cm,
                gm.longest_segment_cm,
                gm.snp_overlap_count,
                gm.matching_method,
                gm.confidence_score,
                gm.computed_at
            FROM genome_match gm
            JOIN genome ga ON gm.genome_a_id = ga.genome_id
            JOIN genome gb ON gm.genome_b_id = gb.genome_id
            JOIN person pa ON ga.person_id = pa.person_id
            JOIN person pb ON gb.person_id = pb.person_id
            WHERE gm.match_id = $1::uuid
            """,
            id,
        )

    if not row:
        raise HTTPException(status_code=404, detail="Match not found")

    result = MatchDetailOut(
        match_id=_str(row["match_id"]),
        person_a_id=_str(row["person_a_id"]),
        person_a_name=row["person_a_name"],
        person_b_id=_str(row["person_b_id"]),
        person_b_name=row["person_b_name"],
        similarity_score=float(row["similarity_score"]) if row["similarity_score"] else None,
        shared_segment_count=row["shared_segment_count"],
        total_shared_cm=float(row["total_shared_cm"]) if row["total_shared_cm"] else None,
        longest_segment_cm=float(row["longest_segment_cm"]) if row["longest_segment_cm"] else None,
        snp_overlap_count=row["snp_overlap_count"],
        matching_method=row["matching_method"],
        confidence_score=float(row["confidence_score"]) if row["confidence_score"] else None,
        computed_at=str(row["computed_at"]) if row["computed_at"] else None,
    )

    await set_cached(cache_key, result.model_dump())
    return result