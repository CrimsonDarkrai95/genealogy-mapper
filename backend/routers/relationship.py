from fastapi import APIRouter, HTTPException, Request
from backend.db.pool import get_pool
from backend.models.responses import RelationshipDetailOut
from backend.cache.query import get_cached, set_cached
from backend.cache.keys import relationship_key

router = APIRouter()


def _str(val) -> str | None:
    return str(val) if val is not None else None


@router.get("/{id}", response_model=RelationshipDetailOut)
async def get_relationship(id: str, request: Request):
    cache_key = relationship_key(id)
    cached = await get_cached(cache_key)
    if cached:
        return cached

    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                ir.relationship_id,
                pa.person_id AS person_a_id,
                pa.full_name AS person_a_name,
                pb.person_id AS person_b_id,
                pb.full_name AS person_b_name,
                ir.relationship_type,
                ir.generational_distance,
                ir.confidence_level,
                ir.inference_method,
                ir.supporting_evidence,
                ir.inferred_at
            FROM inferred_relationship ir
            JOIN person pa ON ir.person_a_id = pa.person_id
            JOIN person pb ON ir.person_b_id = pb.person_id
            WHERE ir.relationship_id = $1::uuid
            """,
            id,
        )

    if not row:
        raise HTTPException(status_code=404, detail="Relationship not found")

    result = RelationshipDetailOut(
        relationship_id=_str(row["relationship_id"]),
        person_a_id=_str(row["person_a_id"]),
        person_a_name=row["person_a_name"],
        person_b_id=_str(row["person_b_id"]),
        person_b_name=row["person_b_name"],
        relationship_type=row["relationship_type"],
        generational_distance=row["generational_distance"],
        confidence_level=float(row["confidence_level"]) if row["confidence_level"] else None,
        inference_method=row["inference_method"],
        supporting_evidence=(
        row["supporting_evidence"] if isinstance(row["supporting_evidence"], dict)
        else None
    ) if row["supporting_evidence"] else None,
        inferred_at=str(row["inferred_at"]) if row["inferred_at"] else None,
    )

    await set_cached(cache_key, result.model_dump())
    return result