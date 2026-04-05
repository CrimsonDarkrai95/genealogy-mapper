from fastapi import APIRouter, HTTPException, Request
from backend.db.pool import get_pool
from backend.models.responses import ClusterDetailOut, ClusterMemberOut
from backend.cache.cluster import get_cluster_cache, set_cluster_cache

router = APIRouter()


def _str(val) -> str | None:
    return str(val) if val is not None else None


@router.get("/{id}", response_model=ClusterDetailOut)
async def get_cluster(id: str, request: Request):
    cached = await get_cluster_cache(id)
    if cached:
        return cached

    pool = get_pool()
    async with pool.acquire() as conn:
        meta = await conn.fetchrow(
            """
            SELECT cluster_id, cluster_code, cluster_name
            FROM population_cluster
            WHERE cluster_id = $1::uuid
            """,
            id,
        )
        if not meta:
            raise HTTPException(status_code=404, detail="Cluster not found")

        rows = await conn.fetch(
            """
            SELECT
                p.person_id,
                p.full_name,
                gca.genome_id,
                gca.membership_probability
            FROM genome_cluster_assignment gca
            JOIN genome g ON gca.genome_id = g.genome_id
            JOIN person p ON g.person_id = p.person_id
            WHERE gca.cluster_id = $1::uuid
            ORDER BY gca.membership_probability DESC
            LIMIT 200
            """,
            id,
        )

    members = [
        ClusterMemberOut(
            person_id=_str(r["person_id"]),
            full_name=r["full_name"],
            genome_id=_str(r["genome_id"]),
            membership_probability=float(r["membership_probability"]) if r["membership_probability"] else None,
        )
        for r in rows
    ]

    result = ClusterDetailOut(
        cluster_id=_str(meta["cluster_id"]),
        cluster_code=meta["cluster_code"],
        cluster_name=meta["cluster_name"],
        member_count=len(members),
        members=members,
    )

    await set_cluster_cache(id, result.model_dump())
    return result