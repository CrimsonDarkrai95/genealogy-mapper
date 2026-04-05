import json
from uuid import UUID
from fastapi import APIRouter, Request, HTTPException, Query
from backend.db.pool import get_pool
from backend.models.responses import (
    PersonOut, AliasOut, ExternalIdOut, RegionOut,
    PersonGenomeOut, GenomeOut,
    PersonMatchesOut, MatchSummaryOut,
    PersonAncestryOut, AncestryNodeOut,
    PersonLineageOut, DynastyMembershipOut,
)
from backend.cache.query import get_cached, set_cached
from backend.cache.ancestry import get_ancestry_cache, set_ancestry_cache
from backend.cache.keys import (
    person_key, genome_key, matches_key, lineage_key
)
import hashlib

router = APIRouter()


def _str(val) -> str | None:
    return str(val) if val is not None else None


# ── GET /person/{id} ──────────────────────────────────────────────────────────

@router.get("/{id}", response_model=PersonOut)
async def get_person(id: str, request: Request):
    cache_key = person_key(id)
    cached = await get_cached(cache_key)
    if cached:
        return cached

    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                p.person_id, p.full_name, p.birth_name, p.is_historical,
                p.birth_year, p.death_year, p.gender, p.wikidata_qid,
                r.region_id, r.region_name, r.modern_country, r.region_type
            FROM person p
            LEFT JOIN region r ON p.birth_region_id = r.region_id
            WHERE p.person_id = $1::uuid
            """,
            id,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Person not found")

        aliases = await conn.fetch(
            """
            SELECT alias_id, alias_name, language_code, alias_type
            FROM person_alias WHERE person_id = $1::uuid
            """,
            id,
        )
        ext_ids = await conn.fetch(
            """
            SELECT ext_id, source_name, external_key, url
            FROM person_external_id WHERE person_id = $1::uuid
            """,
            id,
        )

    birth_region = None
    if row["region_id"]:
        birth_region = RegionOut(
            region_id=row["region_id"],
            region_name=row["region_name"],
            modern_country=row["modern_country"],
            region_type=row["region_type"],
        )

    result = PersonOut(
        person_id=_str(row["person_id"]),
        full_name=row["full_name"],
        birth_name=row["birth_name"],
        is_historical=row["is_historical"],
        birth_year=row["birth_year"],
        death_year=row["death_year"],
        gender=row["gender"],
        wikidata_qid=row["wikidata_qid"],
        birth_region=birth_region,
        aliases=[
            AliasOut(
                alias_id=_str(a["alias_id"]),
                alias_name=a["alias_name"],
                language_code=a["language_code"],
                alias_type=a["alias_type"],
            )
            for a in aliases
        ],
        external_ids=[
            ExternalIdOut(
                ext_id=_str(e["ext_id"]),
                source_name=e["source_name"],
                external_key=e["external_key"],
                url=e["url"],
            )
            for e in ext_ids
        ],
    )

    await set_cached(cache_key, result.model_dump())
    return result


# ── GET /person/{id}/genome ───────────────────────────────────────────────────

@router.get("/{id}/genome", response_model=PersonGenomeOut)
async def get_person_genome(id: str, request: Request):
    cache_key = genome_key(id)
    cached = await get_cached(cache_key)
    if cached:
        return cached

    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                genome_id, dataset_id, y_haplogroup, mt_haplogroup,
                coverage_depth, coverage_breadth, endogenous_dna_pct,
                damage_pattern, assembly_reference, raw_metadata,
                ingested_at
            FROM genome
            WHERE person_id = $1::uuid
            ORDER BY ingested_at DESC
            """,
            id,
        )

    genomes = [
        GenomeOut(
            genome_id=_str(r["genome_id"]),
            y_haplogroup=r["y_haplogroup"],
            mt_haplogroup=r["mt_haplogroup"],
            coverage_depth=float(r["coverage_depth"]) if r["coverage_depth"] else None,
            coverage_breadth=float(r["coverage_breadth"]) if r["coverage_breadth"] else None,
            endogenous_dna_pct=float(r["endogenous_dna_pct"]) if r["endogenous_dna_pct"] else None,
            damage_pattern=r["damage_pattern"],
            assembly_reference=r["assembly_reference"],
            dataset_id=r["dataset_id"],
            raw_metadata=(
                r["raw_metadata"] if isinstance(r["raw_metadata"], dict)
                else None
                ) if r["raw_metadata"] else None,
                ingested_at=str(r["ingested_at"]) if r["ingested_at"] else None,
                )
                for r in rows
                ]

    result = PersonGenomeOut(person_id=id, genomes=genomes)
    await set_cached(cache_key, result.model_dump())
    return result


# ── GET /person/{id}/matches ──────────────────────────────────────────────────

@router.get("/{id}/matches", response_model=PersonMatchesOut)
async def get_person_matches(
    id: str,
    request: Request,
    method: str | None = Query(default=None),
    min_score: float = Query(default=0.0),
    limit: int = Query(default=50, le=200),
):
    params_hash = hashlib.sha256(f"{method}{min_score}{limit}".encode()).hexdigest()[:12]
    cache_key = matches_key(id, params_hash)
    cached = await get_cached(cache_key)
    if cached:
        return cached

    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                gm.match_id,
                CASE
                    WHEN ga.person_id = $1::uuid THEN gb.person_id
                    ELSE ga.person_id
                END AS counterpart_person_id,
                CASE
                    WHEN ga.person_id = $1::uuid THEN pb.person_id
                    ELSE pa.person_id
                END AS _cp_id,
                CASE
                    WHEN ga.person_id = $1::uuid THEN pb.full_name
                    ELSE pa.full_name
                END AS counterpart_name,
                gm.similarity_score,
                gm.shared_segment_count,
                gm.total_shared_cm,
                gm.matching_method,
                gm.confidence_score
            FROM genome_match gm
            JOIN genome ga ON gm.genome_a_id = ga.genome_id
            JOIN genome gb ON gm.genome_b_id = gb.genome_id
            JOIN person pa ON ga.person_id = pa.person_id
            JOIN person pb ON gb.person_id = pb.person_id
            WHERE (ga.person_id = $1::uuid OR gb.person_id = $1::uuid)
              AND gm.similarity_score >= $2
              AND ($3::text IS NULL OR gm.matching_method = $3)
            ORDER BY gm.similarity_score DESC
            LIMIT $4
            """,
            id, min_score, method, limit,
        )

    matches = [
        MatchSummaryOut(
            match_id=_str(r["match_id"]),
            counterpart_person_id=_str(r["counterpart_person_id"]),
            counterpart_name=r["counterpart_name"],
            similarity_score=float(r["similarity_score"]) if r["similarity_score"] else None,
            shared_segment_count=r["shared_segment_count"],
            total_shared_cm=float(r["total_shared_cm"]) if r["total_shared_cm"] else None,
            matching_method=r["matching_method"],
            confidence_score=float(r["confidence_score"]) if r["confidence_score"] else None,
        )
        for r in rows
    ]

    result = PersonMatchesOut(person_id=id, matches=matches)
    await set_cached(cache_key, result.model_dump())
    return result


# ── GET /person/{id}/ancestry ─────────────────────────────────────────────────

@router.get("/{id}/ancestry", response_model=PersonAncestryOut)
async def get_person_ancestry(
    id: str,
    request: Request,
    depth: int = Query(default=10, le=20),
    min_confidence: float = Query(default=0.0),
):
    cached = await get_ancestry_cache(id)
    if cached:
        return cached

    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH RECURSIVE ancestry AS (
                SELECT
                    ir.person_a_id AS ancestor_id,
                    1 AS generation,
                    ir.relationship_type,
                    ir.confidence_level,
                    ir.inference_method
                FROM inferred_relationship ir
                WHERE ir.person_b_id = $1::uuid
                  AND ir.relationship_type = 'ancestor'
                  AND ir.confidence_level >= $3

                UNION ALL

                SELECT
                    ir.person_a_id,
                    a.generation + 1,
                    ir.relationship_type,
                    ir.confidence_level,
                    ir.inference_method
                FROM inferred_relationship ir
                JOIN ancestry a ON ir.person_b_id = a.ancestor_id
                WHERE a.generation < $2
                  AND ir.relationship_type = 'ancestor'
                  AND ir.confidence_level >= $3
            )
            SELECT
                a.generation,
                p.person_id,
                p.full_name,
                a.relationship_type,
                a.confidence_level,
                a.inference_method
            FROM ancestry a
            JOIN person p ON a.ancestor_id = p.person_id
            ORDER BY a.generation ASC
            LIMIT 50
            """,
            id, depth, min_confidence,
        )

    chain = [
        AncestryNodeOut(
            generation=r["generation"],
            person_id=_str(r["person_id"]),
            full_name=r["full_name"],
            relationship_type=r["relationship_type"],
            confidence_level=float(r["confidence_level"]) if r["confidence_level"] else None,
            inference_method=r["inference_method"],
        )
        for r in rows
    ]

    result = PersonAncestryOut(person_id=id, ancestry_chain=chain)
    await set_ancestry_cache(id, result.model_dump())
    return result


# ── GET /person/{id}/lineage ──────────────────────────────────────────────────

@router.get("/{id}/lineage", response_model=PersonLineageOut)
async def get_person_lineage(id: str, request: Request):
    cache_key = lineage_key(id)
    cached = await get_cached(cache_key)
    if cached:
        return cached

    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                d.dynasty_id,
                d.dynasty_name,
                dm.role,
                dm.start_year,
                dm.end_year,
                r.region_name AS origin_region
            FROM dynasty_membership dm
            JOIN dynasty d ON dm.dynasty_id = d.dynasty_id
            LEFT JOIN region r ON d.origin_region_id = r.region_id
            WHERE dm.person_id = $1::uuid
            ORDER BY dm.start_year ASC NULLS LAST
            """,
            id,
        )

    memberships = [
        DynastyMembershipOut(
            dynasty_id=_str(r["dynasty_id"]),
            dynasty_name=r["dynasty_name"],
            role=r["role"],
            start_year=r["start_year"],
            end_year=r["end_year"],
            origin_region=r["origin_region"],
        )
        for r in rows
    ]

    result = PersonLineageOut(person_id=id, memberships=memberships)
    await set_cached(cache_key, result.model_dump())
    return result