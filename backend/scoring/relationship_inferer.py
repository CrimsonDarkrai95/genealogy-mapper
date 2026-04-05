# backend/scoring/relationship_inferrer.py
import asyncio
import json
import logging
import math
import os
import uuid

import asyncpg
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
BATCH_SIZE = 500

FETCH_MATCHES_SQL = """
SELECT
    gm.match_id::TEXT,
    gm.genome_a_id::TEXT,
    gm.genome_b_id::TEXT,
    gm.similarity_score,
    gm.matching_method,
    gm.confidence_score,
    gm.snp_overlap_count,
    ga.person_id::TEXT AS person_a_id,
    gb.person_id::TEXT AS person_b_id
FROM genome_match gm
JOIN genome ga ON ga.genome_id = gm.genome_a_id
JOIN genome gb ON gb.genome_id = gm.genome_b_id
WHERE ga.person_id IS NOT NULL
  AND gb.person_id IS NOT NULL
  AND ga.person_id <> gb.person_id
"""

UPSERT_RELATIONSHIP_SQL = """
INSERT INTO inferred_relationship (
    relationship_id,
    person_a_id,
    person_b_id,
    match_id,
    relationship_type,
    generational_distance,
    confidence_level,
    inference_method,
    supporting_evidence,
    inferred_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
ON CONFLICT (person_a_id, person_b_id)
DO UPDATE SET
    relationship_type     = EXCLUDED.relationship_type,
    generational_distance = EXCLUDED.generational_distance,
    confidence_level      = EXCLUDED.confidence_level,
    inference_method      = EXCLUDED.inference_method,
    supporting_evidence   = EXCLUDED.supporting_evidence,
    match_id              = EXCLUDED.match_id,
    inferred_at           = NOW()
WHERE EXCLUDED.confidence_level > inferred_relationship.confidence_level;
"""

METHOD_LABEL = {
    "ibd": "ibd_segment",
    "haplogroup": "haplogroup_tree",
    "pca": "pca_proximity",
}


def classify_match(
    similarity_score: float,
    matching_method: str,
    confidence_score: float,
    snp_overlap_count: int,
) -> tuple:
    """
    Returns (relationship_type, generational_distance, confidence_level, supporting_evidence).
    Returns (None, None, None, {}) if score is below any meaningful threshold.
    """
    evidence = {
        "method": matching_method,
        "similarity_score": similarity_score,
        "snp_overlap_count": snp_overlap_count,
        "confidence_input": confidence_score,
    }

    if matching_method == "haplogroup":
        estimated_depth = round((1.0 / max(similarity_score, 0.01)) - 1)
        gen_dist = max(1, estimated_depth)
        confidence = round(confidence_score * 0.85, 4)
        evidence["lca_depth_estimate"] = estimated_depth
        return "haplogroup_shared", gen_dist, confidence, evidence

    if matching_method == "pca":
        confidence = round(similarity_score * 0.6, 4)
        return "population_cluster", 0, confidence, evidence

    # ibd method
    if similarity_score >= 0.95:
        confidence = round(min(0.95, confidence_score + 0.1), 4)
        return "sibling", 1, confidence, evidence

    if similarity_score >= 0.70:
        return "cousin", 2, round(confidence_score, 4), evidence

    if similarity_score >= 0.40:
        gen_dist = max(3, round(math.log(similarity_score) / math.log(0.5)))
        confidence = round(confidence_score * 0.8, 4)
        return "distant_relative", gen_dist, confidence, evidence

    return None, None, None, {}


async def get_connection() -> asyncpg.Connection:
    return await asyncpg.connect(
        DATABASE_URL,
        ssl="require",
        statement_cache_size=0,
        timeout=30,
    )


async def run(conn: asyncpg.Connection) -> int:
    log.info("relationship_inferrer: fetching genome_match rows...")
    rows = await conn.fetch(FETCH_MATCHES_SQL)

    if not rows:
        log.info("relationship_inferrer: no genome_match rows found — run scorers first. Exiting cleanly.")
        return 0

    log.info(f"relationship_inferrer: {len(rows)} match rows loaded. Classifying...")

    batch = []
    pairs_written = 0
    pairs_skipped = 0

    for r in rows:
        rel_type, gen_dist, confidence, evidence = classify_match(
            similarity_score=float(r["similarity_score"]),
            matching_method=r["matching_method"],
            confidence_score=float(r["confidence_score"]),
            snp_overlap_count=r["snp_overlap_count"] or 0,
        )

        if rel_type is None:
            pairs_skipped += 1
            continue

        pid_a, pid_b = sorted([r["person_a_id"], r["person_b_id"]])
        inference_method = METHOD_LABEL.get(r["matching_method"], "rule_based")

        batch.append((
            str(uuid.uuid4()),
            pid_a,
            pid_b,
            r["match_id"],
            rel_type,
            gen_dist,
            confidence,
            inference_method,
            json.dumps(evidence),
        ))
        pairs_written += 1

        if len(batch) >= BATCH_SIZE:
            await conn.executemany(UPSERT_RELATIONSHIP_SQL, batch)
            log.info(f"relationship_inferrer: flushed {len(batch)} relationships...")
            batch.clear()

    if batch:
        await conn.executemany(UPSERT_RELATIONSHIP_SQL, batch)
        log.info(f"relationship_inferrer: flushed final {len(batch)} relationships.")

    log.info(
        f"relationship_inferrer: complete. "
        f"{pairs_written} relationships written, {pairs_skipped} pairs below threshold."
    )
    return pairs_written


async def main():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set in environment.")
    conn = await get_connection()
    try:
        await run(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())