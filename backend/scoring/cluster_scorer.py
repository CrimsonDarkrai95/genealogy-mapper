# backend/scoring/cluster_scorer.py
import asyncio
import logging
import math
import os
import uuid
from itertools import combinations

import asyncpg
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
BATCH_SIZE = 500

UPSERT_MATCH_SQL = """
INSERT INTO genome_match (
    match_id, genome_a_id, genome_b_id,
    similarity_score, shared_segment_count, total_shared_cm,
    longest_segment_cm, snp_overlap_count,
    matching_method, confidence_score, computed_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pca', $9, NOW())
ON CONFLICT (genome_a_id, genome_b_id)
DO UPDATE SET
    similarity_score = GREATEST(genome_match.similarity_score, EXCLUDED.similarity_score),
    confidence_score = EXCLUDED.confidence_score,
    matching_method  = CASE
                           WHEN EXCLUDED.similarity_score > genome_match.similarity_score
                           THEN 'pca'
                           ELSE genome_match.matching_method
                       END,
    computed_at      = NOW();
"""


async def get_connection() -> asyncpg.Connection:
    return await asyncpg.connect(
        DATABASE_URL,
        ssl="require",
        statement_cache_size=0,
        timeout=30,
    )


def cosine_similarity(vec_a: dict, vec_b: dict) -> float:
    shared = set(vec_a.keys()) & set(vec_b.keys())
    if not shared:
        return 0.0

    dot = sum(vec_a[k] * vec_b[k] for k in shared)
    norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
    norm_b = math.sqrt(sum(v * v for v in vec_b.values()))

    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0

    return round(dot / (norm_a * norm_b), 6)


async def run(conn: asyncpg.Connection) -> int:
    log.info("cluster_scorer: fetching genome cluster assignments...")
    rows = await conn.fetch(
        """
        SELECT genome_id::TEXT, cluster_id::TEXT, membership_probability
        FROM genome_cluster_assignment
        WHERE membership_probability > 0
        """
    )

    if not rows:
        log.info("cluster_scorer: no cluster assignments found — exiting cleanly.")
        return 0

    genome_vectors = {}
    for r in rows:
        gid = r["genome_id"]
        if gid not in genome_vectors:
            genome_vectors[gid] = {}
        genome_vectors[gid][r["cluster_id"]] = float(r["membership_probability"])

    genome_ids = list(genome_vectors.keys())
    if len(genome_ids) < 2:
        log.info("cluster_scorer: fewer than 2 genomes with cluster data — exiting cleanly.")
        return 0

    log.info(f"cluster_scorer: {len(genome_ids)} genomes with cluster vectors. Computing pairs...")

    batch = []
    pairs_processed = 0

    for gid_a, gid_b in combinations(genome_ids, 2):
        score = cosine_similarity(genome_vectors[gid_a], genome_vectors[gid_b])
        if score == 0.0:
            continue

        confidence = round(score * 0.7, 4)
        a_sorted, b_sorted = sorted([gid_a, gid_b])

        batch.append((
            str(uuid.uuid4()),
            a_sorted,
            b_sorted,
            score,
            0,
            0.0,
            0.0,
            0,
            confidence,
        ))
        pairs_processed += 1

        if len(batch) >= BATCH_SIZE:
            await conn.executemany(UPSERT_MATCH_SQL, batch)
            log.info(f"cluster_scorer: flushed {len(batch)} pairs...")
            batch.clear()

    if batch:
        await conn.executemany(UPSERT_MATCH_SQL, batch)
        log.info(f"cluster_scorer: flushed final {len(batch)} pairs.")

    log.info(f"cluster_scorer: complete. {pairs_processed} pairs written.")
    return pairs_processed


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