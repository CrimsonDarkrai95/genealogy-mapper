# backend/scoring/snp_scorer.py
import asyncio
import logging
import os
import uuid
from itertools import combinations

import asyncpg
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

MIN_SHARED_SNPS = 10
BATCH_SIZE = 200
GENOME_FETCH_LIMIT = 500

UPSERT_MATCH_SQL = """
INSERT INTO genome_match (
    match_id, genome_a_id, genome_b_id,
    similarity_score, shared_segment_count, total_shared_cm,
    longest_segment_cm, snp_overlap_count,
    matching_method, confidence_score, computed_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'ibd', $9, NOW())
ON CONFLICT (genome_a_id, genome_b_id)
DO UPDATE SET
    similarity_score  = GREATEST(genome_match.similarity_score, EXCLUDED.similarity_score),
    snp_overlap_count = EXCLUDED.snp_overlap_count,
    confidence_score  = EXCLUDED.confidence_score,
    matching_method   = CASE
                            WHEN EXCLUDED.similarity_score > genome_match.similarity_score
                            THEN 'ibd'
                            ELSE genome_match.matching_method
                        END,
    computed_at       = NOW();
"""

SNP_FETCH_SQL = """
SELECT snp_id::TEXT, observed_genotype
FROM genome_snp
WHERE genome_id = $1
"""


async def get_connection() -> asyncpg.Connection:
    return await asyncpg.connect(
        DATABASE_URL,
        ssl="require",
        statement_cache_size=0,
        timeout=30,
    )


async def fetch_snp_profile(conn: asyncpg.Connection, genome_id: str) -> dict:
    rows = await conn.fetch(SNP_FETCH_SQL, genome_id)
    return {str(r["snp_id"]): r["observed_genotype"] for r in rows}


def score_snp_pair(profile_a: dict, profile_b: dict):
    """
    Returns (score, snp_overlap_count, confidence) or None if overlap < MIN_SHARED_SNPS.
    """
    shared_snps = set(profile_a.keys()) & set(profile_b.keys())
    if len(shared_snps) < MIN_SHARED_SNPS:
        return None

    agreed = sum(1 for snp in shared_snps if profile_a[snp] == profile_b[snp])
    score = round(agreed / len(shared_snps), 6)
    confidence = round(min(0.95, len(shared_snps) / 1000), 4)
    return score, len(shared_snps), confidence


async def run(conn: asyncpg.Connection) -> int:
    log.info("snp_scorer: fetching genome IDs with SNP data...")
    rows = await conn.fetch(
        """
        SELECT DISTINCT genome_id::TEXT
        FROM genome_snp
        LIMIT $1
        """,
        GENOME_FETCH_LIMIT,
    )

    if len(rows) < 2:
        log.info("snp_scorer: fewer than 2 genomes with SNP data — exiting cleanly.")
        return 0

    genome_ids = [r["genome_id"] for r in rows]
    log.info(f"snp_scorer: {len(genome_ids)} genomes with SNP data. Loading profiles...")

    profiles = {}
    for gid in genome_ids:
        profiles[gid] = await fetch_snp_profile(conn, gid)

    log.info("snp_scorer: profiles loaded. Computing pairs...")

    batch = []
    pairs_processed = 0

    for gid_a, gid_b in combinations(genome_ids, 2):
        result = score_snp_pair(profiles[gid_a], profiles[gid_b])
        if result is None:
            continue

        score, overlap_count, confidence = result
        a_sorted, b_sorted = sorted([gid_a, gid_b])

        batch.append((
            str(uuid.uuid4()),
            a_sorted,
            b_sorted,
            score,
            0,
            0.0,
            0.0,
            overlap_count,
            confidence,
        ))
        pairs_processed += 1

        if len(batch) >= BATCH_SIZE:
            await conn.executemany(UPSERT_MATCH_SQL, batch)
            log.info(f"snp_scorer: flushed {len(batch)} pairs...")
            batch.clear()

    if batch:
        await conn.executemany(UPSERT_MATCH_SQL, batch)
        log.info(f"snp_scorer: flushed final {len(batch)} pairs.")

    log.info(f"snp_scorer: complete. {pairs_processed} pairs written.")
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