# backend/scoring/haplogroup_scorer.py
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

ANCESTOR_PATH_SQL = """
WITH RECURSIVE ancestors AS (
    SELECT haplogroup_id, haplogroup_code, parent_haplogroup_id, 0 AS depth
    FROM haplogroup_reference
    WHERE haplogroup_code = $1

    UNION ALL

    SELECT h.haplogroup_id, h.haplogroup_code, h.parent_haplogroup_id, a.depth + 1
    FROM haplogroup_reference h
    JOIN ancestors a ON h.haplogroup_id = a.parent_haplogroup_id
)
SELECT haplogroup_id, haplogroup_code, depth
FROM ancestors
ORDER BY depth ASC;
"""

UPSERT_MATCH_SQL = """
INSERT INTO genome_match (
    match_id, genome_a_id, genome_b_id,
    similarity_score, shared_segment_count, total_shared_cm,
    longest_segment_cm, snp_overlap_count,
    matching_method, confidence_score, computed_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'haplogroup', $9, NOW())
ON CONFLICT (genome_a_id, genome_b_id)
DO UPDATE SET
    similarity_score = GREATEST(genome_match.similarity_score, EXCLUDED.similarity_score),
    confidence_score = EXCLUDED.confidence_score,
    matching_method  = CASE
                           WHEN EXCLUDED.similarity_score > genome_match.similarity_score
                           THEN 'haplogroup'
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


async def fetch_ancestor_path(conn: asyncpg.Connection, haplogroup_code: str) -> list:
    rows = await conn.fetch(ANCESTOR_PATH_SQL, haplogroup_code)
    return [dict(r) for r in rows]


def compute_lca(path_a: list, path_b: list) -> tuple:
    """
    Returns (lca_code, combined_depth).
    combined_depth = sum of depths from each leaf to the LCA node.
    Returns (None, 999) if no common ancestor exists.
    """
    ids_a = {node["haplogroup_id"]: node["depth"] for node in path_a}
    for node in path_b:
        if node["haplogroup_id"] in ids_a:
            return node["haplogroup_code"], ids_a[node["haplogroup_id"]] + node["depth"]
    return None, 999


async def run(conn: asyncpg.Connection) -> int:
    log.info("haplogroup_scorer: fetching genomes...")
    rows = await conn.fetch(
        """
        SELECT genome_id, y_haplogroup, mt_haplogroup
        FROM genome
        WHERE y_haplogroup IS NOT NULL OR mt_haplogroup IS NOT NULL
        """
    )

    if len(rows) < 2:
        log.info("haplogroup_scorer: fewer than 2 eligible genomes — exiting cleanly.")
        return 0

    genomes = [dict(r) for r in rows]
    log.info(f"haplogroup_scorer: {len(genomes)} genomes loaded, computing pairs...")

    path_cache = {}

    async def get_path(code: str) -> list:
        if code not in path_cache:
            path_cache[code] = await fetch_ancestor_path(conn, code)
        return path_cache[code]

    pairs_processed = 0
    batch = []
    BATCH_SIZE = 500

    for g_a, g_b in combinations(genomes, 2):
        best_score = 0.0
        best_confidence = 0.0

        for hap_type in ("y_haplogroup", "mt_haplogroup"):
            code_a = g_a.get(hap_type)
            code_b = g_b.get(hap_type)
            if not code_a or not code_b:
                continue

            path_a = await get_path(code_a)
            path_b = await get_path(code_b)

            if not path_a or not path_b:
                continue

            lca_code, lca_depth = compute_lca(path_a, path_b)
            if lca_code is None:
                continue

            score = round(1.0 / (1.0 + lca_depth), 6)
            confidence = 0.8 if lca_depth == 0 else max(0.3, round(0.8 / (1.0 + lca_depth * 0.5), 4))

            if score > best_score:
                best_score = score
                best_confidence = confidence

        if best_score == 0.0:
            continue

        gid_a, gid_b = sorted([str(g_a["genome_id"]), str(g_b["genome_id"])])

        batch.append((
            str(uuid.uuid4()),
            gid_a,
            gid_b,
            best_score,
            0,
            0.0,
            0.0,
            0,
            best_confidence,
        ))
        pairs_processed += 1

        if len(batch) >= BATCH_SIZE:
            await conn.executemany(UPSERT_MATCH_SQL, batch)
            log.info(f"haplogroup_scorer: flushed {len(batch)} pairs...")
            batch.clear()

    if batch:
        await conn.executemany(UPSERT_MATCH_SQL, batch)
        log.info(f"haplogroup_scorer: flushed final {len(batch)} pairs.")

    log.info(f"haplogroup_scorer: complete. {pairs_processed} pairs written to genome_match.")
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