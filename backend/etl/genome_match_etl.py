"""
ETL: genome_match and inferred_relationship synthetic seed
Creates 500 genome match pairs and 500 inferred relationships.
Includes deliberate multi-generation ancestry chains for 20 historical figures.
Must run after: genome_etl.py, person etl scripts

Usage:
    python backend/etl/genome_match_etl.py
"""

import asyncio
import logging
import os
import random
from datetime import datetime, timezone

import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]

random.seed(42)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

RELATIONSHIP_TYPES = [
    "ancestor", "descendant", "sibling",
    "cousin", "haplogroup_shared",
    "population_cluster", "distant_relative"
]

INFERENCE_METHODS = [
    "ibd_segment", "haplogroup_tree",
    "pca_proximity", "rule_based"
]

MATCHING_METHODS = ["ibd", "haplogroup", "pca", "admixture"]

INSERT_MATCH_SQL = """
    INSERT INTO genome_match (
        genome_a_id,
        genome_b_id,
        similarity_score,
        shared_segment_count,
        total_shared_cm,
        longest_segment_cm,
        snp_overlap_count,
        matching_method,
        confidence_score,
        computed_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (genome_a_id, genome_b_id) DO NOTHING
    RETURNING match_id
"""

INSERT_REL_SQL = """
    INSERT INTO inferred_relationship (
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
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT DO NOTHING
"""

# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

async def run_etl(conn: asyncpg.Connection) -> None:
    now = datetime.now(timezone.utc)

    # Load genomes with person IDs
    genomes = await conn.fetch("""
        SELECT genome_id, person_id
        FROM genome
        LIMIT 300
    """)
    log.info("Loaded %d genomes", len(genomes))

    if len(genomes) < 2:
        raise RuntimeError("Need at least 2 genomes — run genome_etl.py first")

    genome_list = [(str(g["genome_id"]), str(g["person_id"])) for g in genomes]

    match_count = 0
    rel_count   = 0

    # Generate 500 unique genome pairs
    pairs_seen = set()
    attempts   = 0

    while match_count < 500 and attempts < 5000:
        attempts += 1
        a, b = random.sample(genome_list, 2)
        genome_a_id, person_a_id = a
        genome_b_id, person_b_id = b

        # Ensure consistent ordering for UNIQUE constraint
        if genome_a_id > genome_b_id:
            genome_a_id, genome_b_id = genome_b_id, genome_a_id
            person_a_id, person_b_id = person_b_id, person_a_id

        pair_key = (genome_a_id, genome_b_id)
        if pair_key in pairs_seen:
            continue
        pairs_seen.add(pair_key)

        similarity    = round(random.uniform(0.05, 0.45), 6)
        shared_segs   = random.randint(1, 50)
        total_cm      = round(random.uniform(5.0, 800.0), 4)
        longest_cm    = round(random.uniform(5.0, total_cm), 4)
        snp_overlap   = random.randint(100, 1000)
        method        = random.choice(MATCHING_METHODS)
        confidence    = round(random.uniform(0.5, 0.99), 4)

        try:
            result = await conn.fetchrow(
                INSERT_MATCH_SQL,
                genome_a_id, genome_b_id,
                similarity, shared_segs, total_cm,
                longest_cm, snp_overlap, method,
                confidence, now
            )

            if not result:
                continue

            match_id = str(result["match_id"])
            match_count += 1

            # Infer relationship from similarity score
            if similarity > 0.35:
                rel_type = random.choice(["sibling", "ancestor", "descendant"])
                gen_dist = random.randint(1, 2)
            elif similarity > 0.20:
                rel_type = random.choice(["cousin", "ancestor", "descendant"])
                gen_dist = random.randint(2, 4)
            else:
                rel_type = random.choice(["distant_relative", "haplogroup_shared", "population_cluster"])
                gen_dist = random.randint(4, 10)

            await conn.execute(
                INSERT_REL_SQL,
                person_a_id, person_b_id,
                match_id,
                rel_type,
                gen_dist,
                confidence,
                random.choice(INFERENCE_METHODS),
                '{"method": "synthetic", "similarity": ' + str(similarity) + '}',
                now
            )
            rel_count += 1

        except Exception as e:
            log.warning("Failed pair: %s", e)

    # --- Generate deliberate multi-generation ancestry chains ---
    # Pick 20 persons and create 4-generation ancestor chains
    log.info("Generating multi-generation ancestry chains …")

    persons = await conn.fetch("SELECT person_id FROM person WHERE is_historical = true LIMIT 100")
    person_ids = [str(p["person_id"]) for p in persons]

    chain_count = 0
    for i in range(0, min(80, len(person_ids) - 4), 4):
        # Chain: person[i] → person[i+1] → person[i+2] → person[i+3]
        chain = person_ids[i:i+4]
        for gen, (pa, pb) in enumerate(zip(chain, chain[1:]), start=1):
            try:
                await conn.execute(
                    INSERT_REL_SQL,
                    pa, pb,
                    None,           # no match_id for chain relationships
                    "ancestor",
                    gen,
                    round(random.uniform(0.7, 0.95), 4),
                    "rule_based",
                    '{"chain": true, "generation": ' + str(gen) + '}',
                    now
                )
                chain_count += 1
            except Exception:
                pass

    log.info(
        "genome_match ETL complete — %d matches, %d relationships, %d chain links",
        match_count, rel_count, chain_count
    )


async def main() -> None:
    log.info("Connecting to database …")
    for attempt in range(1, 4):
        try:
            conn = await asyncpg.connect(
                DATABASE_URL,
                ssl="require",
                statement_cache_size=0,
                timeout=30,
            )
            break
        except Exception as e:
            log.warning("Attempt %d failed: %s", attempt, e)
            if attempt == 3:
                raise
            await asyncio.sleep(3)
    try:
        await run_etl(conn)
    finally:
        await conn.close()
        log.info("Connection closed.")


if __name__ == "__main__":
    asyncio.run(main())