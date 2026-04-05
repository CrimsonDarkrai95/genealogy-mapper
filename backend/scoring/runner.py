# backend/scoring/runner.py
import argparse
import asyncio
import logging
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
VALID_METHODS = {"haplogroup", "snp", "cluster", "infer", "all"}


async def get_connection() -> asyncpg.Connection:
    return await asyncpg.connect(
        DATABASE_URL,
        ssl="require",
        statement_cache_size=0,
        timeout=30,
    )


async def run_all():
    conn = await get_connection()
    try:
        log.info("=== Phase 7 Runner: START ===")

        log.info("--- Stage 1/4: haplogroup_scorer ---")
        from backend.scoring import haplogroup_scorer
        n1 = await haplogroup_scorer.run(conn)

        log.info("--- Stage 2/4: snp_scorer ---")
        from backend.scoring import snp_scorer
        n2 = await snp_scorer.run(conn)

        log.info("--- Stage 3/4: cluster_scorer ---")
        from backend.scoring import cluster_scorer
        n3 = await cluster_scorer.run(conn)

        log.info("--- Stage 4/4: relationship_inferrer ---")
        from backend.scoring import relationship_inferrer
        n4 = await relationship_inferrer.run(conn)

        log.info("=== Phase 7 Runner: COMPLETE ===")
        log.info(f"  genome_match rows written: haplogroup={n1}, snp={n2}, cluster={n3}")
        log.info(f"  inferred_relationship rows written: {n4}")
    finally:
        await conn.close()


async def run_single(method: str):
    conn = await get_connection()
    try:
        if method == "haplogroup":
            from backend.scoring import haplogroup_scorer
            n = await haplogroup_scorer.run(conn)
            log.info(f"haplogroup scorer complete: {n} pairs written.")
        elif method == "snp":
            from backend.scoring import snp_scorer
            n = await snp_scorer.run(conn)
            log.info(f"snp scorer complete: {n} pairs written.")
        elif method == "cluster":
            from backend.scoring import cluster_scorer
            n = await cluster_scorer.run(conn)
            log.info(f"cluster scorer complete: {n} pairs written.")
        elif method == "infer":
            from backend.scoring import relationship_inferrer
            n = await relationship_inferrer.run(conn)
            log.info(f"relationship inferrer complete: {n} relationships written.")
    finally:
        await conn.close()


def main():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set in environment.")

    parser = argparse.ArgumentParser(description="Phase 7 scoring runner")
    parser.add_argument(
        "--method",
        choices=list(VALID_METHODS),
        required=True,
        help="Which scorer to run. Use 'all' to run all in dependency order.",
    )
    args = parser.parse_args()

    if args.method == "all":
        asyncio.run(run_all())
    else:
        asyncio.run(run_single(args.method))


if __name__ == "__main__":
    main()