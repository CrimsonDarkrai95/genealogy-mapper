"""
ETL: genome and genome_snp synthetic seed
Creates 300 genome records linked to existing persons.
Creates ~22,500 genome_snp observations (75 SNPs per genome).
Must run after: person, snp_marker, haplogroup_reference, dataset_source

Usage:
    python backend/etl/genome_etl.py
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

random.seed(42)  # Reproducible results

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# Y-DNA and mtDNA haplogroup assignments by region
# Maps region_name patterns to likely haplogroups
YDNA_BY_REGION = {
    "Europe":        ["R1b", "R1a", "I1", "I2", "G2a", "J2"],
    "Western":       ["R1b", "I1", "G2a"],
    "Eastern":       ["R1a", "I2", "N1c"],
    "Scandinavia":   ["I1", "R1b", "R1a"],
    "British":       ["R1b", "I1"],
    "Middle East":   ["J1", "J2", "G2a", "E1b1b1a"],
    "South Asia":    ["R1a1", "H1", "L1", "R2"],
    "East Asia":     ["O2", "O1", "C2", "N1"],
    "Africa":        ["E1b1a", "E1b1b", "A", "B"],
    "Central Asia":  ["R1a", "C2", "Q1a", "N1"],
    "Americas":      ["Q1a", "C2"],
    "default":       ["R1b", "R1a", "J2", "E1b1b", "I1"],
}

MTDNA_BY_REGION = {
    "Europe":        ["H", "H1", "H3", "U5", "U5b", "K", "J1", "T2"],
    "Western":       ["H", "H1", "H3", "V", "U5"],
    "Eastern":       ["H", "U4", "U5a", "T2", "K"],
    "Scandinavia":   ["H1", "U5b", "V", "I"],
    "British":       ["H", "H1", "U5b", "K"],
    "Middle East":   ["J1", "J2", "T1", "U1", "H"],
    "South Asia":    ["M2", "U2", "R", "H"],
    "East Asia":     ["D", "B4", "M7", "A"],
    "Africa":        ["L1", "L2", "L3", "L0"],
    "Central Asia":  ["D", "C", "U4", "A"],
    "Americas":      ["A", "B2", "C", "D", "X2"],
    "default":       ["H", "U5", "K", "J1", "T2"],
}

DAMAGE_PATTERNS = ["high", "moderate", "low", "none"]
ASSEMBLY_REFS   = ["GRCh38", "hg19"]
GENOTYPES       = ["A/A", "A/G", "G/G", "C/T", "T/T", "C/C", "A/T", "G/C"]

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

INSERT_GENOME_SQL = """
    INSERT INTO genome (
        person_id,
        dataset_id,
        y_haplogroup,
        mt_haplogroup,
        coverage_depth,
        coverage_breadth,
        endogenous_dna_pct,
        damage_pattern,
        assembly_reference,
        raw_metadata,
        ingested_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ON CONFLICT DO NOTHING
    RETURNING genome_id
"""

INSERT_GENOME_SNP_SQL = """
    INSERT INTO genome_snp (
        genome_id,
        snp_id,
        observed_genotype,
        quality_score,
        is_derived
    )
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (genome_id, snp_id) DO NOTHING
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def pick_haplogroup(region_name: str | None, hmap: dict) -> str:
    if not region_name:
        return random.choice(hmap["default"])
    region_lower = str(region_name).lower()
    for key, haplogroups in hmap.items():
        if key.lower() in region_lower:
            return random.choice(haplogroups)
    return random.choice(hmap["default"])


# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

async def run_etl(conn: asyncpg.Connection) -> None:
    now = datetime.now(timezone.utc)

    # Resolve dataset_id for AADR
    row = await conn.fetchrow(
        "SELECT dataset_id FROM dataset_source WHERE short_code = $1", "AADR"
    )
    if not row:
        raise RuntimeError("AADR dataset_source row not found")
    dataset_id = row["dataset_id"]
    log.info("Resolved AADR dataset_id=%d", dataset_id)

    # Load 300 historical persons
    persons = await conn.fetch("""
        SELECT p.person_id, p.birth_year, r.region_name
        FROM person p
        LEFT JOIN region r ON r.region_id = p.birth_region_id
        WHERE p.is_historical = true
        LIMIT 300
    """)
    log.info("Loaded %d persons for genome assignment", len(persons))

    # Load all SNP marker IDs
    snps = await conn.fetch("SELECT snp_id FROM snp_marker")
    snp_ids = [str(row["snp_id"]) for row in snps]
    log.info("Loaded %d SNP markers", len(snp_ids))

    if not snp_ids:
        raise RuntimeError("No SNP markers found — run snp_marker_etl.py first")

    genome_count  = 0
    snp_obs_count = 0

    for person in persons:
        person_id   = str(person["person_id"])
        region_name = person["region_name"]
        birth_year  = person["birth_year"]

        # Assign haplogroups based on region
        y_hap  = pick_haplogroup(region_name, YDNA_BY_REGION)
        mt_hap = pick_haplogroup(region_name, MTDNA_BY_REGION)

        # Ancient samples have damage; recent samples don't
        is_ancient = birth_year is not None and birth_year < 1800
        damage     = random.choice(["high", "moderate"]) if is_ancient else "none"
        endo_pct   = round(random.uniform(0.05, 0.85), 4) if is_ancient else round(random.uniform(0.85, 0.99), 4)
        coverage_d = round(random.uniform(0.5, 15.0), 2) if is_ancient else round(random.uniform(10.0, 40.0), 2)
        coverage_b = round(random.uniform(0.3, 0.85), 4) if is_ancient else round(random.uniform(0.85, 0.99), 4)

        try:
            result = await conn.fetchrow(
                INSERT_GENOME_SQL,
                person_id,
                dataset_id,
                y_hap,
                mt_hap,
                coverage_d,
                coverage_b,
                endo_pct,
                damage,
                random.choice(ASSEMBLY_REFS),
                '{"source": "synthetic_test"}',
                now,
            )

            if not result:
                continue  # Already exists

            genome_id = str(result["genome_id"])
            genome_count += 1

            # Assign 75 random SNP observations per genome
            selected_snps = random.sample(snp_ids, min(75, len(snp_ids)))
            for snp_id in selected_snps:
                try:
                    await conn.execute(
                        INSERT_GENOME_SNP_SQL,
                        genome_id,
                        snp_id,
                        random.choice(GENOTYPES),
                        round(random.uniform(20.0, 60.0), 2),
                        random.choice([True, False]),
                    )
                    snp_obs_count += 1
                except Exception:
                    pass  # Skip duplicate SNP observations silently

        except Exception as e:
            log.warning("Failed person_id=%s: %s", person_id, e)

    log.info(
        "genome ETL complete — %d genomes created, %d SNP observations inserted",
        genome_count, snp_obs_count
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