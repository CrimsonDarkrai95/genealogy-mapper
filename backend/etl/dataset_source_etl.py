"""
ETL: dataset_source seed insert
Populates the provenance registry for all 10 ingestion sources.
Run this before any other ETL script — every downstream table references dataset_id.

Usage:
    python backend/etl/dataset_source_etl.py
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

import asyncpg
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Seed data
# All 10 source datasets. short_code values are used as stable references
# in downstream ETL scripts — do not change them after first insert.
# ---------------------------------------------------------------------------

DATASET_RECORDS = [
    {
        "dataset_name": "NCBI dbSNP",
        "short_code": "DBSNP",
        "description": (
            "NCBI's Single Nucleotide Polymorphism database. "
            "Provides rs_id, chromosome, GRCh38 position, ref/alt alleles, "
            "gene context, and population allele frequencies."
        ),
        "source_url": "https://ftp.ncbi.nih.gov/snp/latest_release/VCF/",
        "publication_doi": "10.1093/nar/gkv1099",
        "reliability_score": 0.98,
        "data_type": "genomic",
        "record_count_estimate": 10_000_000,
    },
    {
        "dataset_name": "1000 Genomes Project",
        "short_code": "1KG",
        "description": (
            "Phase 3 release of the 1000 Genomes Project. "
            "2,504 modern human genomes across 26 populations. "
            "Source for genome, genome_snp, population_cluster, "
            "and genome_cluster_assignment records."
        ),
        "source_url": "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/",
        "publication_doi": "10.1038/nature15393",
        "reliability_score": 0.97,
        "data_type": "genomic",
        "record_count_estimate": 2_504,
    },
    {
        "dataset_name": "Allen Ancient DNA Resource",
        "short_code": "AADR",
        "description": (
            "Ancient DNA compendium from the Reich Lab / Allen Institute. "
            "Covers ~10,000 ancient samples with haplogroup calls, "
            "coverage metrics, and geographic/temporal anchoring."
        ),
        "source_url": "https://reich.hms.harvard.edu/allen-ancient-dna-resource-aadr-downloadable-genotypes-present-day-and-ancient-dna-data",
        "publication_doi": "10.1126/science.aav4003",
        "reliability_score": 0.95,
        "data_type": "genomic",
        "record_count_estimate": 10_000,
    },
    {
        "dataset_name": "YFull YTree / ISOGG Y-DNA Haplogroup Tree",
        "short_code": "YFULL",
        "description": (
            "Curated Y-DNA and mtDNA haplogroup reference tree from YFull and ISOGG. "
            "Provides parent-child haplogroup relationships, defining SNPs, "
            "and age estimates in years before present."
        ),
        "source_url": "https://www.yfull.com/tree/",
        "publication_doi": None,
        "reliability_score": 0.90,
        "data_type": "haplogroup",
        "record_count_estimate": 5_000,
    },
    {
        "dataset_name": "Wikidata SPARQL API",
        "short_code": "WIKIDATA",
        "description": (
            "Wikidata knowledge graph queried via SPARQL endpoint. "
            "Source for historical persons, aliases, external IDs, "
            "dynasty definitions, dynasty memberships, and parent/child relationships "
            "via P22, P25, P40 properties."
        ),
        "source_url": "https://query.wikidata.org/sparql",
        "publication_doi": "10.1145/2740908.2742035",
        "reliability_score": 0.82,
        "data_type": "biographical",
        "record_count_estimate": 50_000,
    },
    {
        "dataset_name": "Kaggle Wikipedia People Dataset",
        "short_code": "KAGGLE_WIKI",
        "description": (
            "Wikipedia-derived biographical dataset from Kaggle covering ~800,000 "
            "notable people. Provides full_name, birth_name, and bio_text for FTS indexing."
        ),
        "source_url": "https://www.kaggle.com/datasets/sameersmahajan/people-wikipedia-dataset",
        "publication_doi": None,
        "reliability_score": 0.75,
        "data_type": "biographical",
        "record_count_estimate": 800_000,
    },
    {
        "dataset_name": "Cross-verified Database of Notable People",
        "short_code": "CVNP",
        "description": (
            "Cross-verified database of ~3,500 notable historical persons "
            "covering 3500 BC to 2018 AD. Provides birth/death years, "
            "birth regions, and location timeline events."
        ),
        "source_url": "https://www.science.org/doi/10.1126/science.aas9/",
        "publication_doi": "10.1126/science.aas9521",
        "reliability_score": 0.88,
        "data_type": "biographical",
        "record_count_estimate": 3_500,
    },
    {
        "dataset_name": "Genographic Project Population Clusters",
        "short_code": "GENOGRAPHIC",
        "description": (
            "National Geographic Genographic Project population cluster definitions. "
            "Provides cluster codes, haplogroup signatures, geographic centroids, "
            "and time period ranges for ancestry population groupings."
        ),
        "source_url": "https://genographic.nationalgeographic.com/",
        "publication_doi": "10.1371/journal.pgen.1000072",
        "reliability_score": 0.87,
        "data_type": "genomic",
        "record_count_estimate": 50,
    },
    {
        "dataset_name": "H-DATA Historical Conflicts Dataset",
        "short_code": "HDATA",
        "description": (
            "Historical conflict dataset covering wars, civil wars, battles, "
            "and campaigns with start/end years, region, and faction identifiers. "
            "Approximately 2,500 conflict records."
        ),
        "source_url": "https://h-data.org/",
        "publication_doi": None,
        "reliability_score": 0.83,
        "data_type": "conflict",
        "record_count_estimate": 2_500,
    },
    {
        "dataset_name": "OpenDataBay Historical Conflicts CSV",
        "short_code": "OPENDATABAY",
        "description": (
            "Open-licensed historical conflict CSV from OpenDataBay. "
            "Covers ~3,300 conflict records with conflict name, "
            "start/end years, and region identifiers."
        ),
        "source_url": "https://opendatabay.com/data/humanities/historical-conflicts",
        "publication_doi": None,
        "reliability_score": 0.78,
        "data_type": "conflict",
        "record_count_estimate": 3_300,
    },
]

# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

INSERT_SQL = """
    INSERT INTO dataset_source (
        dataset_name,
        short_code,
        description,
        source_url,
        publication_doi,
        reliability_score,
        data_type,
        record_count_estimate,
        ingested_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (short_code) DO UPDATE SET
        dataset_name          = EXCLUDED.dataset_name,
        description           = EXCLUDED.description,
        source_url            = EXCLUDED.source_url,
        publication_doi       = EXCLUDED.publication_doi,
        reliability_score     = EXCLUDED.reliability_score,
        data_type             = EXCLUDED.data_type,
        record_count_estimate = EXCLUDED.record_count_estimate,
        last_refreshed_at     = now()
    RETURNING dataset_id, short_code
"""


async def seed_dataset_source(conn: asyncpg.Connection) -> None:
    now = datetime.now(timezone.utc)
    inserted = 0
    updated = 0

    for record in DATASET_RECORDS:
        row = await conn.fetchrow(
            INSERT_SQL,
            record["dataset_name"],
            record["short_code"],
            record["description"],
            record["source_url"],
            record.get("publication_doi"),
            record["reliability_score"],
            record["data_type"],
            record["record_count_estimate"],
            now,
        )
        # asyncpg returns the row regardless of insert vs update;
        # check whether ingested_at was just set (rough heuristic for logging).
        log.info("Upserted dataset_id=%s  short_code=%s", row["dataset_id"], row["short_code"])
        inserted += 1

    log.info("dataset_source seed complete — %d records upserted.", inserted)


async def main() -> None:
    log.info("Connecting to database …")
    for attempt in range(1, 4):
        try:
            conn = await asyncpg.connect(DATABASE_URL, ssl="require", statement_cache_size=0)
            break
        except Exception as e:
            log.warning("Attempt %d failed: %s", attempt, e)
            if attempt == 3:
                raise
            await asyncio.sleep(3)
    try:
        await seed_dataset_source(conn)
    finally:
        await conn.close()
        log.info("Connection closed.")


if __name__ == "__main__":
    asyncio.run(main())