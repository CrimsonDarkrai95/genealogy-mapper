"""
ETL: person records from AgeDataset-V1.csv
Targets: person, person_external_id tables
Must run after: dataset_source_etl.py, region_etl.py

Usage:
    python backend/etl/age_dataset_etl.py
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

import asyncpg
import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]

CSV_PATH = r"C:\Users\GOVIND\Desktop\Dataset only\AgeDataset-V1.csv"

TEST_MODE  = True
TEST_LIMIT = 3000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_gender(val: str | None) -> str:
    if not val or str(val).strip().lower() in ("", "nan"):
        return "unknown"
    val = str(val).strip().lower()
    if val == "female":
        return "female"
    if val == "male":
        return "male"
    return "other"


def parse_year(val) -> int | None:
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN check
            return None
        return int(f)
    except (ValueError, TypeError):
        return None


def resolve_region(country: str | None, region_map: dict[str, int]) -> int | None:
    if not country or str(country).strip().lower() in ("", "nan"):
        return None
    country = str(country).strip()
    # Exact match first
    if country in region_map:
        return region_map[country]
    # Partial match — check if any known region name appears in country string
    country_lower = country.lower()
    for region_name, rid in region_map.items():
        if region_name.lower() in country_lower or country_lower in region_name.lower():
            return rid
    return None

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

INSERT_PERSON_SQL = """
    INSERT INTO person (
        full_name,
        birth_name,
        is_historical,
        birth_year,
        death_year,
        birth_region_id,
        gender,
        wikidata_qid,
        bio_text,
        source_dataset_id,
        created_at,
        updated_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)
    ON CONFLICT (wikidata_qid) DO UPDATE SET
        full_name       = EXCLUDED.full_name,
        birth_year      = EXCLUDED.birth_year,
        death_year      = EXCLUDED.death_year,
        birth_region_id = EXCLUDED.birth_region_id,
        gender          = EXCLUDED.gender,
        bio_text        = EXCLUDED.bio_text,
        updated_at      = EXCLUDED.updated_at
    RETURNING person_id, wikidata_qid
"""

INSERT_EXT_ID_SQL = """
    INSERT INTO person_external_id (
        person_id,
        source_name,
        external_key,
        url
    )
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (person_id, source_name, external_key) DO NOTHING
"""

# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

async def run_etl(conn: asyncpg.Connection) -> None:
    now = datetime.now(timezone.utc)

    # Resolve dataset_id
    row = await conn.fetchrow(
        "SELECT dataset_id FROM dataset_source WHERE short_code = $1", "WIKIDATA"
    )
    if not row:
        raise RuntimeError("WIKIDATA dataset_source row not found")
    dataset_id = row["dataset_id"]
    log.info("Resolved dataset_id=%d", dataset_id)

    # Load region lookup
    regions = await conn.fetch("SELECT region_id, region_name FROM region")
    region_map = {r["region_name"]: r["region_id"] for r in regions}
    log.info("Loaded %d regions", len(region_map))

    # Load CSV
    log.info("Loading CSV: %s", CSV_PATH)
    df = pd.read_csv(CSV_PATH, dtype=str)
    log.info("CSV loaded — %d total rows", len(df))

    if TEST_MODE:
        df = df.head(TEST_LIMIT)
        log.info("TEST_MODE — capped to %d rows", len(df))

    # Filter out rows with no name or no QID
    df = df.dropna(subset=["Id", "Name"])
    df = df[df["Id"].str.startswith("Q", na=False)]
    log.info("After filtering — %d rows to process", len(df))

    person_upserted = 0
    ext_id_inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        qid       = str(row["Id"]).strip()
        full_name = str(row["Name"]).strip()
        bio_text  = str(row.get("Short description", "")).strip()
        bio_text  = None if bio_text.lower() in ("", "nan") else bio_text
        gender    = normalize_gender(row.get("Gender"))
        birth_year = parse_year(row.get("Birth year"))
        death_year = parse_year(row.get("Death year"))
        country   = str(row.get("Country", "")).strip()
        region_id = resolve_region(country, region_map)

        # Determine is_historical — anyone with death_year is historical
        is_historical = death_year is not None

        try:
            result = await conn.fetchrow(
                INSERT_PERSON_SQL,
                full_name,
                None,           # birth_name
                is_historical,
                birth_year,
                death_year,
                region_id,
                gender,
                qid,
                bio_text,
                dataset_id,
                now,
            )
            person_id = result["person_id"]
            person_upserted += 1

            # Insert external ID
            await conn.execute(
                INSERT_EXT_ID_SQL,
                person_id,
                "wikidata",
                qid,
                f"https://www.wikidata.org/wiki/{qid}",
            )
            ext_id_inserted += 1

        except Exception as e:
            log.warning("Failed QID=%s (%s): %s", qid, full_name, e)
            skipped += 1

    log.info(
        "Age Dataset ETL complete — %d persons upserted, %d external IDs inserted, %d skipped",
        person_upserted, ext_id_inserted, skipped,
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