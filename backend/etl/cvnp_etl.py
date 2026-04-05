"""
ETL: person records from cross-verified-database.csv (CVNP)
Targets: person, person_location_timeline tables
Must run after: age_dataset_etl.py (person table partially populated)

Usage:
    python backend/etl/cvnp_etl.py
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

CSV_PATH = r"C:\Users\GOVIND\Desktop\Dataset only\cross-verified-database.csv"

TEST_MODE  = True
TEST_LIMIT = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_gender(val) -> str:
    if not val or str(val).strip().lower() in ("", "nan"):
        return "unknown"
    val = str(val).strip().lower()
    if val in ("f", "female"):
        return "female"
    if val in ("m", "male"):
        return "male"
    return "other"


def parse_year(val) -> int | None:
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:
            return None
        return int(f)
    except (ValueError, TypeError):
        return None


def resolve_region(val: str | None, region_map: dict[str, int]) -> int | None:
    if not val or str(val).strip().lower() in ("", "nan"):
        return None
    val = str(val).strip()
    if val in region_map:
        return region_map[val]
    val_lower = val.lower()
    for rname, rid in region_map.items():
        if rname.lower() in val_lower or val_lower in rname.lower():
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
        updated_at      = EXCLUDED.updated_at
    RETURNING person_id, wikidata_qid
"""

INSERT_LOCATION_SQL = """
    INSERT INTO person_location_timeline (
        person_id,
        region_id,
        event_type,
        event_year,
        certainty,
        source_dataset_id
    )
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT DO NOTHING
"""

# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

async def run_etl(conn: asyncpg.Connection) -> None:
    now = datetime.now(timezone.utc)

    # Resolve dataset_id
    row = await conn.fetchrow(
        "SELECT dataset_id FROM dataset_source WHERE short_code = $1", "CVNP"
    )
    if not row:
        raise RuntimeError("CVNP dataset_source row not found")
    dataset_id = row["dataset_id"]
    log.info("Resolved CVNP dataset_id=%d", dataset_id)

    # Load region lookup
    regions = await conn.fetch("SELECT region_id, region_name FROM region")
    region_map = {r["region_name"]: r["region_id"] for r in regions}
    log.info("Loaded %d regions", len(region_map))

    # Load CSV
    log.info("Loading CSV: %s", CSV_PATH)
    df = pd.read_csv(CSV_PATH, dtype=str, low_memory=False, encoding="latin-1")
    log.info("CSV loaded — %d total rows", len(df))

    if TEST_MODE:
        df = df.head(TEST_LIMIT)
        log.info("TEST_MODE — capped to %d rows", len(df))

    # Filter rows missing name or wikidata code
    df = df.dropna(subset=["name", "wikidata_code"])
    log.info("After filtering — %d rows to process", len(df))

    person_upserted = 0
    location_inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        qid       = str(row["wikidata_code"]).strip()
        full_name = str(row["name"]).strip()
        gender    = normalize_gender(row.get("gender"))
        birth_year = parse_year(row.get("birth"))
        death_year = parse_year(row.get("death"))

        # Try citizenship_1_b first, fall back to un_region
        country = str(row.get("citizenship_1_b", "")).strip()
        if country.lower() in ("", "nan"):
            country = str(row.get("un_region", "")).strip()
        region_id = resolve_region(country, region_map)

        is_historical = True  # CVNP is all historical persons

        try:
            result = await conn.fetchrow(
                INSERT_PERSON_SQL,
                full_name,
                None,
                is_historical,
                birth_year,
                death_year,
                region_id,
                gender,
                qid,
                None,   # bio_text — people_wiki ETL adds this later
                dataset_id,
                now,
            )
            person_id = result["person_id"]
            person_upserted += 1

            # Insert birth location event if region resolved and birth year exists
            if region_id and birth_year:
                try:
                    await conn.execute(
                        INSERT_LOCATION_SQL,
                        person_id,
                        region_id,
                        "birth",
                        birth_year,
                        "estimated",
                        dataset_id,
                    )
                    location_inserted += 1
                except Exception:
                    pass  # Non-fatal

            # Insert death location event if region resolved and death year exists
            if region_id and death_year:
                try:
                    await conn.execute(
                        INSERT_LOCATION_SQL,
                        person_id,
                        region_id,
                        "death",
                        death_year,
                        "estimated",
                        dataset_id,
                    )
                    location_inserted += 1
                except Exception:
                    pass  # Non-fatal

        except Exception as e:
            log.warning("Failed QID=%s (%s): %s", qid, full_name, e)
            skipped += 1

    log.info(
        "CVNP ETL complete — %d persons upserted, %d location events inserted, %d skipped",
        person_upserted, location_inserted, skipped,
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