"""
ETL: biographical text from people_wiki.csv
Enriches existing person records with full bio_text for FTS indexing.
Matches on name — updates bio_text only, does not create new persons.
Targets: person table (bio_text column only)

Usage:
    python backend/etl/people_wiki_etl.py
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

CSV_PATH = r"C:\Users\GOVIND\Desktop\Dataset only\people_wiki.csv"

TEST_MODE  = True
TEST_LIMIT = 1000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

UPDATE_BIO_SQL = """
    UPDATE person
    SET bio_text   = $1,
        updated_at = $2
    WHERE full_name = $3
      AND bio_text IS NULL
"""

# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

async def run_etl(conn: asyncpg.Connection) -> None:
    now = datetime.now(timezone.utc)

    # Load CSV
    log.info("Loading CSV: %s", CSV_PATH)
    df = pd.read_csv(CSV_PATH, dtype=str)
    log.info("CSV loaded — %d total rows", len(df))

    if TEST_MODE:
        df = df.head(TEST_LIMIT)
        log.info("TEST_MODE — capped to %d rows", len(df))

    df = df.dropna(subset=["name", "text"])
    log.info("After filtering — %d rows to process", len(df))

    updated = 0
    skipped = 0

    for _, row in df.iterrows():
        name     = str(row["name"]).strip()
        bio_text = str(row["text"]).strip()

        if not name or not bio_text or bio_text.lower() == "nan":
            skipped += 1
            continue

        try:
            result = await conn.execute(
                UPDATE_BIO_SQL,
                bio_text,
                now,
                name,
            )
            # asyncpg returns "UPDATE N" string
            count = int(result.split()[-1])
            updated += count
            if count == 0:
                skipped += 1
        except Exception as e:
            log.warning("Failed name=%s: %s", name, e)
            skipped += 1

    log.info(
        "people_wiki ETL complete — %d person bio_text fields updated, %d skipped",
        updated, skipped,
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