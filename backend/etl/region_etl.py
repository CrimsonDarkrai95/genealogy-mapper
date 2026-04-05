"""
ETL: region seed insert
Populates the geographic reference table with major world regions.
Must run after dataset_source_etl.py and before any ETL that references region_id.

Usage:
    python backend/etl/region_etl.py
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
# Regions are split into two passes:
# Pass 1 — top-level regions with no parent (parent_region_id = None)
# Pass 2 — sub-regions that reference a parent from Pass 1
# This avoids FK violations on the self-referencing parent_region_id column.
# ---------------------------------------------------------------------------

REGIONS_PASS_1 = [
    # ---- Continents / Top-level geographic zones ----
    {
        "region_name": "Europe",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 54.526,
        "centroid_lon": 15.255,
        "parent_region_name": None,
    },
    {
        "region_name": "Asia",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 34.048,
        "centroid_lon": 100.620,
        "parent_region_name": None,
    },
    {
        "region_name": "Africa",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 8.788,
        "centroid_lon": 34.508,
        "parent_region_name": None,
    },
    {
        "region_name": "North America",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 54.526,
        "centroid_lon": -105.255,
        "parent_region_name": None,
    },
    {
        "region_name": "South America",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": -8.783,
        "centroid_lon": -55.491,
        "parent_region_name": None,
    },
    {
        "region_name": "Oceania",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": -22.735,
        "centroid_lon": 140.020,
        "parent_region_name": None,
    },
    {
        "region_name": "Middle East",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 29.311,
        "centroid_lon": 42.461,
        "parent_region_name": None,
    },
    {
        "region_name": "Central Asia",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 45.000,
        "centroid_lon": 63.000,
        "parent_region_name": None,
    },
    {
        "region_name": "South Asia",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 20.594,
        "centroid_lon": 78.962,
        "parent_region_name": None,
    },
    {
        "region_name": "East Asia",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 35.861,
        "centroid_lon": 104.195,
        "parent_region_name": None,
    },
    {
        "region_name": "Southeast Asia",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 12.565,
        "centroid_lon": 104.991,
        "parent_region_name": None,
    },
    {
        "region_name": "North Africa",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 25.000,
        "centroid_lon": 17.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Sub-Saharan Africa",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": -2.500,
        "centroid_lon": 23.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Eastern Europe",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 52.000,
        "centroid_lon": 32.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Western Europe",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 48.000,
        "centroid_lon": 4.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Northern Europe",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 60.000,
        "centroid_lon": 15.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Southern Europe",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 42.000,
        "centroid_lon": 14.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Steppe",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 48.000,
        "centroid_lon": 55.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Levant",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 33.854,
        "centroid_lon": 35.862,
        "parent_region_name": None,
    },
    {
        "region_name": "Anatolia",
        "modern_country": "TR",
        "region_type": "geographic_zone",
        "centroid_lat": 39.000,
        "centroid_lon": 35.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Iberian Peninsula",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 40.000,
        "centroid_lon": -4.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Mesopotamia",
        "modern_country": "IQ",
        "region_type": "geographic_zone",
        "centroid_lat": 33.000,
        "centroid_lon": 44.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Nile Valley",
        "modern_country": "EG",
        "region_type": "geographic_zone",
        "centroid_lat": 26.000,
        "centroid_lon": 32.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Horn of Africa",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 8.000,
        "centroid_lon": 42.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Arabian Peninsula",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 23.885,
        "centroid_lon": 45.079,
        "parent_region_name": None,
    },
    {
        "region_name": "Iranian Plateau",
        "modern_country": "IR",
        "region_type": "geographic_zone",
        "centroid_lat": 32.000,
        "centroid_lon": 53.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Indus Valley",
        "modern_country": "PK",
        "region_type": "geographic_zone",
        "centroid_lat": 27.000,
        "centroid_lon": 68.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Gangetic Plain",
        "modern_country": "IN",
        "region_type": "geographic_zone",
        "centroid_lat": 25.000,
        "centroid_lon": 83.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Yellow River Basin",
        "modern_country": "CN",
        "region_type": "geographic_zone",
        "centroid_lat": 35.000,
        "centroid_lon": 108.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Mongolian Steppe",
        "modern_country": "MN",
        "region_type": "geographic_zone",
        "centroid_lat": 46.862,
        "centroid_lon": 103.847,
        "parent_region_name": None,
    },
    {
        "region_name": "Caucasus",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 42.000,
        "centroid_lon": 45.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Balkans",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 42.500,
        "centroid_lon": 21.000,
        "parent_region_name": None,
    },
    {
        "region_name": "British Isles",
        "modern_country": "GB",
        "region_type": "geographic_zone",
        "centroid_lat": 54.000,
        "centroid_lon": -2.000,
        "parent_region_name": None,
    },
    {
        "region_name": "Scandinavia",
        "modern_country": None,
        "region_type": "geographic_zone",
        "centroid_lat": 63.000,
        "centroid_lon": 16.000,
        "parent_region_name": None,
    },
]

REGIONS_PASS_2 = [
    # ---- Modern countries (children of geographic zones) ----
    {"region_name": "Egypt", "modern_country": "EG", "region_type": "country", "centroid_lat": 26.820, "centroid_lon": 30.802, "parent_region_name": "North Africa"},
    {"region_name": "Ethiopia", "modern_country": "ET", "region_type": "country", "centroid_lat": 9.145, "centroid_lon": 40.489, "parent_region_name": "Horn of Africa"},
    {"region_name": "Nigeria", "modern_country": "NG", "region_type": "country", "centroid_lat": 9.082, "centroid_lon": 8.675, "parent_region_name": "Sub-Saharan Africa"},
    {"region_name": "South Africa", "modern_country": "ZA", "region_type": "country", "centroid_lat": -30.559, "centroid_lon": 22.938, "parent_region_name": "Sub-Saharan Africa"},
    {"region_name": "China", "modern_country": "CN", "region_type": "country", "centroid_lat": 35.861, "centroid_lon": 104.195, "parent_region_name": "East Asia"},
    {"region_name": "Japan", "modern_country": "JP", "region_type": "country", "centroid_lat": 36.204, "centroid_lon": 138.252, "parent_region_name": "East Asia"},
    {"region_name": "Korea", "modern_country": "KR", "region_type": "country", "centroid_lat": 35.907, "centroid_lon": 127.766, "parent_region_name": "East Asia"},
    {"region_name": "Mongolia", "modern_country": "MN", "region_type": "country", "centroid_lat": 46.862, "centroid_lon": 103.847, "parent_region_name": "East Asia"},
    {"region_name": "India", "modern_country": "IN", "region_type": "country", "centroid_lat": 20.594, "centroid_lon": 78.962, "parent_region_name": "South Asia"},
    {"region_name": "Pakistan", "modern_country": "PK", "region_type": "country", "centroid_lat": 30.375, "centroid_lon": 69.345, "parent_region_name": "South Asia"},
    {"region_name": "Iran", "modern_country": "IR", "region_type": "country", "centroid_lat": 32.427, "centroid_lon": 53.688, "parent_region_name": "Middle East"},
    {"region_name": "Iraq", "modern_country": "IQ", "region_type": "country", "centroid_lat": 33.223, "centroid_lon": 43.679, "parent_region_name": "Middle East"},
    {"region_name": "Turkey", "modern_country": "TR", "region_type": "country", "centroid_lat": 38.964, "centroid_lon": 35.243, "parent_region_name": "Middle East"},
    {"region_name": "Saudi Arabia", "modern_country": "SA", "region_type": "country", "centroid_lat": 23.886, "centroid_lon": 45.079, "parent_region_name": "Arabian Peninsula"},
    {"region_name": "Israel", "modern_country": "IL", "region_type": "country", "centroid_lat": 31.046, "centroid_lon": 34.852, "parent_region_name": "Levant"},
    {"region_name": "Syria", "modern_country": "SY", "region_type": "country", "centroid_lat": 34.802, "centroid_lon": 38.997, "parent_region_name": "Levant"},
    {"region_name": "Kazakhstan", "modern_country": "KZ", "region_type": "country", "centroid_lat": 48.019, "centroid_lon": 66.924, "parent_region_name": "Central Asia"},
    {"region_name": "Uzbekistan", "modern_country": "UZ", "region_type": "country", "centroid_lat": 41.377, "centroid_lon": 64.585, "parent_region_name": "Central Asia"},
    {"region_name": "United Kingdom", "modern_country": "GB", "region_type": "country", "centroid_lat": 55.378, "centroid_lon": -3.436, "parent_region_name": "British Isles"},
    {"region_name": "France", "modern_country": "FR", "region_type": "country", "centroid_lat": 46.228, "centroid_lon": 2.214, "parent_region_name": "Western Europe"},
    {"region_name": "Germany", "modern_country": "DE", "region_type": "country", "centroid_lat": 51.166, "centroid_lon": 10.452, "parent_region_name": "Western Europe"},
    {"region_name": "Italy", "modern_country": "IT", "region_type": "country", "centroid_lat": 41.872, "centroid_lon": 12.567, "parent_region_name": "Southern Europe"},
    {"region_name": "Spain", "modern_country": "ES", "region_type": "country", "centroid_lat": 40.463, "centroid_lon": -3.749, "parent_region_name": "Iberian Peninsula"},
    {"region_name": "Greece", "modern_country": "GR", "region_type": "country", "centroid_lat": 39.074, "centroid_lon": 21.824, "parent_region_name": "Southern Europe"},
    {"region_name": "Russia", "modern_country": "RU", "region_type": "country", "centroid_lat": 61.524, "centroid_lon": 105.319, "parent_region_name": "Eastern Europe"},
    {"region_name": "Ukraine", "modern_country": "UA", "region_type": "country", "centroid_lat": 48.379, "centroid_lon": 31.166, "parent_region_name": "Eastern Europe"},
    {"region_name": "Poland", "modern_country": "PL", "region_type": "country", "centroid_lat": 51.920, "centroid_lon": 19.145, "parent_region_name": "Eastern Europe"},
    {"region_name": "Sweden", "modern_country": "SE", "region_type": "country", "centroid_lat": 60.128, "centroid_lon": 18.644, "parent_region_name": "Scandinavia"},
    {"region_name": "Norway", "modern_country": "NO", "region_type": "country", "centroid_lat": 60.472, "centroid_lon": 8.469, "parent_region_name": "Scandinavia"},
    {"region_name": "Denmark", "modern_country": "DK", "region_type": "country", "centroid_lat": 56.263, "centroid_lon": 9.502, "parent_region_name": "Scandinavia"},
    {"region_name": "United States", "modern_country": "US", "region_type": "country", "centroid_lat": 37.090, "centroid_lon": -95.713, "parent_region_name": "North America"},
    {"region_name": "Mexico", "modern_country": "MX", "region_type": "country", "centroid_lat": 23.635, "centroid_lon": -102.553, "parent_region_name": "North America"},
    {"region_name": "Peru", "modern_country": "PE", "region_type": "country", "centroid_lat": -9.190, "centroid_lon": -75.015, "parent_region_name": "South America"},
    {"region_name": "Brazil", "modern_country": "BR", "region_type": "country", "centroid_lat": -14.235, "centroid_lon": -51.925, "parent_region_name": "South America"},
    {"region_name": "Australia", "modern_country": "AU", "region_type": "country", "centroid_lat": -25.274, "centroid_lon": 133.775, "parent_region_name": "Oceania"},
    # ---- Historical empires ----
    {"region_name": "Roman Empire", "modern_country": None, "region_type": "empire", "centroid_lat": 41.902, "centroid_lon": 12.496, "parent_region_name": "Southern Europe"},
    {"region_name": "Byzantine Empire", "modern_country": None, "region_type": "empire", "centroid_lat": 41.008, "centroid_lon": 28.978, "parent_region_name": "Southern Europe"},
    {"region_name": "Ottoman Empire", "modern_country": None, "region_type": "empire", "centroid_lat": 39.920, "centroid_lon": 32.854, "parent_region_name": "Middle East"},
    {"region_name": "Mongol Empire", "modern_country": None, "region_type": "empire", "centroid_lat": 46.862, "centroid_lon": 103.847, "parent_region_name": "Mongolian Steppe"},
    {"region_name": "Achaemenid Persian Empire", "modern_country": None, "region_type": "empire", "centroid_lat": 32.427, "centroid_lon": 53.688, "parent_region_name": "Iranian Plateau"},
    {"region_name": "Macedonian Empire", "modern_country": None, "region_type": "empire", "centroid_lat": 40.614, "centroid_lon": 22.967, "parent_region_name": "Balkans"},
    {"region_name": "Maurya Empire", "modern_country": None, "region_type": "empire", "centroid_lat": 25.000, "centroid_lon": 83.000, "parent_region_name": "Gangetic Plain"},
    {"region_name": "Han Dynasty China", "modern_country": None, "region_type": "empire", "centroid_lat": 35.861, "centroid_lon": 104.195, "parent_region_name": "Yellow River Basin"},
    {"region_name": "Abbasid Caliphate", "modern_country": None, "region_type": "empire", "centroid_lat": 33.341, "centroid_lon": 44.401, "parent_region_name": "Mesopotamia"},
    {"region_name": "Umayyad Caliphate", "modern_country": None, "region_type": "empire", "centroid_lat": 33.510, "centroid_lon": 36.292, "parent_region_name": "Levant"},
    {"region_name": "Mali Empire", "modern_country": None, "region_type": "empire", "centroid_lat": 12.000, "centroid_lon": -8.000, "parent_region_name": "Sub-Saharan Africa"},
    {"region_name": "Aztec Empire", "modern_country": None, "region_type": "empire", "centroid_lat": 19.433, "centroid_lon": -99.133, "parent_region_name": "North America"},
    {"region_name": "Inca Empire", "modern_country": None, "region_type": "empire", "centroid_lat": -13.532, "centroid_lon": -71.967, "parent_region_name": "South America"},
]

# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

INSERT_SQL = """
    INSERT INTO region (
        region_name,
        modern_country,
        region_type,
        centroid_lat,
        centroid_lon,
        parent_region_id
    )
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (region_name) DO UPDATE SET
        modern_country   = EXCLUDED.modern_country,
        region_type      = EXCLUDED.region_type,
        centroid_lat     = EXCLUDED.centroid_lat,
        centroid_lon     = EXCLUDED.centroid_lon,
        parent_region_id = EXCLUDED.parent_region_id
    RETURNING region_id, region_name
"""


async def seed_regions(conn: asyncpg.Connection) -> None:
    # Build a name → region_id lookup after each pass
    name_to_id: dict[str, int] = {}

    # Load any already-existing regions into the lookup first (idempotency)
    existing = await conn.fetch("SELECT region_id, region_name FROM region")
    for row in existing:
        name_to_id[row["region_name"]] = row["region_id"]

    total = 0

    async def upsert_region(record: dict, parent_id: int | None) -> int:
        row = await conn.fetchrow(
            INSERT_SQL,
            record["region_name"],
            record.get("modern_country"),
            record["region_type"],
            record["centroid_lat"],
            record["centroid_lon"],
            parent_id,
        )
        log.info(
            "Upserted region_id=%-4s  %s",
            row["region_id"],
            row["region_name"],
        )
        return row["region_id"]

    log.info("Pass 1 — top-level regions …")
    for record in REGIONS_PASS_1:
        rid = await upsert_region(record, None)
        name_to_id[record["region_name"]] = rid
        total += 1

    log.info("Pass 2 — sub-regions and countries …")
    for record in REGIONS_PASS_2:
        parent_name = record.get("parent_region_name")
        parent_id = name_to_id.get(parent_name) if parent_name else None
        if parent_name and parent_id is None:
            log.warning(
                "Parent region '%s' not found for '%s' — inserting with NULL parent",
                parent_name,
                record["region_name"],
            )
        rid = await upsert_region(record, parent_id)
        name_to_id[record["region_name"]] = rid
        total += 1

    log.info("region seed complete — %d records upserted.", total)


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
        await seed_regions(conn)
    finally:
        await conn.close()
        log.info("Connection closed.")


if __name__ == "__main__":
    asyncio.run(main())