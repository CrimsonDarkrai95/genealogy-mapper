"""
ETL: haplogroup_reference static seed
Populates the Y-DNA and mtDNA haplogroup reference tree.
Covers major branches at 4-5 levels deep.
Must run after: dataset_source_etl.py

Usage:
    python backend/etl/haplogroup_etl.py
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Haplogroup tree data
# Structured as flat list with parent_code reference.
# Insert order matters — parents must exist before children.
# ---------------------------------------------------------------------------

# Each entry: (haplogroup_code, haplogroup_type, parent_code, defining_snp, age_estimate_ybp, geographic_origin)
# parent_code = None means root node

HAPLOGROUPS = [

    # -------------------------------------------------------------------------
    # Y-DNA ROOT
    # -------------------------------------------------------------------------
    ("A",       "Y-DNA", None,  "M91",    340000, "Africa"),
    ("BT",      "Y-DNA", "A",   "M42",    160000, "Africa"),
    ("CT",      "Y-DNA", "BT",  "M168",   100000, "Africa"),
    ("DE",      "Y-DNA", "CT",  "M145",    70000, "Africa"),
    ("CF",      "Y-DNA", "CT",  "P143",    70000, "Africa / Asia"),

    # -------------------------------------------------------------------------
    # Y-DNA Haplogroup D
    # -------------------------------------------------------------------------
    ("D",       "Y-DNA", "DE",  "M174",    60000, "Asia"),
    ("D1",      "Y-DNA", "D",   "M15",     50000, "Central Asia"),
    ("D2",      "Y-DNA", "D",   "M55",     40000, "Japan"),

    # -------------------------------------------------------------------------
    # Y-DNA Haplogroup E
    # -------------------------------------------------------------------------
    ("E",       "Y-DNA", "DE",  "M96",     70000, "Africa"),
    ("E1",      "Y-DNA", "E",   "P147",    60000, "Africa"),
    ("E1a",     "Y-DNA", "E1",  "M132",    50000, "West Africa"),
    ("E1b",     "Y-DNA", "E1",  "P2",      60000, "Africa"),
    ("E1b1",    "Y-DNA", "E1b", "P177",    55000, "Africa"),
    ("E1b1a",   "Y-DNA", "E1b1","M2",      50000, "Sub-Saharan Africa"),
    ("E1b1b",   "Y-DNA", "E1b1","M215",    30000, "Horn of Africa / North Africa"),
    ("E1b1b1",  "Y-DNA", "E1b1b","M35",   25000, "Horn of Africa"),
    ("E1b1b1a", "Y-DNA", "E1b1b1","M78",  15000, "North Africa / Mediterranean"),

    # -------------------------------------------------------------------------
    # Y-DNA Haplogroup C and F
    # -------------------------------------------------------------------------
    ("C",       "Y-DNA", "CF",  "M130",    60000, "Asia / Oceania"),
    ("C1",      "Y-DNA", "C",   "F3393",   50000, "East Asia"),
    ("C2",      "Y-DNA", "C",   "M217",    40000, "Central Asia / Mongols"),
    ("F",       "Y-DNA", "CF",  "M89",     60000, "Middle East"),
    ("G",       "Y-DNA", "F",   "M201",    26000, "Caucasus / Middle East"),
    ("G1",      "Y-DNA", "G",   "M285",    20000, "Central Asia"),
    ("G2",      "Y-DNA", "G",   "P287",    26000, "Caucasus / Europe"),
    ("G2a",     "Y-DNA", "G2",  "P15",     22000, "Caucasus / Europe"),
    ("G2b",     "Y-DNA", "G2",  "M3115",   15000, "Middle East"),

    # -------------------------------------------------------------------------
    # Y-DNA Haplogroup H, I, J
    # -------------------------------------------------------------------------
    ("H",       "Y-DNA", "F",   "M69",     40000, "South Asia"),
    ("H1",      "Y-DNA", "H",   "M52",     30000, "India"),
    ("H2",      "Y-DNA", "H",   "B176",    25000, "South Asia"),

    ("IJ",      "Y-DNA", "F",   "S2",      45000, "Middle East"),
    ("I",       "Y-DNA", "IJ",  "M170",    35000, "Europe"),
    ("I1",      "Y-DNA", "I",   "M253",    10000, "Scandinavia / Northwestern Europe"),
    ("I2",      "Y-DNA", "I",   "M438",    25000, "Balkans / Eastern Europe"),
    ("I2a",     "Y-DNA", "I2",  "P37.2",   20000, "Balkans"),
    ("I2b",     "Y-DNA", "I2",  "M436",    15000, "Western Europe"),

    ("J",       "Y-DNA", "IJ",  "M304",    40000, "Middle East"),
    ("J1",      "Y-DNA", "J",   "M267",    30000, "Arabian Peninsula / Horn of Africa"),
    ("J1a",     "Y-DNA", "J1",  "M62",     20000, "Middle East"),
    ("J2",      "Y-DNA", "J",   "M172",    30000, "Middle East / Mediterranean"),
    ("J2a",     "Y-DNA", "J2",  "M410",    25000, "Anatolia / Mediterranean"),
    ("J2b",     "Y-DNA", "J2",  "M12",     20000, "South Asia / Balkans"),

    # -------------------------------------------------------------------------
    # Y-DNA Haplogroup K, L, M, N, O, P, Q
    # -------------------------------------------------------------------------
    ("K",       "Y-DNA", "F",   "M9",      47000, "Asia"),
    ("L",       "Y-DNA", "K",   "M20",     30000, "South Asia"),
    ("L1",      "Y-DNA", "L",   "M76",     25000, "India / Pakistan"),

    ("M",       "Y-DNA", "K",   "P256",    35000, "Melanesia"),
    ("N",       "Y-DNA", "K",   "M231",    35000, "Northern Asia / Northern Europe"),
    ("N1",      "Y-DNA", "N",   "M214",    30000, "Northern Asia"),
    ("N1a",     "Y-DNA", "N1",  "M128",    15000, "Northern Asia / Uralic"),
    ("N1b",     "Y-DNA", "N1",  "P43",     20000, "Siberia / Finland"),
    ("N1c",     "Y-DNA", "N1",  "M46",     12000, "Finland / Baltic / Siberia"),

    ("O",       "Y-DNA", "K",   "M175",    35000, "East Asia / Southeast Asia"),
    ("O1",      "Y-DNA", "O",   "M119",    30000, "Southeast Asia"),
    ("O2",      "Y-DNA", "O",   "M122",    25000, "East Asia"),
    ("O2a",     "Y-DNA", "O2",  "M95",     20000, "Southeast Asia"),
    ("O2b",     "Y-DNA", "O2",  "M176",    15000, "Korea / Japan"),

    ("P",       "Y-DNA", "K",   "M45",     38000, "Central Asia"),
    ("Q",       "Y-DNA", "P",   "M242",    25000, "Central Asia / Americas"),
    ("Q1",      "Y-DNA", "Q",   "F1096",   20000, "Central Asia"),
    ("Q1a",     "Y-DNA", "Q1",  "M120",    17000, "Central Asia / Americas"),
    ("Q1b",     "Y-DNA", "Q1",  "M378",    15000, "South Asia"),

    # -------------------------------------------------------------------------
    # Y-DNA Haplogroup R — most important for European / South Asian ancestry
    # -------------------------------------------------------------------------
    ("R",       "Y-DNA", "P",   "M207",    30000, "Central Asia"),
    ("R1",      "Y-DNA", "R",   "M173",    25000, "Europe / South Asia"),
    ("R1a",     "Y-DNA", "R1",  "M420",    22000, "Eastern Europe / South Asia / Central Asia"),
    ("R1a1",    "Y-DNA", "R1a", "M17",     18000, "Eastern Europe / South Asia"),
    ("R1a1a",   "Y-DNA", "R1a1","M198",    15000, "Eastern Europe / South Asia"),
    ("R1b",     "Y-DNA", "R1",  "M343",    22000, "Western Europe"),
    ("R1b1",    "Y-DNA", "R1b", "L278",    20000, "Western Europe"),
    ("R1b1a",   "Y-DNA", "R1b1","P297",    18000, "Western Europe / Steppe"),
    ("R1b1a1",  "Y-DNA", "R1b1a","M73",   15000, "Central Asia"),
    ("R1b1a2",  "Y-DNA", "R1b1a","M269",  12000, "Western Europe / Steppe"),
    ("R2",      "Y-DNA", "R",   "M479",    15000, "South Asia"),

    # -------------------------------------------------------------------------
    # Y-DNA Haplogroup S and T
    # -------------------------------------------------------------------------
    ("S",       "Y-DNA", "K",   "M230",    35000, "Melanesia / Papua New Guinea"),
    ("T",       "Y-DNA", "K",   "M184",    25000, "Middle East / East Africa"),
    ("T1",      "Y-DNA", "T",   "M272",    20000, "Middle East / Mediterranean"),

    # -------------------------------------------------------------------------
    # mtDNA ROOT
    # -------------------------------------------------------------------------
    ("L0",      "mtDNA", None,  "None",   190000, "Sub-Saharan Africa"),
    ("L1",      "mtDNA", "L0",  "None",   170000, "Sub-Saharan Africa"),
    ("L2",      "mtDNA", "L1",  "None",   100000, "Sub-Saharan Africa / West Africa"),
    ("L3",      "mtDNA", "L1",  "None",    80000, "East Africa"),
    ("L4",      "mtDNA", "L1",  "None",    90000, "East Africa"),
    ("L5",      "mtDNA", "L1",  "None",   100000, "Sub-Saharan Africa"),

    # -------------------------------------------------------------------------
    # mtDNA Macro-haplogroup M and N
    # -------------------------------------------------------------------------
    ("M",       "mtDNA", "L3",  "None",    65000, "South Asia / East Asia"),
    ("M1",      "mtDNA", "M",   "None",    45000, "North Africa / Horn of Africa"),
    ("M2",      "mtDNA", "M",   "None",    50000, "South Asia"),
    ("M7",      "mtDNA", "M",   "None",    40000, "East Asia / Southeast Asia"),
    ("M8",      "mtDNA", "M",   "None",    40000, "East Asia"),
    ("C",       "mtDNA", "M",   "None",    35000, "Northern Asia / Americas"),
    ("D",       "mtDNA", "M",   "None",    40000, "Northern Asia / East Asia"),
    ("G",       "mtDNA", "M",   "None",    30000, "Northern Asia"),
    ("Q",       "mtDNA", "M",   "None",    35000, "Melanesia"),

    ("N",       "mtDNA", "L3",  "None",    65000, "Middle East / Asia"),
    ("N1",      "mtDNA", "N",   "None",    55000, "Middle East"),
    ("N2",      "mtDNA", "N",   "None",    50000, "Middle East"),
    ("A",       "mtDNA", "N",   "None",    40000, "Northern Asia / Americas"),
    ("I",       "mtDNA", "N",   "None",    30000, "Europe / Middle East"),
    ("W",       "mtDNA", "N",   "None",    25000, "Europe / South Asia"),
    ("X",       "mtDNA", "N",   "None",    30000, "Middle East / Europe / Americas"),

    # -------------------------------------------------------------------------
    # mtDNA Macro-haplogroup R
    # -------------------------------------------------------------------------
    ("R",       "mtDNA", "N",   "None",    65000, "Asia / Europe"),
    ("R0",      "mtDNA", "R",   "None",    40000, "Middle East"),
    ("HV",      "mtDNA", "R0",  "None",    35000, "Middle East / Europe"),
    ("H",       "mtDNA", "HV",  "None",    30000, "Europe / Middle East"),
    ("H1",      "mtDNA", "H",   "None",    15000, "Western Europe"),
    ("H2",      "mtDNA", "H",   "None",    20000, "Europe / Middle East"),
    ("H3",      "mtDNA", "H",   "None",    10000, "Western Europe / North Africa"),
    ("H4",      "mtDNA", "H",   "None",    12000, "Europe"),
    ("H5",      "mtDNA", "H",   "None",    11000, "Europe"),
    ("H6",      "mtDNA", "H",   "None",    14000, "Europe / Middle East"),
    ("V",       "mtDNA", "HV",  "None",    12000, "Europe / North Africa"),

    ("B",       "mtDNA", "R",   "None",    50000, "East Asia / Americas / Oceania"),
    ("B2",      "mtDNA", "B",   "None",    18000, "Americas"),
    ("B4",      "mtDNA", "B",   "None",    40000, "East Asia / Southeast Asia"),
    ("B5",      "mtDNA", "B",   "None",    35000, "East Asia"),

    ("F",       "mtDNA", "R",   "None",    40000, "East Asia / Southeast Asia"),
    ("F1",      "mtDNA", "F",   "None",    30000, "East Asia"),

    ("P",       "mtDNA", "R",   "None",    35000, "Melanesia / Australia"),

    # -------------------------------------------------------------------------
    # mtDNA Haplogroup U — important for ancient European ancestry
    # -------------------------------------------------------------------------
    ("U",       "mtDNA", "R",   "None",    55000, "Europe / Middle East / South Asia"),
    ("U1",      "mtDNA", "U",   "None",    30000, "Middle East / South Asia"),
    ("U2",      "mtDNA", "U",   "None",    50000, "South Asia / Europe"),
    ("U3",      "mtDNA", "U",   "None",    35000, "Middle East / Europe"),
    ("U4",      "mtDNA", "U",   "None",    25000, "Europe / Northern Asia"),
    ("U5",      "mtDNA", "U",   "None",    45000, "Europe — oldest European mtDNA"),
    ("U5a",     "mtDNA", "U5",  "None",    25000, "Europe"),
    ("U5b",     "mtDNA", "U5",  "None",    30000, "Europe / North Africa"),
    ("U6",      "mtDNA", "U",   "None",    40000, "North Africa"),
    ("U7",      "mtDNA", "U",   "None",    30000, "South Asia / Middle East"),
    ("U8",      "mtDNA", "U",   "None",    45000, "Europe / Middle East"),
    ("K",       "mtDNA", "U8",  "None",    16000, "Europe / Middle East"),
    ("K1",      "mtDNA", "K",   "None",    12000, "Europe / Middle East"),
    ("K2",      "mtDNA", "K",   "None",    10000, "Europe"),

    # -------------------------------------------------------------------------
    # mtDNA Haplogroup T and J — common in Middle East / Europe
    # -------------------------------------------------------------------------
    ("T",       "mtDNA", "R",   "None",    45000, "Middle East / Europe"),
    ("T1",      "mtDNA", "T",   "None",    25000, "Middle East / Eastern Europe"),
    ("T2",      "mtDNA", "T",   "None",    30000, "Europe / Middle East"),

    ("J",       "mtDNA", "R",   "None",    45000, "Middle East / Europe"),
    ("J1",      "mtDNA", "J",   "None",    20000, "Middle East / Europe"),
    ("J2",      "mtDNA", "J",   "None",    25000, "Middle East / Europe"),

    # -------------------------------------------------------------------------
    # mtDNA Haplogroup E, S, Z
    # -------------------------------------------------------------------------
    ("E",       "mtDNA", "M",   "None",    35000, "Southeast Asia / Melanesia"),
    ("S",       "mtDNA", "M",   "None",    30000, "Australia"),
    ("Z",       "mtDNA", "M",   "None",    20000, "Northern Asia / Korea / Finland"),

    # -------------------------------------------------------------------------
    # mtDNA Americas
    # -------------------------------------------------------------------------
    ("X2",      "mtDNA", "X",   "None",    20000, "Americas / Middle East / Europe"),

    # -------------------------------------------------------------------------
    # mtDNA Y and O
    # -------------------------------------------------------------------------
    ("Y",       "mtDNA", "N",   "None",    30000, "Southeast Asia / Melanesia"),
    ("O",       "mtDNA", "M",   "None",    35000, "Southeast Asia"),
]

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

INSERT_SQL = """
    INSERT INTO haplogroup_reference (
        haplogroup_code,
        haplogroup_type,
        parent_haplogroup_id,
        defining_snp,
        age_estimate_ybp,
        geographic_origin
    )
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (haplogroup_code, haplogroup_type) DO UPDATE SET
        parent_haplogroup_id = EXCLUDED.parent_haplogroup_id,
        defining_snp         = EXCLUDED.defining_snp,
        age_estimate_ybp     = EXCLUDED.age_estimate_ybp,
        geographic_origin    = EXCLUDED.geographic_origin
    RETURNING haplogroup_id, haplogroup_code
"""

# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

async def seed_haplogroups(conn: asyncpg.Connection) -> None:
    # Build code → uuid lookup as we insert
    code_to_id: dict[str, str] = {}

    # Load any already existing haplogroups into lookup (idempotency)
    existing = await conn.fetch(
        "SELECT haplogroup_id, haplogroup_code FROM haplogroup_reference"
    )
    for row in existing:
        code_to_id[row["haplogroup_code"]] = str(row["haplogroup_id"])
    log.info("Pre-loaded %d existing haplogroup nodes", len(code_to_id))

    inserted = 0
    skipped  = 0

    for code, htype, parent_code, snp, age, geo in HAPLOGROUPS:
        # Resolve parent UUID
        parent_id = None
        if parent_code:
            parent_id = code_to_id.get(parent_code)
            if not parent_id:
                log.warning(
                    "Parent '%s' not found for '%s' — inserting with NULL parent",
                    parent_code, code
                )

        # Skip duplicate codes within same type that conflict
        # e.g. "M", "C", "D", "G", "Q", "R", "A", "I", "N", "E", "S", "T",
        # "J", "F", "P", "Y", "O" exist in both Y-DNA and mtDNA
        # Use composite key: code + type
        lookup_key = f"{code}_{htype}"

        try:
            row = await conn.fetchrow(
                INSERT_SQL,
                code,
                htype,
                parent_id,
                snp if snp != "None" else None,
                age,
                geo,
            )
            code_to_id[lookup_key] = str(row["haplogroup_id"])
            code_to_id[code] = str(row["haplogroup_id"])  # also store by code alone for parent resolution
            log.info(
                "Upserted %-12s [%s] parent=%-12s age=%d ybp",
                code, htype, parent_code or "ROOT", age
            )
            inserted += 1
        except Exception as e:
            log.warning("Failed %s (%s): %s", code, htype, e)
            skipped += 1

    log.info(
        "haplogroup_reference seed complete — %d upserted, %d skipped",
        inserted, skipped
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
        await seed_haplogroups(conn)
    finally:
        await conn.close()
        log.info("Connection closed.")


if __name__ == "__main__":
    asyncio.run(main())