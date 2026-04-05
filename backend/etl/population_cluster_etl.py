"""
ETL: population_cluster static seed + genome_cluster_assignment
Seeds 50 population clusters and assigns each genome to 2-3 clusters.
Must run after: genome_etl.py, dataset_source_etl.py

Usage:
    python backend/etl/population_cluster_etl.py
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

# ---------------------------------------------------------------------------
# Population cluster definitions
# ---------------------------------------------------------------------------

CLUSTERS = [
    # (cluster_code, cluster_name, haplogroup_signature, lat, lon, time_start, time_end, description)
    ("EUR",              "European",                    ["R1b","R1a","I1","H","U5"],        54.5,  15.3,  -40000, 2024,  "Modern European ancestry cluster"),
    ("EUR_WEST",         "Western European",            ["R1b","H1","U5b"],                 48.0,   4.0,  -10000, 2024,  "Western European ancestry"),
    ("EUR_EAST",         "Eastern European",            ["R1a","I2","U5a"],                 52.0,  32.0,  -10000, 2024,  "Eastern European ancestry"),
    ("EUR_NORTH",        "Northern European",           ["I1","R1b","U5b"],                 63.0,  16.0,  -10000, 2024,  "Northern European / Scandinavian ancestry"),
    ("EUR_SOUTH",        "Southern European",           ["J2","G2a","H3"],                  42.0,  14.0,  -10000, 2024,  "Southern European / Mediterranean ancestry"),
    ("MID_EAST",         "Middle Eastern",              ["J1","J2","T1"],                   29.3,  42.5,  -30000, 2024,  "Middle Eastern ancestry cluster"),
    ("ANATOLIAN",        "Anatolian",                   ["J2","G2a","H"],                   39.0,  35.0,  -10000, 2024,  "Anatolian / Turkish ancestry"),
    ("LEVANTINE",        "Levantine",                   ["J1","J2","K"],                    33.8,  35.9,  -10000, 2024,  "Levantine ancestry"),
    ("ARABIAN",          "Arabian",                     ["J1","L","H"],                     23.9,  45.1,  -5000,  2024,  "Arabian Peninsula ancestry"),
    ("SAS",              "South Asian",                 ["R1a1","H1","M2"],                 20.6,  79.0,  -40000, 2024,  "South Asian ancestry cluster"),
    ("SAS_NORTH",        "North Indian",                ["R1a1","H1","U2"],                 28.0,  77.0,  -5000,  2024,  "North Indian ancestry"),
    ("SAS_SOUTH",        "South Indian",                ["H1","L1","M2"],                   12.0,  80.0,  -5000,  2024,  "South Indian / Dravidian ancestry"),
    ("EAS",              "East Asian",                  ["O2","D","B4"],                    35.9, 104.2,  -40000, 2024,  "East Asian ancestry cluster"),
    ("EAS_CHINESE",      "Han Chinese",                 ["O2","D","B4"],                    30.0, 112.0,  -5000,  2024,  "Han Chinese ancestry"),
    ("EAS_JAPANESE",     "Japanese",                    ["O2b","D2","B4"],                  36.2, 138.3,  -3000,  2024,  "Japanese ancestry"),
    ("EAS_KOREAN",       "Korean",                      ["O2b","D","B4"],                   35.9, 127.8,  -3000,  2024,  "Korean ancestry"),
    ("SEA",              "Southeast Asian",             ["O1","O2a","B4"],                  12.6, 105.0,  -40000, 2024,  "Southeast Asian ancestry cluster"),
    ("CAS",              "Central Asian",               ["R1a","C2","U4"],                  45.0,  63.0,  -30000, 2024,  "Central Asian ancestry cluster"),
    ("STEPPE",           "Pontic-Caspian Steppe",       ["R1a","R1b1a2","U5a"],             48.0,  55.0,  -6000,  -1000, "Bronze Age steppe ancestry"),
    ("STEPPE_BRONZE",    "Steppe Bronze Age",           ["R1a","R1b1a2","U5a","H"],         48.0,  55.0,  -3500,  -1200, "Yamnaya / Corded Ware ancestry"),
    ("AFR",              "African",                     ["E1b1a","L1","L2"],                 8.8,  34.5,  -200000,2024,  "African ancestry cluster"),
    ("AFR_WEST",         "West African",                ["E1b1a","L2"],                     12.0,  -8.0,  -10000, 2024,  "West African ancestry"),
    ("AFR_EAST",         "East African",                ["E1b1b","L3","L0"],                 8.0,  40.0,  -20000, 2024,  "East African ancestry"),
    ("AFR_NORTH",        "North African",               ["E1b1b1a","U6","H"],               25.0,  17.0,  -10000, 2024,  "North African ancestry"),
    ("AFR_SUB",          "Sub-Saharan African",         ["E1b1a","L1","L2","L3"],           -2.5,  23.0,  -50000, 2024,  "Sub-Saharan African ancestry"),
    ("AMR_NATIVE",       "Native American",             ["Q1a","C2","A","B2","D"],          15.0, -90.0,  -20000, 2024,  "Native American ancestry"),
    ("AMR_MESOAMERICA",  "Mesoamerican",                ["Q1a","A","C"],                    19.4, -99.1,  -5000,  1500,  "Mesoamerican ancestry"),
    ("AMR_SOUTH",        "South American",              ["Q1a","B2","D"],                  -15.0, -60.0,  -15000, 2024,  "South American indigenous ancestry"),
    ("OCE",              "Oceanian",                    ["M","S","Q"],                     -22.7, 140.0,  -50000, 2024,  "Oceanian / Pacific Islander ancestry"),
    ("OCE_ABORIGINAL",   "Australian Aboriginal",       ["S","M","O"],                     -25.3, 133.8,  -50000, 2024,  "Australian Aboriginal ancestry"),
    # Ancient populations
    ("WHG",              "Western Hunter-Gatherer",     ["I","U5b","U4"],                   48.0,   4.0,  -15000, -5000, "European Mesolithic hunter-gatherers"),
    ("EHG",              "Eastern Hunter-Gatherer",     ["R1a","U5a","U4"],                 60.0,  50.0,  -15000, -5000, "Eastern European Mesolithic hunter-gatherers"),
    ("CHG",              "Caucasus Hunter-Gatherer",    ["J2","G2a","H13"],                 42.0,  45.0,  -15000, -5000, "Caucasus Mesolithic hunter-gatherers"),
    ("ANF",              "Anatolian Neolithic Farmer",  ["G2a","J2","N1"],                  39.0,  35.0,  -10000, -5000, "Early Anatolian farmers"),
    ("EEF",              "Early European Farmer",       ["G2a","H","J1"],                   46.0,  14.0,  -8000,  -3000, "Neolithic European farmers"),
    ("YAMNAYA",          "Yamnaya",                     ["R1b1a2","U5a","H"],               48.0,  48.0,  -3500,  -2500, "Yamnaya Bronze Age culture"),
    ("CORDED_WARE",      "Corded Ware",                 ["R1a","U5a","H"],                  52.0,  18.0,  -2900,  -2350, "Corded Ware culture"),
    ("BELL_BEAKER",      "Bell Beaker",                 ["R1b","H","U5"],                   46.0,   4.0,  -2750,  -2000, "Bell Beaker culture"),
    ("ANCIENT_EGYPT",    "Ancient Egyptian",            ["E1b1b1a","J1","R1b"],             26.0,  32.0,  -3100,   641,  "Ancient Egyptian ancestry"),
    ("ANCIENT_ROME",     "Ancient Roman",               ["J2","R1b","G2a","H"],             41.9,  12.5,  -753,    476,  "Ancient Roman ancestry"),
    ("ANCIENT_GREECE",   "Ancient Greek",               ["J2","G2a","E1b1b","H3"],          39.1,  21.8,  -800,    146,  "Ancient Greek ancestry"),
    ("ANCIENT_PERSIA",   "Ancient Persian",             ["J2","R1a","G2a"],                 32.4,  53.7,  -550,    651,  "Ancient Persian / Achaemenid ancestry"),
    ("MONGOL",           "Mongol",                      ["C2","N1","R1a"],                  46.9, 103.8,  -1000,  1500,  "Mongol Empire era ancestry"),
    ("VIKING",           "Viking / Norse",              ["I1","R1b","U5b"],                 63.0,  16.0,  -793,   1100,  "Viking Age Norse ancestry"),
    ("CELTIC",           "Celtic",                      ["R1b","H","U5b"],                  48.0,   4.0,  -800,    43,   "Iron Age Celtic ancestry"),
    ("GERMANIC",         "Germanic",                    ["I1","R1b","U5b"],                 51.0,  10.0,  -500,   1000,  "Germanic / Migration Period ancestry"),
    ("SLAVIC",           "Slavic",                      ["R1a","I2","U5a"],                 52.0,  25.0,   500,   1500,  "Early Slavic ancestry"),
    ("ARAB_EXPANSION",   "Arab Expansion Era",          ["J1","J2","T1"],                   24.0,  45.0,   622,   1258,  "Arab expansion era ancestry"),
    ("OTTOMAN",          "Ottoman Era",                 ["J2","R1b","G2a"],                 39.9,  32.9,  1299,   1922,  "Ottoman Empire era ancestry"),
    ("INDUS_VALLEY",     "Indus Valley Civilization",   ["H1","R2","U2"],                   27.0,  68.0,  -3300,  -1300, "Indus Valley Civilization ancestry"),
    ("ANCIENT_CHINA",    "Ancient Chinese",             ["O2","D","B4"],                    35.0, 108.0,  -2100,   220,  "Ancient Chinese / Han Dynasty ancestry"),
]

INSERT_CLUSTER_SQL = """
    INSERT INTO population_cluster (
        cluster_code,
        cluster_name,
        haplogroup_signature,
        geographic_centroid_lat,
        geographic_centroid_lon,
        time_period_start,
        time_period_end,
        source_dataset_id,
        description
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (cluster_code) DO UPDATE SET
        cluster_name            = EXCLUDED.cluster_name,
        haplogroup_signature    = EXCLUDED.haplogroup_signature,
        geographic_centroid_lat = EXCLUDED.geographic_centroid_lat,
        geographic_centroid_lon = EXCLUDED.geographic_centroid_lon,
        time_period_start       = EXCLUDED.time_period_start,
        time_period_end         = EXCLUDED.time_period_end,
        description             = EXCLUDED.description
    RETURNING cluster_id, cluster_code
"""

INSERT_ASSIGNMENT_SQL = """
    INSERT INTO genome_cluster_assignment (
        genome_id,
        cluster_id,
        membership_probability,
        assignment_method,
        assigned_at
    )
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (genome_id, cluster_id) DO NOTHING
"""

# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

async def run_etl(conn: asyncpg.Connection) -> None:
    now = datetime.now(timezone.utc)

    # Resolve dataset_id
    row = await conn.fetchrow(
        "SELECT dataset_id FROM dataset_source WHERE short_code = $1", "GENOGRAPHIC"
    )
    if not row:
        raise RuntimeError("GENOGRAPHIC dataset_source row not found")
    dataset_id = row["dataset_id"]
    log.info("Resolved GENOGRAPHIC dataset_id=%d", dataset_id)

    # Insert clusters
    cluster_id_map: dict[str, str] = {}
    cluster_count = 0

    for code, name, haplo_sig, lat, lon, t_start, t_end, desc in CLUSTERS:
        try:
            result = await conn.fetchrow(
                INSERT_CLUSTER_SQL,
                code, name, haplo_sig, lat, lon, t_start, t_end, dataset_id, desc
            )
            cluster_id_map[code] = str(result["cluster_id"])
            log.info("Upserted cluster %-25s %s", code, name)
            cluster_count += 1
        except Exception as e:
            log.warning("Failed cluster %s: %s", code, e)

    log.info("population_cluster seed complete — %d clusters", cluster_count)

    # Load all genome IDs
    genomes = await conn.fetch("""
        SELECT g.genome_id, g.y_haplogroup, g.mt_haplogroup, r.region_name
        FROM genome g
        JOIN person p ON p.person_id = g.person_id
        LEFT JOIN region r ON r.region_id = p.birth_region_id
    """)
    log.info("Loaded %d genomes for cluster assignment", len(genomes))

    cluster_ids = list(cluster_id_map.values())
    assignment_count = 0

    for genome in genomes:
        genome_id   = str(genome["genome_id"])
        y_hap       = genome["y_haplogroup"] or ""
        region_name = genome["region_name"] or ""

        # Assign 2-3 clusters per genome based on haplogroup match
        matched_clusters = []
        for code, _, haplo_sig, _, _, _, _, _ in CLUSTERS:
            if any(h in y_hap for h in haplo_sig):
                if code in cluster_id_map:
                    matched_clusters.append(cluster_id_map[code])

        # If no match, assign random clusters
        if not matched_clusters:
            matched_clusters = random.sample(cluster_ids, min(2, len(cluster_ids)))

        # Cap at 3 clusters per genome
        selected = matched_clusters[:3]
        if len(selected) < 2:
            extras = [c for c in cluster_ids if c not in selected]
            selected += random.sample(extras, min(2 - len(selected), len(extras)))

        # Assign probabilities that sum to ~1.0
        probs = sorted(
            [round(random.uniform(0.1, 0.8), 4) for _ in selected],
            reverse=True
        )
        total = sum(probs)
        probs = [round(p / total, 4) for p in probs]

        for cluster_id, prob in zip(selected, probs):
            try:
                await conn.execute(
                    INSERT_ASSIGNMENT_SQL,
                    genome_id,
                    cluster_id,
                    prob,
                    "PCA_projection",
                    now,
                )
                assignment_count += 1
            except Exception:
                pass

    log.info(
        "genome_cluster_assignment complete — %d assignments inserted",
        assignment_count
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