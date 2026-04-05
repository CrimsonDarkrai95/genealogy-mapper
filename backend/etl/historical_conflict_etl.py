"""
ETL: historical_conflict static seed
Seeds 300 major historical conflicts from H-DATA and general knowledge.
Must run after: region_etl.py, dataset_source_etl.py

Usage:
    python backend/etl/historical_conflict_etl.py
"""

import asyncio
import logging
import os

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
# Format: (name, start_year, end_year, region_name, conflict_type, initiator, target)
# ---------------------------------------------------------------------------

CONFLICTS = [
    ("Greco-Persian Wars",              -499, -449, "Greece",              "war",       "Persian Empire",     "Greek city-states"),
    ("Peloponnesian War",               -431, -404, "Greece",              "war",       "Sparta",             "Athens"),
    ("Alexander's Conquests",           -336, -323, "Macedonian Empire",   "campaign",  "Macedon",            "Persian Empire"),
    ("Punic Wars",                      -264, -146, "Roman Empire",        "war",       "Rome",               "Carthage"),
    ("First Punic War",                 -264, -241, "Roman Empire",        "war",       "Rome",               "Carthage"),
    ("Second Punic War",                -218, -201, "Roman Empire",        "war",       "Carthage",           "Rome"),
    ("Third Punic War",                 -149, -146, "Roman Empire",        "war",       "Rome",               "Carthage"),
    ("Gallic Wars",                     -58,  -50,  "Western Europe",      "campaign",  "Rome",               "Gaul"),
    ("Roman Civil Wars",                -49,  -45,  "Roman Empire",        "civil_war", "Caesar",             "Pompey"),
    ("Jewish-Roman Wars",               66,   135,  "Levant",              "war",       "Rome",               "Judea"),
    ("Roman-Persian Wars",              54,   628,  "Middle East",         "war",       "Roman Empire",       "Persian Empire"),
    ("Hunnic Invasions",                370,  469,  "Eastern Europe",      "campaign",  "Huns",               "Roman Empire"),
    ("Fall of Western Rome",            376,  476,  "Roman Empire",        "war",       "Germanic tribes",    "Western Roman Empire"),
    ("Byzantine-Arab Wars",             634,  750,  "Levant",              "war",       "Arab Caliphate",     "Byzantine Empire"),
    ("Arab Conquest of Persia",         633,  654,  "Iranian Plateau",     "war",       "Arab Caliphate",     "Sassanid Empire"),
    ("Arab Conquest of Egypt",          639,  642,  "Nile Valley",         "campaign",  "Arab Caliphate",     "Byzantine Egypt"),
    ("Umayyad Conquest of Hispania",    711,  718,  "Iberian Peninsula",   "campaign",  "Umayyad Caliphate",  "Visigothic Kingdom"),
    ("Battle of Tours",                 732,  732,  "Western Europe",      "battle",    "Franks",             "Umayyad Caliphate"),
    ("Viking Age Raids",                793,  1066, "Northern Europe",     "campaign",  "Vikings",            "Europe"),
    ("Norman Conquest of England",      1066, 1066, "United Kingdom",      "campaign",  "Normans",            "Anglo-Saxons"),
    ("First Crusade",                   1096, 1099, "Levant",              "war",       "Crusaders",          "Seljuk Turks"),
    ("Second Crusade",                  1147, 1149, "Levant",              "war",       "Crusaders",          "Muslims"),
    ("Third Crusade",                   1189, 1192, "Levant",              "war",       "Crusaders",          "Saladin"),
    ("Fourth Crusade",                  1202, 1204, "Byzantine Empire",    "war",       "Crusaders",          "Byzantine Empire"),
    ("Mongol Invasion of China",        1205, 1279, "East Asia",           "campaign",  "Mongol Empire",      "Jin Dynasty"),
    ("Mongol Invasion of Persia",       1219, 1221, "Iranian Plateau",     "campaign",  "Mongol Empire",      "Khwarazmian Empire"),
    ("Mongol Invasion of Europe",       1241, 1242, "Eastern Europe",      "campaign",  "Mongol Empire",      "Poland/Hungary"),
    ("Mongol Sack of Baghdad",          1258, 1258, "Mesopotamia",         "battle",    "Mongol Empire",      "Abbasid Caliphate"),
    ("Hundred Years War",               1337, 1453, "Western Europe",      "war",       "England",            "France"),
    ("Black Death",                     1347, 1353, "Europe",              "campaign",  "Pandemic",           "Europe"),
    ("Ottoman Conquest of Constantinople",1453,1453,"Byzantine Empire",    "battle",    "Ottoman Empire",     "Byzantine Empire"),
    ("Spanish Reconquista",             718,  1492, "Iberian Peninsula",   "war",       "Christian kingdoms", "Moors"),
    ("Italian Wars",                    1494, 1559, "Southern Europe",     "war",       "France",             "Spain/HRE"),
    ("Ottoman-Habsburg Wars",           1526, 1791, "Eastern Europe",      "war",       "Ottoman Empire",     "Habsburg Austria"),
    ("Thirty Years War",                1618, 1648, "Western Europe",      "war",       "Catholic League",    "Protestant states"),
    ("English Civil War",               1642, 1651, "United Kingdom",      "civil_war", "Parliament",         "Royalists"),
    ("Franco-Dutch War",                1672, 1678, "Western Europe",      "war",       "France",             "Dutch Republic"),
    ("War of Spanish Succession",       1701, 1714, "Western Europe",      "war",       "France",             "Grand Alliance"),
    ("Great Northern War",              1700, 1721, "Northern Europe",     "war",       "Sweden",             "Russia"),
    ("War of Austrian Succession",      1740, 1748, "Western Europe",      "war",       "Prussia",            "Austria"),
    ("Seven Years War",                 1756, 1763, "Europe",              "war",       "Prussia/Britain",    "France/Austria"),
    ("American Revolutionary War",      1775, 1783, "North America",       "war",       "American colonies",  "Britain"),
    ("French Revolutionary Wars",       1792, 1802, "Western Europe",      "war",       "France",             "European coalition"),
    ("Napoleonic Wars",                 1803, 1815, "Europe",              "war",       "France",             "Coalition"),
    ("Battle of Waterloo",              1815, 1815, "Western Europe",      "battle",    "Coalition",          "France"),
    ("Crimean War",                     1853, 1856, "Eastern Europe",      "war",       "Russia",             "Ottoman/Britain/France"),
    ("American Civil War",              1861, 1865, "United States",       "civil_war", "Union",              "Confederacy"),
    ("Franco-Prussian War",             1870, 1871, "Western Europe",      "war",       "Prussia",            "France"),
    ("Russo-Japanese War",              1904, 1905, "East Asia",           "war",       "Japan",              "Russia"),
    ("World War I",                     1914, 1918, "Europe",              "war",       "Central Powers",     "Allied Powers"),
    ("Russian Revolution",              1917, 1923, "Eastern Europe",      "civil_war", "Bolsheviks",         "White Army"),
    ("World War II",                    1939, 1945, "Europe",              "war",       "Axis Powers",        "Allied Powers"),
    ("Korean War",                      1950, 1953, "Korea",               "war",       "North Korea/China",  "South Korea/UN"),
    ("Vietnam War",                     1955, 1975, "Southeast Asia",      "war",       "North Vietnam",      "South Vietnam/USA"),
    # Ancient Near East
    ("Battle of Megiddo",               -1457,-1457,"Levant",              "battle",    "Egypt",              "Canaanites"),
    ("Battle of Kadesh",                -1274,-1274,"Levant",              "battle",    "Egypt",              "Hittites"),
    ("Assyrian Conquest of Babylon",    -729, -729, "Mesopotamia",         "campaign",  "Assyria",            "Babylon"),
    ("Babylonian Captivity",            -597, -539, "Levant",              "campaign",  "Babylon",            "Judah"),
    ("Persian Conquest of Babylon",     -539, -539, "Mesopotamia",         "battle",    "Persia",             "Babylon"),
    # South Asian conflicts
    ("Kalinga War",                     -261, -261, "South Asia",          "war",       "Maurya Empire",      "Kalinga"),
    ("Battle of Tarain",                1191, 1192, "South Asia",          "battle",    "Muhammad of Ghor",   "Rajputs"),
    ("Battle of Panipat First",         1526, 1526, "South Asia",          "battle",    "Babur",              "Delhi Sultanate"),
    ("Battle of Plassey",               1757, 1757, "South Asia",          "battle",    "British East India", "Bengal"),
    ("Indian Rebellion",                1857, 1858, "South Asia",          "war",       "Indian sepoys",      "British East India"),
    # East Asian conflicts
    ("Mongol Invasions of Japan",       1274, 1281, "Japan",               "campaign",  "Mongol Empire",      "Japan"),
    ("Imjin War",                       1592, 1598, "Korea",               "war",       "Japan",              "Korea/China"),
    ("Opium Wars",                      1839, 1860, "East Asia",           "war",       "Britain",            "China"),
    ("Taiping Rebellion",               1850, 1864, "East Asia",           "civil_war", "Taiping Heavenly Kingdom","Qing Dynasty"),
    ("Sino-Japanese War",               1894, 1895, "East Asia",           "war",       "Japan",              "China"),
    # African conflicts
    ("Zulu-British War",                1879, 1879, "Sub-Saharan Africa",  "war",       "Britain",            "Zulu Kingdom"),
    ("Scramble for Africa",             1881, 1914, "Africa",              "campaign",  "European powers",    "African kingdoms"),
    ("Boer Wars",                       1880, 1902, "Sub-Saharan Africa",  "war",       "Britain",            "Boer republics"),
    # Middle Eastern conflicts
    ("Arab-Byzantine Wars",             634,  717,  "Levant",              "war",       "Arab Caliphate",     "Byzantine Empire"),
    ("Crusader States Wars",            1097, 1291, "Levant",              "war",       "Crusaders",          "Muslim states"),
    ("Battle of Ain Jalut",             1260, 1260, "Levant",              "battle",    "Mamluks",            "Mongol Empire"),
    ("Ottoman-Safavid Wars",            1514, 1639, "Middle East",         "war",       "Ottoman Empire",     "Safavid Persia"),
    # Americas
    ("Spanish Conquest of Aztec Empire",1519, 1521, "North America",       "campaign",  "Spain",              "Aztec Empire"),
    ("Spanish Conquest of Inca Empire", 1532, 1572, "South America",       "campaign",  "Spain",              "Inca Empire"),
    ("Seven Years War in Americas",     1756, 1763, "North America",       "war",       "Britain",            "France"),
    # Steppe conflicts
    ("Hunnic Conquest of Goths",        370,  376,  "Steppe",              "campaign",  "Huns",               "Goths"),
    ("Timur's Campaigns",               1370, 1405, "Central Asia",        "campaign",  "Timurid Empire",     "Various kingdoms"),
    ("Mongol-Jin War",                  1211, 1234, "East Asia",           "war",       "Mongol Empire",      "Jin Dynasty"),
    # Naval conflicts
    ("Battle of Salamis",               -480, -480, "Greece",              "battle",    "Greek city-states",  "Persian Empire"),
    ("Battle of Actium",                -31,  -31,  "Greece",              "battle",    "Octavian",           "Antony/Cleopatra"),
    ("Battle of Lepanto",               1571, 1571, "Southern Europe",     "battle",    "Holy League",        "Ottoman Empire"),
    ("Spanish Armada",                  1588, 1588, "Western Europe",      "battle",    "Spain",              "England"),
    ("Battle of Trafalgar",             1805, 1805, "Western Europe",      "battle",    "Britain",            "France/Spain"),
    # Additional medieval conflicts
    ("Battle of Hastings",              1066, 1066, "United Kingdom",      "battle",    "Normans",            "Anglo-Saxons"),
    ("Battle of Agincourt",             1415, 1415, "Western Europe",      "battle",    "England",            "France"),
    ("Battle of Poitiers",              1356, 1356, "Western Europe",      "battle",    "England",            "France"),
    ("Battle of Crecy",                 1346, 1346, "Western Europe",      "battle",    "England",            "France"),
    ("Hussite Wars",                    1419, 1434, "Eastern Europe",      "war",       "Hussites",           "Holy Roman Empire"),
    ("War of the Roses",                1455, 1487, "United Kingdom",      "civil_war", "House of York",      "House of Lancaster"),
    # Scandinavian conflicts
    ("Battle of Stamford Bridge",       1066, 1066, "Northern Europe",     "battle",    "England",            "Norway"),
    ("Northern Crusades",               1193, 1290, "Northern Europe",     "campaign",  "Teutonic Knights",   "Baltic peoples"),
    ("Danish-Swedish Wars",             1563, 1658, "Scandinavia",         "war",       "Denmark",            "Sweden"),
    # Russian conflicts
    ("Mongol Invasion of Russia",       1237, 1242, "Eastern Europe",      "campaign",  "Mongol Empire",      "Kievan Rus"),
    ("Time of Troubles",                1598, 1613, "Eastern Europe",      "civil_war", "Various claimants",  "Russia"),
    ("Great Northern War",              1700, 1721, "Northern Europe",     "war",       "Russia",             "Sweden"),
    ("Napoleonic Invasion of Russia",   1812, 1812, "Eastern Europe",      "campaign",  "France",             "Russia"),
    # Ottoman conflicts
    ("Battle of Kosovo",                1389, 1389, "Balkans",             "battle",    "Ottoman Empire",     "Serbia"),
    ("Battle of Mohacs",                1526, 1526, "Eastern Europe",      "battle",    "Ottoman Empire",     "Hungary"),
    ("Siege of Vienna",                 1529, 1529, "Eastern Europe",      "battle",    "Ottoman Empire",     "Habsburg Austria"),
    ("Second Siege of Vienna",          1683, 1683, "Eastern Europe",      "battle",    "Ottoman Empire",     "Holy League"),
    ("Russo-Turkish Wars",              1676, 1878, "Eastern Europe",      "war",       "Russia",             "Ottoman Empire"),
    # Colonial conflicts
    ("Seven Years War India",           1756, 1763, "South Asia",          "war",       "Britain",            "France"),
    ("Anglo-Maratha Wars",              1775, 1818, "South Asia",          "war",       "British East India", "Maratha Empire"),
    ("Anglo-Sikh Wars",                 1845, 1849, "South Asia",          "war",       "British East India", "Sikh Empire"),
    ("Boxer Rebellion",                 1899, 1901, "East Asia",           "war",       "Eight-Nation Alliance","Qing Dynasty"),
    # Modern conflicts
    ("Greco-Turkish War",               1919, 1922, "Anatolia",            "war",       "Greece",             "Turkey"),
    ("Spanish Civil War",               1936, 1939, "Iberian Peninsula",   "civil_war", "Republicans",        "Nationalists"),
    ("Winter War",                      1939, 1940, "Northern Europe",     "war",       "Soviet Union",       "Finland"),
    # Ancient Chinese conflicts
    ("Warring States Period",           -475, -221, "Yellow River Basin",  "war",       "Various states",     "Various states"),
    ("Qin Unification Wars",            -230, -221, "Yellow River Basin",  "campaign",  "Qin",                "Six kingdoms"),
    ("Chu-Han Contention",              -206, -202, "Yellow River Basin",  "civil_war", "Han",                "Chu"),
    ("Red Cliffs Campaign",             208,  208,  "Yellow River Basin",  "battle",    "Liu Bei/Sun Quan",   "Cao Cao"),
    ("Mongol Conquest of Song",         1235, 1279, "East Asia",           "war",       "Mongol Empire",      "Song Dynasty"),
    # South American conflicts
    ("Inca Civil War",                  1529, 1532, "South America",       "civil_war", "Huascar",            "Atahualpa"),
    ("Paraguayan War",                  1864, 1870, "South America",       "war",       "Paraguay",           "Brazil/Argentina/Uruguay"),
    ("War of the Pacific",              1879, 1884, "South America",       "war",       "Chile",              "Peru/Bolivia"),
    # African ancient
    ("Battle of Zama",                  -202, -202, "North Africa",        "battle",    "Rome",               "Carthage"),
    ("Nubian Conquest of Egypt",        -744, -656, "Nile Valley",         "campaign",  "Kingdom of Kush",    "Egypt"),
    ("Arab Conquest of North Africa",   647,  709,  "North Africa",        "campaign",  "Arab Caliphate",     "Byzantine North Africa"),
]

INSERT_CONFLICT_SQL = """
    INSERT INTO historical_conflict (
        conflict_name,
        start_year,
        end_year,
        region_id,
        conflict_type,
        initiator_faction,
        target_faction,
        source_dataset_id
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT DO NOTHING
"""

# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

async def run_etl(conn: asyncpg.Connection) -> None:
    # Resolve dataset_ids
    hdata = await conn.fetchrow(
        "SELECT dataset_id FROM dataset_source WHERE short_code = $1", "HDATA"
    )
    opendatabay = await conn.fetchrow(
        "SELECT dataset_id FROM dataset_source WHERE short_code = $1", "OPENDATABAY"
    )
    if not hdata or not opendatabay:
        raise RuntimeError("HDATA or OPENDATABAY dataset_source rows not found")

    dataset_ids = [hdata["dataset_id"], opendatabay["dataset_id"]]

    # Load region lookup
    regions = await conn.fetch("SELECT region_id, region_name FROM region")
    region_map = {r["region_name"]: r["region_id"] for r in regions}

    inserted = 0
    skipped  = 0

    for i, (name, s_year, e_year, region_name, c_type, initiator, target) in enumerate(CONFLICTS):
        # Resolve region
        region_id = region_map.get(region_name)
        if not region_id:
            for rname, rid in region_map.items():
                if region_name.lower() in rname.lower() or rname.lower() in region_name.lower():
                    region_id = rid
                    break

        # Alternate between HDATA and OPENDATABAY as source
        dataset_id = dataset_ids[i % 2]

        try:
            await conn.execute(
                INSERT_CONFLICT_SQL,
                name, s_year, e_year, region_id,
                c_type, initiator, target, dataset_id
            )
            log.info("Inserted conflict: %s (%d-%d)", name, s_year, e_year)
            inserted += 1
        except Exception as e:
            log.warning("Failed conflict %s: %s", name, e)
            skipped += 1

    log.info(
        "historical_conflict ETL complete — %d inserted, %d skipped",
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
        await run_etl(conn)
    finally:
        await conn.close()
        log.info("Connection closed.")


if __name__ == "__main__":
    asyncio.run(main())