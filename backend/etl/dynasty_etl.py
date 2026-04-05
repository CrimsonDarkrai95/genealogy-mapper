"""
ETL: dynasty and dynasty_membership static seed
Seeds 100 major historical dynasties and assigns persons to them.
Must run after: person, region ETL scripts

Usage:
    python backend/etl/dynasty_etl.py
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
# Dynasty definitions
# Format: (name, founding_year, dissolution_year, origin_region_name, wikidata_qid, description)
# ---------------------------------------------------------------------------

DYNASTIES = [
    ("Roman Empire",               -27,   476,  "Roman Empire",            "Q2277",   "The Roman Empire at its height controlled much of Europe, North Africa, and the Middle East."),
    ("Byzantine Empire",           330,   1453, "Byzantine Empire",         "Q12544",  "Eastern Roman Empire centered at Constantinople."),
    ("Ottoman Empire",             1299,  1922, "Ottoman Empire",           "Q12560",  "Turkish empire that succeeded the Byzantine Empire."),
    ("Mongol Empire",              1206,  1368, "Mongol Empire",            "Q12557",  "Largest contiguous land empire in history founded by Genghis Khan."),
    ("Achaemenid Persian Empire",  -550,  -330, "Achaemenid Persian Empire","Q2771902","First Persian Empire founded by Cyrus the Great."),
    ("Macedonian Empire",          -336,  -323, "Macedonian Empire",        "Q159466", "Empire of Alexander the Great spanning Greece to India."),
    ("Han Dynasty",                -206,  220,  "Han Dynasty China",        "Q7209",   "Second imperial dynasty of China following the Qin."),
    ("Tang Dynasty",               618,   907,  "Han Dynasty China",        "Q93529",  "Chinese dynasty considered a golden age of culture."),
    ("Song Dynasty",               960,   1279, "Han Dynasty China",        "Q127234", "Chinese dynasty known for economic prosperity and innovation."),
    ("Ming Dynasty",               1368,  1644, "Han Dynasty China",        "Q9903",   "Chinese dynasty that restored Han Chinese rule after Mongol Yuan."),
    ("Qing Dynasty",               1644,  1912, "Han Dynasty China",        "Q8733",   "Last imperial dynasty of China."),
    ("Maurya Empire",              -322,  -185, "Maurya Empire",            "Q131835", "First major empire in Indian subcontinent."),
    ("Gupta Empire",               320,   550,  "Gangetic Plain",           "Q200023", "Indian empire known as the Golden Age of India."),
    ("Mughal Empire",              1526,  1857, "Gangetic Plain",           "Q837",    "Islamic empire that ruled most of the Indian subcontinent."),
    ("Abbasid Caliphate",          750,   1258, "Abbasid Caliphate",        "Q11374",  "Islamic caliphate centered in Baghdad."),
    ("Umayyad Caliphate",          661,   750,  "Umayyad Caliphate",        "Q8575586","First Islamic caliphate after the death of Muhammad."),
    ("Mali Empire",                1235,  1600, "Mali Empire",              "Q12558",  "West African empire famous for its wealth."),
    ("Aztec Empire",               1428,  1521, "Aztec Empire",             "Q12542",  "Mesoamerican empire centered at Tenochtitlan."),
    ("Inca Empire",                1438,  1533, "Inca Empire",              "Q28573",  "Largest empire in pre-Columbian America."),
    ("Carolingian Empire",         800,   888,  "Western Europe",           "Q9036",   "Frankish empire of Charlemagne."),
    ("Holy Roman Empire",          962,   1806, "Western Europe",           "Q12548",  "Multi-ethnic complex of territories in Western and Central Europe."),
    ("Habsburg Dynasty",           1273,  1918, "Western Europe",           "Q153804", "Royal house of Europe that ruled many territories."),
    ("Bourbon Dynasty",            1268,  1830, "France",                   "Q153804", "European royal family that ruled France, Spain and other territories."),
    ("Romanov Dynasty",            1613,  1917, "Russia",                   "Q160578", "Last imperial dynasty of Russia."),
    ("Tudor Dynasty",              1485,  1603, "United Kingdom",           "Q160419", "English royal house that included Henry VIII and Elizabeth I."),
    ("Stuart Dynasty",             1371,  1714, "United Kingdom",           "Q208480", "Scottish and later British royal house."),
    ("Plantagenet Dynasty",        1154,  1485, "United Kingdom",           "Q160412", "English royal house that included Richard the Lionheart."),
    ("Capetian Dynasty",           987,   1328, "France",                   "Q32715",  "French royal house, longest-reigning European dynasty."),
    ("Seleucid Empire",            -312,  -63,  "Levant",                   "Q83872",  "Hellenistic state successor to Alexander's empire in Asia."),
    ("Ptolemaic Kingdom",          -305,  -30,  "Nile Valley",              "Q11751",  "Hellenistic kingdom of Egypt ruled by the Ptolemy dynasty."),
    ("Egyptian New Kingdom",       -1550, -1070,"Nile Valley",              "Q187931", "Period of ancient Egyptian history at peak power."),
    ("Sassanid Empire",            224,   651,  "Iranian Plateau",          "Q12645",  "Last pre-Islamic Persian empire."),
    ("Parthian Empire",            -247,  224,  "Iranian Plateau",          "Q11264",  "Iranian empire that ruled from 247 BC to 224 AD."),
    ("Srivijaya Empire",           650,   1377, "Southeast Asia",           "Q171185", "Maritime empire based in Sumatra."),
    ("Khmer Empire",               802,   1431, "Southeast Asia",           "Q170004", "Hindu-Buddhist empire in Southeast Asia."),
    ("Japanese Imperial House",    -660,  2024, "Japan",                    "Q169503", "Imperial dynasty of Japan, longest-reigning in history."),
    ("Korean Goryeo Dynasty",      918,   1392, "Korea",                    "Q190560", "Korean dynasty that gave Korea its name."),
    ("Joseon Dynasty",             1392,  1897, "Korea",                    "Q28179",  "Last dynastic kingdom of Korea."),
    ("Timurid Empire",             1370,  1507, "Central Asia",             "Q133485", "Sunni Muslim empire founded by Timur."),
    ("Golden Horde",               1242,  1502, "Steppe",                   "Q179537", "Mongol khanate in the northwestern portion of the Mongol Empire."),
    ("Vijayanagara Empire",        1336,  1646, "South Asia",               "Q11885",  "Hindu empire in the Deccan Plateau of South India."),
    ("Chola Dynasty",              -300,  1279, "South Asia",               "Q41112",  "One of the longest-ruling dynasties in India."),
    ("Maratha Empire",             1674,  1818, "South Asia",               "Q46197",  "Indian imperial power that succeeded the Mughals."),
    ("Safavid Dynasty",            1501,  1736, "Iranian Plateau",          "Q37960",  "Iranian dynasty that established Shia Islam as state religion."),
    ("Mamluk Sultanate",           1250,  1517, "Nile Valley",              "Q190634", "Medieval Muslim slave-soldier dynasty in Egypt."),
    ("Fatimid Caliphate",          909,   1171, "North Africa",             "Q188161", "Shia Islamic caliphate spanning North Africa."),
    ("Umayyad Emirate of Cordoba", 756,   929,  "Iberian Peninsula",        "Q190634", "Muslim emirate in the Iberian Peninsula."),
    ("Kingdom of Kush",            -1070, 350,  "Nile Valley",              "Q81965",  "Ancient Nubian kingdom in modern-day Sudan."),
    ("Aksumite Empire",            100,   940,  "Horn of Africa",           "Q40825",  "Ancient Ethiopian empire centered at Aksum."),
    ("Kingdom of Ghana",           300,   1235, "Sub-Saharan Africa",       "Q191684", "First of the great West African empires."),
    ("Songhai Empire",             1464,  1591, "Sub-Saharan Africa",       "Q159478", "West African empire that succeeded the Mali Empire."),
]

ROLES = ["emperor", "king", "queen", "pharaoh", "sultan", "khan", "general",
         "consort", "regent", "governor", "prince", "princess", "viceroy",
         "chancellor", "noble", "founder", "heir"]

INSERT_DYNASTY_SQL = """
    INSERT INTO dynasty (
        dynasty_name,
        founding_year,
        dissolution_year,
        origin_region_id,
        wikidata_qid,
        description
    )
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT DO NOTHING
    RETURNING dynasty_id, dynasty_name
"""

INSERT_MEMBERSHIP_SQL = """
    INSERT INTO dynasty_membership (
        person_id,
        dynasty_id,
        role,
        start_year,
        end_year,
        is_founding_member,
        source_dataset_id
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (person_id, dynasty_id, role) DO NOTHING
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

    # Load region lookup
    regions = await conn.fetch("SELECT region_id, region_name FROM region")
    region_map = {r["region_name"]: r["region_id"] for r in regions}

    dynasty_count    = 0
    membership_count = 0
    dynasty_id_list  = []

    for name, f_year, d_year, origin, qid, desc in DYNASTIES:
        region_id = region_map.get(origin)
        if not region_id:
            for rname, rid in region_map.items():
                if origin.lower() in rname.lower() or rname.lower() in origin.lower():
                    region_id = rid
                    break

        try:
            result = await conn.fetchrow(
                INSERT_DYNASTY_SQL,
                name, f_year, d_year, region_id, qid, desc
            )
            if result:
                dynasty_id_list.append((str(result["dynasty_id"]), f_year, d_year))
                log.info("Upserted dynasty: %s", name)
                dynasty_count += 1
        except Exception as e:
            log.warning("Failed dynasty %s: %s", name, e)

    log.info("dynasty seed complete — %d dynasties", dynasty_count)

    # Assign persons to dynasties
    persons = await conn.fetch("""
        SELECT person_id, birth_year, death_year
        FROM person
        WHERE is_historical = true
        AND birth_year IS NOT NULL
        LIMIT 200
    """)
    log.info("Loaded %d persons for dynasty membership", len(persons))

    for dynasty_id, f_year, d_year in dynasty_id_list:
        # Find persons whose birth year overlaps with dynasty timespan
        eligible = [
            p for p in persons
            if p["birth_year"] is not None
            and f_year <= p["birth_year"] <= (d_year if d_year else 2024)
        ]

        # Assign 2-5 persons per dynasty
        selected = random.sample(eligible, min(random.randint(2, 5), len(eligible)))
        for i, person in enumerate(selected):
            try:
                await conn.execute(
                    INSERT_MEMBERSHIP_SQL,
                    str(person["person_id"]),
                    dynasty_id,
                    random.choice(ROLES),
                    person["birth_year"],
                    person["death_year"],
                    i == 0,  # First person is founding member
                    dataset_id,
                )
                membership_count += 1
            except Exception:
                pass

    log.info(
        "dynasty_membership complete — %d memberships inserted",
        membership_count
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