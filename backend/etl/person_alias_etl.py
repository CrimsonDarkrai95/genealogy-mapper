"""
ETL: person_alias synthetic seed
Generates aliases from Age Dataset short descriptions and known alternate names.
Targets 500 alias rows derived from existing person records.
Must run after: age_dataset_etl.py

Usage:
    python backend/etl/person_alias_etl.py
"""

import asyncio
import logging
import os
import re

import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

INSERT_ALIAS_SQL = """
    INSERT INTO person_alias (
        person_id,
        alias_name,
        language_code,
        alias_type
    )
    VALUES ($1, $2, $3, $4)
    ON CONFLICT DO NOTHING
"""

# Known aliases for well-known historical figures by name
KNOWN_ALIASES = {
    "George Washington":       [("General Washington", "en", "title"), ("Father of His Country", "en", "nickname")],
    "Abraham Lincoln":         [("Honest Abe", "en", "nickname"), ("The Great Emancipator", "en", "title")],
    "Julius Caesar":           [("Gaius Julius Caesar", "la", "birth_name"), ("Caesar", "en", "nickname")],
    "Cleopatra":               [("Cleopatra VII Philopator", "el", "birth_name"), ("Queen of the Nile", "en", "nickname")],
    "Alexander the Great":     [("Alexandros III", "el", "birth_name"), ("Megas Alexandros", "el", "nickname")],
    "Genghis Khan":            [("Temüjin", "mn", "birth_name"), ("Chinggis Khan", "mn", "transliteration")],
    "Napoleon Bonaparte":      [("Napoleone di Buonaparte", "it", "birth_name"), ("The Little Corporal", "en", "nickname")],
    "Leonardo da Vinci":       [("Leonardo di ser Piero da Vinci", "it", "birth_name")],
    "Michelangelo":            [("Michelangelo di Lodovico Buonarroti Simoni", "it", "birth_name")],
    "Wolfgang Amadeus Mozart":  [("Johannes Chrysostomus Wolfgangus Theophilus Mozart", "la", "birth_name")],
    "Ludwig van Beethoven":    [("Louis van Beethoven", "fr", "transliteration")],
    "Galileo Galilei":         [("Galileo", "it", "nickname")],
    "Isaac Newton":            [("Sir Isaac Newton", "en", "title")],
    "William Shakespeare":     [("The Bard", "en", "nickname"), ("The Bard of Avon", "en", "nickname")],
    "Aristotle":               [("Aristoteles", "el", "transliteration")],
    "Plato":                   [("Platon", "el", "transliteration"), ("Aristocles", "el", "birth_name")],
    "Socrates":                [("Sokrates", "el", "transliteration")],
    "Confucius":               [("Kong Qiu", "zh", "birth_name"), ("Kong Fuzi", "zh", "transliteration")],
    "Saladin":                 [("Salah ad-Din Yusuf ibn Ayyub", "ar", "birth_name")],
    "Charlemagne":             [("Charles the Great", "en", "transliteration"), ("Karl der Große", "de", "transliteration")],
    "Richard I of England":    [("Richard the Lionheart", "en", "nickname"), ("Cœur de Lion", "fr", "nickname")],
    "Joan of Arc":             [("Jeanne d'Arc", "fr", "birth_name"), ("The Maid of Orleans", "en", "nickname")],
    "Christopher Columbus":    [("Cristóbal Colón", "es", "transliteration"), ("Cristoforo Colombo", "it", "birth_name")],
    "Nicolaus Copernicus":     [("Mikołaj Kopernik", "pl", "birth_name")],
    "Marco Polo":              [("Il Milione", "it", "nickname")],
    "Attila the Hun":          [("Attila", "hu", "birth_name"), ("Scourge of God", "en", "nickname")],
    "Vlad III":                [("Vlad the Impaler", "en", "nickname"), ("Vlad Dracula", "ro", "birth_name")],
    "Ivan the Terrible":       [("Ivan IV Vasilyevich", "ru", "birth_name"), ("Ivan Grozny", "ru", "transliteration")],
    "Peter the Great":         [("Pyotr Alekseyevich Romanov", "ru", "birth_name")],
    "Catherine the Great":     [("Sophie von Anhalt-Zerbst", "de", "birth_name"), ("Yekaterina Velikaya", "ru", "transliteration")],
    "Suleiman the Magnificent":  [("Suleiman I", "en", "title"), ("Kanuni Sultan Süleyman", "tr", "transliteration")],
    "Mehmed II":               [("Mehmed the Conqueror", "en", "nickname"), ("Fatih Sultan Mehmed", "tr", "title")],
    "Tamerlane":               [("Timur", "uz", "birth_name"), ("Timur-e Lang", "fa", "birth_name")],
    "Kublai Khan":             [("Setsen Khan", "mn", "title")],
    "Montezuma II":            [("Moctezuma Xocoyotzin", "nah", "birth_name")],
    "Ramesses II":             [("Ramesses the Great", "en", "nickname"), ("Usermaatre Setepenre", "egy", "birth_name")],
    "Tutankhamun":             [("King Tut", "en", "nickname"), ("Nebkheperure", "egy", "birth_name")],
    "Hannibal":                [("Hannibal Barca", "la", "birth_name")],
    "Cicero":                  [("Marcus Tullius Cicero", "la", "birth_name")],
    "Augustus":                [("Gaius Octavius", "la", "birth_name"), ("Octavian", "en", "nickname")],
    "Marcus Aurelius":         [("Marcus Aurelius Antoninus Augustus", "la", "birth_name")],
    "Nero":                    [("Nero Claudius Caesar Augustus Germanicus", "la", "birth_name")],
    "Caligula":                [("Gaius Julius Caesar Augustus Germanicus", "la", "birth_name")],
    "Constantine I":           [("Constantine the Great", "en", "nickname")],
    "Justinian I":             [("Flavius Petrus Sabbatius Iustinianus", "la", "birth_name")],
    "Attila":                  [("Scourge of God", "en", "nickname")],
    "Alfred the Great":        [("Alfred of Wessex", "en", "birth_name")],
    "William the Conqueror":   [("William I of England", "en", "title"), ("Guillaume le Conquérant", "fr", "transliteration")],
    "Salah ad-Din":            [("Saladin", "en", "transliteration")],
}

# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

async def run_etl(conn: asyncpg.Connection) -> None:
    inserted = 0
    skipped  = 0

    # Load all persons
    persons = await conn.fetch(
        "SELECT person_id, full_name FROM person LIMIT 3500"
    )
    log.info("Loaded %d persons", len(persons))

    name_to_id = {p["full_name"]: str(p["person_id"]) for p in persons}

    # Insert known aliases
    for name, aliases in KNOWN_ALIASES.items():
        person_id = name_to_id.get(name)
        if not person_id:
            # Try partial match
            for pname, pid in name_to_id.items():
                if name.lower() in pname.lower() or pname.lower() in name.lower():
                    person_id = pid
                    break

        if not person_id:
            log.warning("Person not found for alias: %s", name)
            skipped += 1
            continue

        for alias_name, lang, alias_type in aliases:
            try:
                await conn.execute(
                    INSERT_ALIAS_SQL,
                    person_id,
                    alias_name,
                    lang,
                    alias_type,
                )
                inserted += 1
            except Exception as e:
                log.warning("Failed alias %s for %s: %s", alias_name, name, e)
                skipped += 1

    # Generate additional aliases from bio_text patterns
    # Extract parenthetical alternate names like "Jean-François (Frank) Champollion"
    persons_with_bio = await conn.fetch(
        "SELECT person_id, full_name, bio_text FROM person WHERE bio_text IS NOT NULL LIMIT 1000"
    )

    for person in persons_with_bio:
        bio = person["bio_text"] or ""
        # Extract text in parentheses as potential alternate names
        matches = re.findall(r'\(([A-Z][a-zA-Z\s\-]+)\)', bio)
        for match in matches[:1]:  # Max 1 alias per person from bio
            match = match.strip()
            if len(match) > 3 and match != person["full_name"]:
                try:
                    await conn.execute(
                        INSERT_ALIAS_SQL,
                        str(person["person_id"]),
                        match,
                        "en",
                        "nickname",
                    )
                    inserted += 1
                except Exception:
                    pass

    log.info(
        "person_alias ETL complete — %d inserted, %d skipped",
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