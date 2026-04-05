import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


async def main():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set in .env")

    conn = await asyncpg.connect(
        DATABASE_URL,
        ssl="require",
        statement_cache_size=0
    )

    print("\n--- Fetching valid test inputs ---\n")

    # ✅ 1. Get valid person_id (best: from inferred relationships)
    person_row = await conn.fetchrow("""
        SELECT DISTINCT person_a_id
        FROM inferred_relationship
        LIMIT 1
    """)

    if person_row:
        person_id = person_row["person_a_id"]
        print(f"Valid person_id (from inferred_relationship):\n{person_id}\n")
    else:
        print("⚠️ No inferred relationships found. Falling back to genome-linked persons...\n")
        person_row = await conn.fetchrow("""
            SELECT p.person_id
            FROM person p
            JOIN genome g ON g.person_id = p.person_id
            LIMIT 1
        """)
        if person_row:
            person_id = person_row["person_id"]
            print(f"Valid person_id (fallback):\n{person_id}\n")
        else:
            print("❌ No valid person_id found.\n")
            person_id = None

    # ✅ 2. Get valid haplogroup_code (actually used in genomes)
    haplo_row = await conn.fetchrow("""
        SELECT DISTINCT y_haplogroup
        FROM genome
        WHERE y_haplogroup IS NOT NULL
        LIMIT 1
    """)

    if haplo_row:
        haplogroup_code = haplo_row["y_haplogroup"]
        print(f"Valid haplogroup_code (from genome):\n{haplogroup_code}\n")
    else:
        print("⚠️ No Y haplogroups found. Trying mtDNA...\n")
        haplo_row = await conn.fetchrow("""
            SELECT DISTINCT mt_haplogroup
            FROM genome
            WHERE mt_haplogroup IS NOT NULL
            LIMIT 1
        """)
        if haplo_row:
            haplogroup_code = haplo_row["mt_haplogroup"]
            print(f"Valid haplogroup_code (mtDNA fallback):\n{haplogroup_code}\n")
        else:
            print("❌ No haplogroup_code found.\n")
            haplogroup_code = None

    print("--- Done ---\n")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())