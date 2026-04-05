from uuid import UUID


def get_historical(
    dynasty_id: UUID | None = None,
    region_id: int | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[str, tuple]:
    sql = """
        SELECT
            p.person_id,
            p.full_name,
            p.birth_year,
            p.death_year,
            p.gender,
            p.wikidata_qid,
            r.region_name    AS birth_region,
            r.modern_country AS birth_region_country,
            COUNT(*) OVER () AS total_count
        FROM person p
        LEFT JOIN region            r  ON r.region_id  = p.birth_region_id
        LEFT JOIN dynasty_membership dm ON dm.person_id = p.person_id
        WHERE p.is_historical = true
          AND ($1::UUID    IS NULL OR dm.dynasty_id  = $1)
          AND ($2::INTEGER IS NULL OR p.birth_region_id = $2)
          AND ($3::INTEGER IS NULL OR p.birth_year  >= $3)
          AND ($4::INTEGER IS NULL OR p.death_year  <= $4)
        GROUP BY p.person_id, r.region_name, r.modern_country
        ORDER BY p.birth_year ASC NULLS LAST
        LIMIT  $5
        OFFSET $6
    """
    return sql, (dynasty_id, region_id, from_year, to_year, limit, offset)