from uuid import UUID


def get_timeline(
    person_id: UUID,
    from_year: int | None = None,
    to_year: int | None = None,
) -> tuple[str, tuple]:
    sql = """
        SELECT
            plt.location_event_id,
            plt.event_type,
            plt.event_year,
            plt.event_year_end,
            plt.certainty,
            r.region_id,
            r.region_name,
            r.modern_country,
            r.region_type,
            ds.dataset_name AS source_dataset
        FROM person_location_timeline plt
        JOIN region          r  ON r.region_id   = plt.region_id
        LEFT JOIN dataset_source ds ON ds.dataset_id = plt.source_dataset_id
        WHERE plt.person_id = $1
          AND ($2::INTEGER IS NULL OR plt.event_year >= $2)
          AND ($3::INTEGER IS NULL OR plt.event_year <= $3)
        ORDER BY plt.event_year ASC NULLS LAST
    """
    return sql, (person_id, from_year, to_year)