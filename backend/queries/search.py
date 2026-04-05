def get_search_results(q: str) -> tuple[str, tuple]:
    sql = """
        SELECT
            'person'           AS result_type,
            p.person_id::TEXT  AS id,
            p.full_name        AS label,
            ts_rank(p.bio_tsv, plainto_tsquery('english', $1)) AS rank_score,
            ts_headline(
                'english', COALESCE(p.bio_text, ''),
                plainto_tsquery('english', $1),
                'MaxWords=20, MinWords=10, StartSel=**, StopSel=**'
            ) AS snippet
        FROM person p
        WHERE p.bio_tsv @@ plainto_tsquery('english', $1)

        UNION ALL

        SELECT
            'dynasty'              AS result_type,
            d.dynasty_id::TEXT     AS id,
            d.dynasty_name         AS label,
            ts_rank(d.description_tsv, plainto_tsquery('english', $1)) AS rank_score,
            ts_headline(
                'english', COALESCE(d.description, ''),
                plainto_tsquery('english', $1),
                'MaxWords=20, MinWords=10, StartSel=**, StopSel=**'
            ) AS snippet
        FROM dynasty d
        WHERE d.description_tsv @@ plainto_tsquery('english', $1)

        UNION ALL

        SELECT
            'dataset'              AS result_type,
            ds.dataset_id::TEXT    AS id,
            ds.dataset_name        AS label,
            ts_rank(ds.description_tsv, plainto_tsquery('english', $1)) AS rank_score,
            ts_headline(
                'english', COALESCE(ds.description, ''),
                plainto_tsquery('english', $1),
                'MaxWords=20, MinWords=10, StartSel=**, StopSel=**'
            ) AS snippet
        FROM dataset_source ds
        WHERE ds.description_tsv @@ plainto_tsquery('english', $1)

        ORDER BY rank_score DESC
        LIMIT 50
    """
    return sql, (q,)