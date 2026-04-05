from uuid import UUID


def get_person(person_id: UUID) -> tuple[str, tuple]:
    sql = """
        SELECT
            p.person_id,
            p.full_name,
            p.birth_name,
            p.is_historical,
            p.birth_year,
            p.death_year,
            p.gender,
            p.wikidata_qid,
            p.bio_text,
            r.region_name        AS birth_region,
            r.modern_country     AS birth_region_country,
            ds.dataset_name      AS source_dataset,
            COALESCE(
                json_agg(DISTINCT jsonb_build_object(
                    'alias_name',    pa.alias_name,
                    'language_code', pa.language_code,
                    'alias_type',    pa.alias_type
                )) FILTER (WHERE pa.alias_id IS NOT NULL),
                '[]'
            ) AS aliases,
            COALESCE(
                json_agg(DISTINCT jsonb_build_object(
                    'source_name',  pe.source_name,
                    'external_key', pe.external_key,
                    'url',          pe.url
                )) FILTER (WHERE pe.ext_id IS NOT NULL),
                '[]'
            ) AS external_ids
        FROM person p
        LEFT JOIN region             r  ON r.region_id  = p.birth_region_id
        LEFT JOIN dataset_source     ds ON ds.dataset_id = p.source_dataset_id
        LEFT JOIN person_alias       pa ON pa.person_id  = p.person_id
        LEFT JOIN person_external_id pe ON pe.person_id  = p.person_id
        WHERE p.person_id = $1
        GROUP BY p.person_id, r.region_name, r.modern_country, ds.dataset_name
    """
    return sql, (person_id,)


def get_person_genome(person_id: UUID) -> tuple[str, tuple]:
    sql = """
        SELECT
            g.genome_id,
            g.y_haplogroup,
            g.mt_haplogroup,
            g.coverage_depth,
            g.coverage_breadth,
            g.endogenous_dna_pct,
            g.damage_pattern,
            g.assembly_reference,
            g.raw_metadata,
            g.ingested_at,
            ds.dataset_name AS dataset,
            ds.short_code   AS dataset_code
        FROM genome g
        JOIN dataset_source ds ON ds.dataset_id = g.dataset_id
        WHERE g.person_id = $1
        ORDER BY g.ingested_at DESC
    """
    return sql, (person_id,)


def get_person_matches(
    person_id: UUID,
    method: str | None = None,
    min_score: float | None = None,
    limit: int | None = None,
) -> tuple[str, tuple]:
    sql = """
        SELECT
            gm.match_id,
            gm.similarity_score,
            gm.shared_segment_count,
            gm.total_shared_cm,
            gm.longest_segment_cm,
            gm.snp_overlap_count,
            gm.matching_method,
            gm.confidence_score,
            gm.computed_at,
            CASE
                WHEN ga.person_id = $1 THEN p_b.person_id
                ELSE p_a.person_id
            END AS counterpart_person_id,
            CASE
                WHEN ga.person_id = $1 THEN p_b.full_name
                ELSE p_a.full_name
            END AS counterpart_full_name,
            RANK() OVER (
                PARTITION BY gm.matching_method
                ORDER BY gm.similarity_score DESC
            ) AS rank_within_method
        FROM genome_match gm
        JOIN genome  ga ON ga.genome_id  = gm.genome_a_id
        JOIN genome  gb ON gb.genome_id  = gm.genome_b_id
        JOIN person p_a ON p_a.person_id = ga.person_id
        JOIN person p_b ON p_b.person_id = gb.person_id
        WHERE (ga.person_id = $1 OR gb.person_id = $1)
          AND ($2::TEXT    IS NULL OR gm.matching_method  = $2)
          AND gm.similarity_score >= COALESCE($3::NUMERIC, 0)
        ORDER BY gm.similarity_score DESC
        LIMIT COALESCE($4::INTEGER, 50)
    """
    return sql, (person_id, method, min_score, limit)


def get_person_lineage(person_id: UUID) -> tuple[str, tuple]:
    sql = """
        SELECT
            dm.membership_id,
            dm.role,
            dm.start_year,
            dm.end_year,
            dm.is_founding_member,
            d.dynasty_id,
            d.dynasty_name,
            d.founding_year,
            d.dissolution_year,
            r.region_name    AS origin_region,
            ds.dataset_name  AS source_dataset
        FROM dynasty_membership dm
        JOIN dynasty         d  ON d.dynasty_id  = dm.dynasty_id
        LEFT JOIN region     r  ON r.region_id   = d.origin_region_id
        LEFT JOIN dataset_source ds ON ds.dataset_id = dm.source_dataset_id
        WHERE dm.person_id = $1
        ORDER BY dm.start_year ASC NULLS LAST
    """
    return sql, (person_id,)