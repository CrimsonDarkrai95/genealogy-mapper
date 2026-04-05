from uuid import UUID


def get_match(match_id: UUID) -> tuple[str, tuple]:
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
            p_a.person_id    AS person_a_id,
            p_a.full_name    AS person_a_name,
            ga.y_haplogroup  AS person_a_y_haplogroup,
            ga.mt_haplogroup AS person_a_mt_haplogroup,
            p_b.person_id    AS person_b_id,
            p_b.full_name    AS person_b_name,
            gb.y_haplogroup  AS person_b_y_haplogroup,
            gb.mt_haplogroup AS person_b_mt_haplogroup
        FROM genome_match gm
        JOIN genome  ga ON ga.genome_id  = gm.genome_a_id
        JOIN genome  gb ON gb.genome_id  = gm.genome_b_id
        JOIN person p_a ON p_a.person_id = ga.person_id
        JOIN person p_b ON p_b.person_id = gb.person_id
        WHERE gm.match_id = $1
    """
    return sql, (match_id,)