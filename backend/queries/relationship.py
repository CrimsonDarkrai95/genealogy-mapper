from uuid import UUID


def get_relationship(relationship_id: UUID) -> tuple[str, tuple]:
    sql = """
        SELECT
            ir.relationship_id,
            ir.relationship_type,
            ir.generational_distance,
            ir.confidence_level,
            ir.inference_method,
            ir.supporting_evidence,
            ir.inferred_at,
            p_a.person_id   AS person_a_id,
            p_a.full_name   AS person_a_name,
            p_a.birth_year  AS person_a_birth_year,
            p_b.person_id   AS person_b_id,
            p_b.full_name   AS person_b_name,
            p_b.birth_year  AS person_b_birth_year,
            gm.similarity_score,
            gm.matching_method,
            gm.total_shared_cm
        FROM inferred_relationship ir
        JOIN person p_a  ON p_a.person_id = ir.person_a_id
        JOIN person p_b  ON p_b.person_id = ir.person_b_id
        LEFT JOIN genome_match gm ON gm.match_id = ir.match_id
        WHERE ir.relationship_id = $1
    """
    return sql, (relationship_id,)