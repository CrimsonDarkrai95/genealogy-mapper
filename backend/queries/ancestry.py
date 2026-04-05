from uuid import UUID


def get_ancestry(
    person_id: UUID,
    depth: int = 10,
    min_confidence: float | None = None,
) -> tuple[str, tuple]:
    sql = """
        WITH RECURSIVE ancestry_walk AS (
            SELECT
                ir.relationship_id,
                ir.person_a_id,
                ir.person_b_id,
                ir.relationship_type,
                ir.generational_distance,
                ir.confidence_level,
                ir.inference_method,
                ir.supporting_evidence,
                1 AS depth
            FROM inferred_relationship ir
            WHERE ir.person_a_id = $1
              AND ir.relationship_type IN ('ancestor', 'distant_relative')
              AND ir.confidence_level >= COALESCE($3::NUMERIC, 0.0)

            UNION ALL

            SELECT
                ir.relationship_id,
                ir.person_a_id,
                ir.person_b_id,
                ir.relationship_type,
                ir.generational_distance,
                ir.confidence_level,
                ir.inference_method,
                ir.supporting_evidence,
                aw.depth + 1
            FROM inferred_relationship ir
            JOIN ancestry_walk aw ON aw.person_b_id = ir.person_a_id
            WHERE ir.relationship_type IN ('ancestor', 'distant_relative')
              AND ir.confidence_level >= COALESCE($3::NUMERIC, 0.0)
              AND aw.depth < $2
        )
        SELECT
            aw.depth             AS generation,
            p.person_id,
            p.full_name,
            p.birth_year,
            p.death_year,
            p.is_historical,
            aw.relationship_type,
            aw.confidence_level,
            aw.inference_method,
            aw.supporting_evidence
        FROM ancestry_walk aw
        JOIN person p ON p.person_id = aw.person_b_id
        ORDER BY aw.depth ASC, aw.confidence_level DESC
    """
    return sql, (person_id, depth, min_confidence)