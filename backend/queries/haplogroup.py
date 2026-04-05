def get_haplogroup_members(haplogroup_code: str) -> tuple[str, tuple]:
    sql = """
        WITH RECURSIVE haplogroup_tree AS (
            SELECT
                hr.haplogroup_id,
                hr.haplogroup_code,
                hr.haplogroup_type,
                0 AS tree_depth
            FROM haplogroup_reference hr
            WHERE hr.haplogroup_code = $1

            UNION ALL

            SELECT
                child.haplogroup_id,
                child.haplogroup_code,
                child.haplogroup_type,
                ht.tree_depth + 1
            FROM haplogroup_reference child
            JOIN haplogroup_tree ht ON child.parent_haplogroup_id = ht.haplogroup_id
        )
        SELECT
            p.person_id,
            p.full_name,
            p.birth_year,
            p.is_historical,
            g.y_haplogroup,
            g.mt_haplogroup,
            ht.haplogroup_code AS matched_haplogroup_code,
            ht.haplogroup_type,
            ht.tree_depth
        FROM haplogroup_tree ht
        JOIN genome g ON (
            (ht.haplogroup_type = 'Y-DNA'  AND g.y_haplogroup  = ht.haplogroup_code)
            OR
            (ht.haplogroup_type = 'mtDNA'  AND g.mt_haplogroup = ht.haplogroup_code)
        )
        JOIN person p ON p.person_id = g.person_id
        ORDER BY ht.tree_depth ASC, p.birth_year ASC NULLS LAST
    """
    return sql, (haplogroup_code,)