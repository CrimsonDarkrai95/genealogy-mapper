from uuid import UUID


def get_cluster_members(cluster_id: UUID) -> tuple[str, tuple]:
    sql = """
        SELECT
            pc.cluster_id,
            pc.cluster_code,
            pc.cluster_name,
            pc.haplogroup_signature,
            pc.time_period_start,
            pc.time_period_end,
            pc.description,
            p.person_id,
            p.full_name,
            p.birth_year,
            p.is_historical,
            g.genome_id,
            g.y_haplogroup,
            g.mt_haplogroup,
            gca.membership_probability,
            gca.assignment_method
        FROM genome_cluster_assignment gca
        JOIN population_cluster pc ON pc.cluster_id = gca.cluster_id
        JOIN genome             g  ON g.genome_id   = gca.genome_id
        JOIN person             p  ON p.person_id   = g.person_id
        WHERE gca.cluster_id = $1
        ORDER BY gca.membership_probability DESC
    """
    return sql, (cluster_id,)