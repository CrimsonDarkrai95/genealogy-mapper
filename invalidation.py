from cache.client import get_redis
from cache.ancestry import invalidate_ancestry
from cache.haplogroup import invalidate_haplogroup
from cache.cluster import invalidate_cluster
from cache.query import delete_cached
from cache.keys import key_person, key_genome


async def on_genome_ingested(person_id: str, cluster_ids: list[str]) -> None:
    """
    Call after a new genome row is written for a person.
    Clears person-level and cluster-level caches.
    """
    await delete_cached(key_person(person_id))
    await delete_cached(key_genome(person_id))
    await invalidate_ancestry(person_id)

    for cluster_id in cluster_ids:
        await invalidate_cluster(cluster_id)


async def on_relationship_batch(person_ids: list[str]) -> None:
    """
    Call after inferred_relationship batch compute completes.
    Clears ancestry path cache for all affected persons.
    """
    for pid in person_ids:
        await invalidate_ancestry(pid)
        await delete_cached(key_person(pid))


async def on_haplogroup_update(haplogroup_codes: list[str]) -> None:
    """
    Call when haplogroup_reference tree is refreshed.
    """
    for code in haplogroup_codes:
        await invalidate_haplogroup(code)
