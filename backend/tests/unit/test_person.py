import pytest
from unittest.mock import AsyncMock, patch
import uuid

PERSON_ID = str(uuid.uuid4())
GENOME_ID = str(uuid.uuid4())
MATCH_ID  = str(uuid.uuid4())
REL_ID    = str(uuid.uuid4())

# ── /person/{id} ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_person_found(client):
    ac, conn, redis = client
    conn.fetchrow.return_value = {
    "person_id": PERSON_ID,
    "full_name": "Alexander the Great",
    "birth_name": None,
    "birth_year": -356,
    "death_year": -323,
    "is_historical": True,
    "wikidata_qid": "Q8409",
    "gender": "male",
    "region_id": 1,
    "region_name": "Macedonia",
    "modern_country": "GR",
    "region_type": "country",
}
    conn.fetch.return_value = []  # aliases and external_ids

    response = await ac.get(f"/person/{PERSON_ID}")
    assert response.status_code == 200
    data = response.json()
    assert data["person_id"] == PERSON_ID
    assert data["full_name"] == "Alexander the Great"
    assert data["birth_year"] == -356

@pytest.mark.asyncio
async def test_get_person_not_found(client):
    ac, conn, redis = client
    conn.fetchrow.return_value = None
    response = await ac.get(f"/person/{uuid.uuid4()}")
    assert response.status_code == 404

# ── /person/{id}/genome ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_person_genome_found(client):
    ac, conn, redis = client
    conn.fetch.return_value = [
        {
            "genome_id": GENOME_ID,
            "person_id": PERSON_ID,
            "dataset_id": 1,
            "y_haplogroup": "R1b",
            "mt_haplogroup": "H2a",
            "coverage_depth": 12.5,
            "coverage_breadth": 0.85,
            "endogenous_dna_pct": 0.72,
            "damage_pattern": "high",
            "assembly_reference": "GRCh38",
            "raw_metadata": {},
            "ingested_at": "2024-01-01T00:00:00+00:00",
            "dataset_name": "AADR",
        }
    ]
    response = await ac.get(f"/person/{PERSON_ID}/genome")
    assert response.status_code == 200
    data = response.json()
    assert "genomes" in data
    assert data["genomes"][0]["y_haplogroup"] == "R1b"

@pytest.mark.asyncio
async def test_get_person_genome_empty(client):
    ac, conn, redis = client
    conn.fetch.return_value = []
    response = await ac.get(f"/person/{PERSON_ID}/genome")
    assert response.status_code == 200
    assert response.json()["genomes"] == []

# ── /person/{id}/matches ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_person_matches(client):
    ac, conn, redis = client
    conn.fetch.return_value = [
        {
            "match_id": MATCH_ID,
            "counterpart_person_id": str(uuid.uuid4()),
            "counterpart_name": "Julius Caesar",
            "similarity_score": 0.723456,
            "shared_segment_count": 14,
            "total_shared_cm": 312.5,
            "matching_method": "ibd",
            "confidence_score": 0.88,
            "computed_at": "2024-01-01T00:00:00+00:00",
        }
    ]
    response = await ac.get(f"/person/{PERSON_ID}/matches")
    assert response.status_code == 200
    data = response.json()
    assert "matches" in data
    assert data["matches"][0]["similarity_score"] == pytest.approx(0.723456, rel=1e-4)

@pytest.mark.asyncio
async def test_get_person_matches_empty(client):
    ac, conn, redis = client
    conn.fetch.return_value = []
    response = await ac.get(f"/person/{PERSON_ID}/matches")
    assert response.status_code == 200
    assert response.json()["matches"] == []

# ── /person/{id}/ancestry ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_person_ancestry(client):
    ac, conn, redis = client
    conn.fetch.return_value = [
    {
        "generation": 1,
        "person_id": str(uuid.uuid4()),
        "full_name": "Philip II of Macedon",
        "relationship_type": "ancestor",
        "confidence_level": 0.91,
        "inference_method": "rule_based",
    }
]
    response = await ac.get(f"/person/{PERSON_ID}/ancestry")
    assert response.status_code == 200
    data = response.json()
    assert "ancestry_chain" in data
    assert data["ancestry_chain"][0]["full_name"] == "Philip II of Macedon"

@pytest.mark.asyncio
async def test_get_person_ancestry_empty(client):
    ac, conn, redis = client
    conn.fetch.return_value = []
    response = await ac.get(f"/person/{PERSON_ID}/ancestry")
    assert response.status_code == 200
    assert response.json()["ancestry_chain"] == []

# ── /person/{id}/lineage ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_person_lineage(client):
    ac, conn, redis = client
    conn.fetch.return_value = [
        {
            "dynasty_id": str(uuid.uuid4()),
            "dynasty_name": "Argead dynasty",
            "role": "emperor",
            "start_year": -336,
            "end_year": -323,
            "is_founding_member": False,
            "origin_region": "Macedonia",
        }
    ]
    response = await ac.get(f"/person/{PERSON_ID}/lineage")
    assert response.status_code == 200
    data = response.json()
    assert "memberships" in data
    assert data["memberships"][0]["dynasty_name"] == "Argead dynasty"

@pytest.mark.asyncio
async def test_get_person_lineage_empty(client):
    ac, conn, redis = client
    conn.fetch.return_value = []
    response = await ac.get(f"/person/{PERSON_ID}/lineage")
    assert response.status_code == 200
    assert response.json()["memberships"] == []