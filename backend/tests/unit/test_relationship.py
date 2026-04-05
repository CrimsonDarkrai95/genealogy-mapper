import pytest
import uuid

REL_ID    = str(uuid.uuid4())
PERSON_A  = str(uuid.uuid4())
PERSON_B  = str(uuid.uuid4())
MATCH_ID  = str(uuid.uuid4())

@pytest.mark.asyncio
async def test_get_relationship_found(client):
    ac, conn, redis = client
    conn.fetchrow.return_value = {
        "relationship_id": REL_ID,
        "person_a_id": PERSON_A,
        "person_a_name": "Genghis Khan",
        "person_b_id": PERSON_B,
        "person_b_name": "Kublai Khan",
        "match_id": MATCH_ID,
        "relationship_type": "ancestor",
        "generational_distance": 2,
        "confidence_level": 0.87,
        "inference_method": "haplogroup_tree",
        "supporting_evidence": {"haplogroup": "C2"},
        "inferred_at": "2024-01-01T00:00:00+00:00",
    }
    response = await ac.get(f"/relationship/{REL_ID}")
    assert response.status_code == 200
    data = response.json()
    assert data["relationship_id"] == REL_ID
    assert data["relationship_type"] == "ancestor"
    assert data["generational_distance"] == 2

@pytest.mark.asyncio
async def test_get_relationship_not_found(client):
    ac, conn, redis = client
    conn.fetchrow.return_value = None
    response = await ac.get(f"/relationship/{uuid.uuid4()}")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_get_relationship_invalid_uuid(client):
    ac, conn, redis = client
    response = await ac.get("/relationship/bad-uuid")
    assert response.status_code == 404