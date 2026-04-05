import pytest
import uuid

MATCH_ID   = str(uuid.uuid4())
PERSON_A   = str(uuid.uuid4())
PERSON_B   = str(uuid.uuid4())
GENOME_A   = str(uuid.uuid4())
GENOME_B   = str(uuid.uuid4())

@pytest.mark.asyncio
async def test_get_match_found(client):
    ac, conn, redis = client
    conn.fetchrow.return_value = {
        "match_id": MATCH_ID,
        "genome_a_id": GENOME_A,
        "genome_b_id": GENOME_B,
        "person_a_id": PERSON_A,
        "person_a_name": "Alexander the Great",
        "person_b_id": PERSON_B,
        "person_b_name": "Julius Caesar",
        "similarity_score": 0.612345,
        "shared_segment_count": 8,
        "total_shared_cm": 201.3,
        "longest_segment_cm": 45.2,
        "snp_overlap_count": 1200,
        "matching_method": "ibd",
        "confidence_score": 0.79,
        "computed_at": "2024-01-01T00:00:00+00:00",
    }
    response = await ac.get(f"/match/{MATCH_ID}")
    assert response.status_code == 200
    data = response.json()
    assert data["match_id"] == MATCH_ID
    assert data["similarity_score"] == pytest.approx(0.612345, rel=1e-4)

@pytest.mark.asyncio
async def test_get_match_not_found(client):
    ac, conn, redis = client
    conn.fetchrow.return_value = None
    response = await ac.get(f"/match/{uuid.uuid4()}")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_get_match_invalid_uuid(client):
    ac, conn, redis = client
    response = await ac.get("/match/not-a-uuid")
    # FastAPI should return 422 for invalid UUID path param
    assert response.status_code == 404