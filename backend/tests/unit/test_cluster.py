import pytest
import uuid

CLUSTER_ID = str(uuid.uuid4())
PERSON_ID  = str(uuid.uuid4())
GENOME_ID  = str(uuid.uuid4())

@pytest.mark.asyncio
async def test_get_cluster_found(client):
    ac, conn, redis = client
    conn.fetchrow.return_value = {
        "cluster_id": CLUSTER_ID,
        "cluster_code": "EUR",
        "cluster_name": "European",
        "description": "West Eurasian cluster.",
    }
    conn.fetch.return_value = [
        {
            "person_id": PERSON_ID,
            "full_name": "Julius Caesar",
            "genome_id": GENOME_ID,
            "membership_probability": 0.92,
        }
    ]
    response = await ac.get(f"/cluster/{CLUSTER_ID}")
    assert response.status_code == 200
    data = response.json()
    assert data["cluster_code"] == "EUR"
    assert len(data["members"]) == 1

@pytest.mark.asyncio
async def test_get_cluster_not_found(client):
    ac, conn, redis = client
    conn.fetchrow.return_value = None
    response = await ac.get(f"/cluster/{uuid.uuid4()}")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_get_cluster_invalid_uuid(client):
    ac, conn, redis = client
    response = await ac.get("/cluster/not-a-uuid")
    assert response.status_code == 404