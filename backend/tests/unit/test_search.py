import pytest
import uuid

@pytest.mark.asyncio
async def test_search_returns_results(client):
    ac, pool, redis = client
    pool.fetch.return_value = [
        {
            "result_type": "person",
            "id": str(uuid.uuid4()),
            "label": "Alexander the Great",
            "rank_score": 0.92,
            "snippet": "King of Macedon...",
        }
    ]
    response = await ac.get("/search?q=Alexander")
    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    assert data["results"][0]["result_type"] == "person"

@pytest.mark.asyncio
async def test_search_empty_results(client):
    ac, pool, redis = client
    pool.fetch.return_value = []
    response = await ac.get("/search?q=zzznomatch")
    assert response.status_code == 200
    assert response.json()["results"] == []

@pytest.mark.asyncio
async def test_search_missing_query_param(client):
    ac, pool, redis = client
    response = await ac.get("/search")
    # q is required — FastAPI returns 422
    assert response.status_code == 422