import pytest
import uuid

PERSON_ID = str(uuid.uuid4())

@pytest.mark.asyncio
async def test_get_historical_default(client):
    ac, conn, redis = client
    conn.fetch.return_value = [
        {
            "person_id": PERSON_ID,
            "full_name": "Cleopatra",
            "birth_year": -69,
            "death_year": -30,
            "wikidata_qid": "Q1523",
            "birth_region": "Egypt",
            "total_count": 1,
        }
    ]
    response = await ac.get("/historical")
    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] == 1
    assert data["results"][0]["full_name"] == "Cleopatra"

@pytest.mark.asyncio
async def test_get_historical_year_filter(client):
    ac, conn, redis = client
    conn.fetch.return_value = []
    response = await ac.get("/historical?from_year=-500&to_year=-300")
    assert response.status_code == 200
    assert response.json()["total_count"] == 0

@pytest.mark.asyncio
async def test_get_historical_pagination(client):
    ac, conn, redis = client
    # Return 3 rows each with total_count=100 to simulate windowed count
    conn.fetch.return_value = [
        {
            "person_id": str(uuid.uuid4()),
            "full_name": f"Person {i}",
            "birth_year": -300 + i,
            "death_year": -200 + i,
            "wikidata_qid": None,
            "birth_region": None,
            "total_count": 100,
        }
        for i in range(3)
    ]
    response = await ac.get("/historical?limit=10&offset=20")
    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] == 100