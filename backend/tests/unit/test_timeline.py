import pytest
import uuid

PERSON_ID = str(uuid.uuid4())

@pytest.mark.asyncio
async def test_get_timeline_found(client):
    ac, pool, redis = client
    pool.fetch.return_value = [
        {
            "event_type": "birth",
            "event_year": -356,
            "event_year_end": None,
            "region_name": "Pella",
            "certainty": "confirmed",
        },
        {
            "event_type": "death",
            "event_year": -323,
            "event_year_end": None,
            "region_name": "Babylon",
            "certainty": "confirmed",
        },
    ]
    response = await ac.get(f"/timeline?person_id={PERSON_ID}&from=-400&to=-300")
    assert response.status_code == 200
    data = response.json()
    assert "events" in data
    assert len(data["events"]) == 2
    assert data["events"][0]["event_type"] == "birth"

@pytest.mark.asyncio
async def test_get_timeline_empty(client):
    ac, pool, redis = client
    pool.fetch.return_value = []
    response = await ac.get(f"/timeline?person_id={PERSON_ID}&from=-400&to=-300")
    assert response.status_code == 200
    assert response.json()["events"] == []

@pytest.mark.asyncio
async def test_get_timeline_missing_params(client):
    ac, pool, redis = client
    response = await ac.get("/timeline")
    assert response.status_code == 422