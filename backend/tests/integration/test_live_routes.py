import pytest

# ── REPLACE THESE WITH REAL VALUES FROM YOUR DB ───────────────────────────
LIVE_PERSON_ID      = "00808689-8540-4350-a2aa-50111eceebb0"
LIVE_HAPLOGROUP_CODE = "E1b1b"      # a haplogroup_code that exists in haplogroup_reference
# ─────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_live_get_person(live_client):
    response = await live_client.get(f"/person/{LIVE_PERSON_ID}")
    assert response.status_code == 200
    data = response.json()
    assert "person_id" in data
    assert "full_name" in data

@pytest.mark.asyncio
async def test_live_get_person_genome(live_client):
    response = await live_client.get(f"/person/{LIVE_PERSON_ID}/genome")
    assert response.status_code == 200
    assert "genomes" in response.json()

@pytest.mark.asyncio
async def test_live_get_historical(live_client):
    response = await live_client.get("/historical?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    assert isinstance(data["results"], list)

@pytest.mark.asyncio
async def test_live_search(live_client):
    response = await live_client.get("/search?q=alexander")
    assert response.status_code == 200
    assert "results" in response.json()

@pytest.mark.asyncio
async def test_live_haplogroup_members(live_client):
    response = await live_client.get(f"/haplogroup/{LIVE_HAPLOGROUP_CODE}/members")
    assert response.status_code == 200
    assert "members" in response.json()