import pytest
import uuid

PERSON_ID = str(uuid.uuid4())


@pytest.mark.asyncio
async def test_haplogroup_members_found(client):
    ac, conn, redis = client
    conn.fetch.return_value = [
        {
            "person_id": PERSON_ID,
            "full_name": "Alexander the Great",
            "is_historical": True,
            "y_haplogroup": "R1b1a2",
            "mt_haplogroup": None,
        }
    ]
    response = await ac.get("/haplogroup/R1b/members")
    assert response.status_code == 200
    data = response.json()
    assert "members" in data
    assert data["members"][0]["full_name"] == "Alexander the Great"


@pytest.mark.asyncio
async def test_haplogroup_members_empty(client):
    ac, conn, redis = client
    conn.fetch.return_value = []
    response = await ac.get("/haplogroup/ZZZZ/members")
    # Route raises 404 when no members found — this is correct behaviour
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_haplogroup_members_found_mt(client):
    """Member matched via mt_haplogroup instead of y_haplogroup."""
    ac, conn, redis = client
    conn.fetch.return_value = [
        {
            "person_id": str(uuid.uuid4()),
            "full_name": "Cleopatra",
            "is_historical": True,
            "y_haplogroup": None,
            "mt_haplogroup": "H2a",
        }
    ]
    response = await ac.get("/haplogroup/H2a/members")
    assert response.status_code == 200
    data = response.json()
    assert data["members"][0]["haplogroup_exact_code"] == "H2a"