import pytest


@pytest.mark.asyncio
async def test_health_unit(client):
    """
    Basic health endpoint reachability check.
    Full DB + Redis liveness is covered by integration/test_health.py.
    """
    ac, conn, redis = client
    response = await ac.get("/health")
    # Status is 200 regardless of db/redis state — route catches exceptions internally
    assert response.status_code == 200
    data = response.json()
    assert "status" in data