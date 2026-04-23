from keep_alive import _HealthHandler


def test_network_healthcheck_alias_supported():
    assert "/network/healthcheck" in _HealthHandler._HEALTH_PATHS
    assert "/network>healthcheck" in _HealthHandler._HEALTH_PATHS
    assert "/network> healthcheck" in _HealthHandler._HEALTH_PATHS
