from hasql.metrics import CalculateMetrics, HasqlMetrics


def test_metrics_returns_hasql_metrics():
    cm = CalculateMetrics()
    m = cm.metrics()
    assert isinstance(m, HasqlMetrics)
    assert m.pool == 0
    assert m.pool_time == 0.0
    assert m.acquire == {}
    assert m.acquire_time == {}
    assert m.add_connections == {}
    assert m.remove_connections == {}


def test_with_get_pool_increments_counter():
    cm = CalculateMetrics()
    with cm.with_get_pool():
        pass
    m = cm.metrics()
    assert m.pool == 1
    assert m.pool_time > 0


def test_with_acquire_increments_counter():
    cm = CalculateMetrics()
    with cm.with_acquire("host1:5432"):
        pass
    m = cm.metrics()
    assert m.acquire == {"host1:5432": 1}
    assert m.acquire_time["host1:5432"] > 0


def test_add_remove_connection():
    cm = CalculateMetrics()
    cm.add_connection("host1:5432")
    cm.add_connection("host1:5432")
    cm.add_connection("host2:5432")
    assert cm.metrics().add_connections == {"host1:5432": 2, "host2:5432": 1}

    cm.remove_connection("host1:5432")
    assert cm.metrics().remove_connections == {"host1:5432": 1}


def test_multiple_get_pool_calls():
    cm = CalculateMetrics()
    with cm.with_get_pool():
        pass
    with cm.with_get_pool():
        pass
    with cm.with_get_pool():
        pass
    assert cm.metrics().pool == 3


def test_multiple_acquires_different_hosts():
    cm = CalculateMetrics()
    with cm.with_acquire("host1"):
        pass
    with cm.with_acquire("host2"):
        pass
    with cm.with_acquire("host1"):
        pass
    m = cm.metrics()
    assert m.acquire == {"host1": 2, "host2": 1}
