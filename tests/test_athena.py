from economic_data.athena import load_catalog, execute_sql


def test_load_catalog():
    catalog = load_catalog()
    assert "GDPC1" in catalog.keys()


def test_execute_sql():
    assert execute_sql("SELECT 1;") == [(1,)]
