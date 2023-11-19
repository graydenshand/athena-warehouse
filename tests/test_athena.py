from economic_data import athena


def test_execute_sql():
    assert athena.execute_sql("SELECT 1;") == [(1,)]


def test_create_days_table():
    athena.create_days_table()


def test_build_joined_table():
    athena.build_joined_table()
