from economic_data import lambda_handlers


def test_join_tables_handler_doesnt_fail():
    lambda_handlers.join_tables_handler({}, {})
