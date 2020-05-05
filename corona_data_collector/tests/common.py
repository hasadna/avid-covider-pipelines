import logging


def test_corona_bot_answers(actual_row_callback, expected_data, extra_row_test_callback=None):

    def _test(rows):
        logging.info("Testing corona_bot_answers...")
        for row in rows:
            yield row
            expected_row = expected_data[row["id"]]
            actual_row = [rows.res.name, *actual_row_callback(row)]
            assert expected_row == actual_row, "%s: %s" % (row["id"], actual_row)
            if extra_row_test_callback:
                extra_row_test_callback(row)
        logging.info("Testing completed successfully")

    return _test
