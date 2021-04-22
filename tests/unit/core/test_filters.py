import unittest

import glob
from data_service.core.filters import filter_by_time_period
from tests.unit.util.util import convert_csv_to_parquet


class TestFilters(unittest.TestCase):

    def test_filter_by_time_period(self):
        # ../../tests/resources/unit_test_data/TEST_BOSTED__3_2.csv
        # Dette på grunn av OSError: [Errno 30] Read-only file system: '/resources'
        csv_file = 'TEST_BOSTED__3_2.csv'
        parquet_path = 'TEST_BOSTED__3_2'


        convert_csv_to_parquet(csv_file, parquet_path, False)

        parquet_file = glob.glob("TEST_BOSTED__3_2/*.parquet")[0]

        print(parquet_file)
        # filter_by_time_period()
