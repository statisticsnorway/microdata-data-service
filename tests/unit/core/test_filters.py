import shutil
import unittest

import numpy as np
from pandas.testing import assert_frame_equal
from pyarrow import Table

from data_service.core.filters import filter_by_fixed
from data_service.core.filters import filter_by_time
from data_service.core.filters import filter_by_time_period
from tests.unit.util.util import convert_csv_to_parquet


class TestFilters(unittest.TestCase):
    csv_file = None
    parquet_partition_name = None

    @classmethod
    def setUpClass(cls):
        cls.csv_file = 'tests/resources/unit_test_data/TEST_BOSTED__3_2.csv'
        cls.parquet_partition_name = 'tests/resources/unit_test_data/TEST_BOSTED__3_2'
        convert_csv_to_parquet(cls.csv_file, cls.parquet_partition_name)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.parquet_partition_name)

    def test_filter_by_time_period_from_7670_to_8034(self):
        print('TEST : test_filter_by_time_period_from_7670_to_8034')
        expected_data = {
            'unit_id': [1000000002, 1000000004, 1000000003, 1000000001, 1000000001, 1000000003, 1000000003],
            'value': ["8", "2", "12", "3", "16", "2", "12"],
            'start_epoch_days': [1461, 3287, 4018, 5479, 7851, 7701, 7957],
            'stop_epoch_days': [8065, 7710, 7700, 7850, 8125, 7956, np.nan]}

        expected = Table.from_pydict(expected_data)

        print('======== EXPECTED ==================')
        print(expected.to_pandas())

        actual = filter_by_time_period(self.parquet_partition_name, 7670, 8034)

        print('======== ACTUAL ==================')
        print(actual.to_pandas())

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)

    def test_filter_by_time_period_from_7670_to_8400(self):
        print('TEST : test_filter_by_time_period_from_7670_to_8400')

        expected_data = {
            'unit_id': [1000000002, 1000000004, 1000000003, 1000000001, 1000000001, 1000000003, 1000000003, 1000000001,
                        1000000002],
            'value': ["8", "2", "12", "3", "16", "2", "12", "3", "8"],
            'start_epoch_days': [1461, 3287, 4018, 5479, 7851, 7701, 7957, 8126, 8066],
            'stop_epoch_days': [8065, 7710, 7700, 7850, 8125, 7956, np.nan, np.nan, np.nan]}

        expected = Table.from_pydict(expected_data)

        print('======== EXPECTED ==================')
        print(expected.to_pandas())

        actual = filter_by_time_period(self.parquet_partition_name, 7670, 8400)

        print('======== ACTUAL ==================')
        print(actual.to_pandas())

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)

    def test_filter_by_time(self):
        print('TEST : test_filter_by_time')
        expected_data = {
            'unit_id': [1000000002, 1000000004, 1000000003, 1000000001],
            'value': ["8", "2", "12", "3"],
            'start_epoch_days': [1461, 3287, 4018, 5479],
            'stop_epoch_days': [8065, 7710, 7700, 7850]}

        expected = Table.from_pydict(expected_data)

        print('======== EXPECTED ==================')
        print(expected.to_pandas())

        actual = filter_by_time(self.parquet_partition_name, 7669)

        print('======== ACTUAL ==================')
        print(actual.to_pandas())

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)

    def test_filter_by_fixed(self):
        print('TEST : test_filter_by_fixed')
        try:
            filter_by_fixed()
        except Exception:
            assert True
        else:
            self.fail('Exception not raised')
