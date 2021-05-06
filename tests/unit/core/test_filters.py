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
    parquet_dir = None

    @classmethod
    def setUpClass(cls):
        cls.csv_file = 'tests/resources/datastore_unit_test/dataset/TEST_BOSTED/TEST_BOSTED__3_2.csv'
        cls.parquet_dir = 'tests/resources/datastore_unit_test/dataset/TEST_BOSTED/TEST_BOSTED__3_2'
        convert_csv_to_parquet(cls.csv_file, cls.parquet_dir, True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.parquet_dir)

    def test_by_time_period_from_7670_to_8034(self):
        print('TEST : test_filter_by_time_period_from_7670_to_8034')
        expected_data = {
            'unit_id': [1000000002, 1000000004, 1000000003, 1000000001, 1000000001, 1000000003, 1000000003],
            'value': ["8", "2", "12", "3", "16", "2", "12"],
            'start_epoch_days': [1461, 3287, 4018, 5479, 7851, 7701, 7957],
            'stop_epoch_days': [8065, 7710, 7700, 7850, 8125, 7956, np.nan]}

        expected = Table.from_pydict(expected_data)
        self.print_expected(expected)

        actual = filter_by_time_period(self.parquet_dir, 7670, 8034, None, True)
        self.print_actual(actual)

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)

    def test_by_time_period_from_7670_to_8034_excluding_attributes(self):
        print('TEST : test_filter_by_time_period_from_7670_to_8034_excluding_attributes')
        expected_data = {
            'unit_id': [1000000002, 1000000004, 1000000003, 1000000001, 1000000001, 1000000003, 1000000003],
            'value': ["8", "2", "12", "3", "16", "2", "12"]}

        expected = Table.from_pydict(expected_data)
        self.print_expected(expected)

        actual = filter_by_time_period(self.parquet_dir, 7670, 8034)
        self.print_actual(actual)

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)

    def test_by_time_period_from_7670_to_8400(self):
        print('TEST : test_filter_by_time_period_from_7670_to_8400')

        expected_data = {
            'unit_id': [1000000002, 1000000004, 1000000003, 1000000001, 1000000001, 1000000003, 1000000003, 1000000001,
                        1000000002],
            'value': ["8", "2", "12", "3", "16", "2", "12", "3", "8"],
            'start_epoch_days': [1461, 3287, 4018, 5479, 7851, 7701, 7957, 8126, 8066],
            'stop_epoch_days': [8065, 7710, 7700, 7850, 8125, 7956, np.nan, np.nan, np.nan]}

        expected = Table.from_pydict(expected_data)
        self.print_expected(expected)

        actual = filter_by_time_period(self.parquet_dir, 7670, 8400, None, True)
        self.print_actual(actual)

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)

    def test_by_time_period_from_7670_to_8400_including_attributes_and_population_filter(self):
        print('TEST : test_filter_by_time_period_from_7670_to_8400_including_attributes_and_population_filter')

        population_filter = [1000000002, 1000000003]

        expected_data = {
            'unit_id': [1000000002, 1000000003, 1000000003, 1000000003, 1000000002],
            'value': ["8", "12", "2", "12", "8"],
            'start_epoch_days': [1461, 4018, 7701, 7957, 8066],
            'stop_epoch_days': [8065, 7700, 7956, np.nan, np.nan]}

        expected = Table.from_pydict(expected_data)
        self.print_expected(expected)

        actual = filter_by_time_period(self.parquet_dir, 7670, 8400, population_filter, True)
        self.print_actual(actual)

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)

    def test_by_time(self):
        print('TEST : test_filter_by_time')
        expected_data = {
            'unit_id': [1000000002, 1000000004, 1000000003, 1000000001],
            'value': ["8", "2", "12", "3"],
            'start_epoch_days': [1461, 3287, 4018, 5479],
            'stop_epoch_days': [8065, 7710, 7700, 7850]}

        expected = Table.from_pydict(expected_data)
        self.print_expected(expected)

        actual = filter_by_time(self.parquet_dir, 7669, None, True)
        self.print_actual(actual)

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)

    def test_by_time_excluding_attributes(self):
        print('TEST : test_filter_by_time_excluding_attributes')
        expected_data = {
            'unit_id': [1000000002, 1000000004, 1000000003, 1000000001],
            'value': ["8", "2", "12", "3"]}

        expected = Table.from_pydict(expected_data)
        self.print_expected(expected)

        actual = filter_by_time(self.parquet_dir, 7669)
        self.print_actual(actual)

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)

    def test_by_time_including_attributes_and_population_filter(self):
        print('TEST : test_filter_by_time_and_population_filter')

        population_filter = [1000000002, 1000000003]

        expected_data = {
            'unit_id': [1000000002, 1000000003],
            'value': ["8", "12"],
            'start_epoch_days': [1461, 7957],
            'stop_epoch_days': [8065, np.nan]}

        expected = Table.from_pydict(expected_data)
        self.print_expected(expected)

        actual = filter_by_time(self.parquet_dir, 8034, population_filter, True)
        self.print_actual(actual)

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)

    def test_by_fixed(self):
        print('TEST : test_filter_by_fixed')

        expected_data = {
            'unit_id': [1000000002, 1000000004, 1000000003, 1000000001, 1000000001, 1000000003, 1000000003,
                        1000000001, 1000000002],
            'value': ["8", "2", "12", "3", "16", "2", "12", "3", "8"],
            'start_epoch_days': [1461, 3287, 4018, 5479, 7851, 7701, 7957, 8126, 8066],
            'stop_epoch_days': [8065, 7710, 7700, 7850, 8125, 7956, np.nan, np.nan, np.nan]}

        expected = Table.from_pydict(expected_data)
        self.print_expected(expected)

        actual = filter_by_fixed(self.parquet_dir, None, True)
        self.print_actual(actual)

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)

    def test_by_fixed_excluding_attributes(self):
        print('TEST : test_filter_by_fixed_excluding_attributes')

        expected_data = {
            'unit_id': [1000000002, 1000000004, 1000000003, 1000000001, 1000000001, 1000000003, 1000000003,
                        1000000001, 1000000002],
            'value': ["8", "2", "12", "3", "16", "2", "12", "3", "8"]}

        expected = Table.from_pydict(expected_data)
        self.print_expected(expected)

        actual = filter_by_fixed(self.parquet_dir)
        self.print_actual(actual)

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)

    def test_by_fixed_including_attributes_and_population_filter(self):
        print('TEST : test_filter_by_fixed_including_attributes_and_population_filter')

        population_filter = [1000000002, 1000000003]

        expected_data = {
            'unit_id': [1000000002, 1000000003, 1000000003, 1000000003, 1000000002],
            'value': ["8", "12", "2", "12", "8"],
            'start_epoch_days': [1461, 4018, 7701, 7957, 8066],
            'stop_epoch_days': [8065, 7700, 7956, np.nan, np.nan]}

        expected = Table.from_pydict(expected_data)
        self.print_expected(expected)

        actual = filter_by_fixed(self.parquet_dir, population_filter, True)
        self.print_actual(actual)

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)

    def test_by_time_period_non_existing_partition(self):
        print('TEST : test_filter_by_time_period_non_existing_partition')
        parquet_partition_name = 'tests/resources/unit_test_data/NO_PARTITION'

        with self.assertRaises(Exception):
            filter_by_time_period(parquet_partition_name, 7670, 8034, None, True)

    def test_by_time_non_existing_partition(self):
        print('TEST : test_filter_by_time_non_existing_partition')
        parquet_partition_name = 'tests/resources/unit_test_data/NO_PARTITION'

        with self.assertRaises(Exception):
            filter_by_time(parquet_partition_name, 7669, None, True)

    def print_expected(self, expected: Table):
        print('==================== EXPECTED ========================')
        print(expected.to_pandas())

    def print_actual(self, actual: Table):
        print('==================== ACTUAL ==========================')
        print(actual.to_pandas())
