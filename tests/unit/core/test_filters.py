import unittest

import numpy as np
from pandas.testing import assert_frame_equal
from pyarrow import Table

from data_service.core.filters import filter_by_time_period
from tests.unit.util.util import convert_csv_to_parquet


class TestFilters(unittest.TestCase):

    def test_filter_by_time_period_table(self):
        csv_file = 'tests/resources/unit_test_data/TEST_BOSTED__3_2.csv'
        parquet_partition_name = 'tests/resources/unit_test_data/TEST_BOSTED__3_2'

        convert_csv_to_parquet(csv_file, parquet_partition_name)
        print(parquet_partition_name)

        # Rekkefølgen av records her matcher rekkefølgen som blir returnert etter filtrering i partisjonert parquet.
        dict = {
            'unit_id': [1000000002, 1000000004, 1000000003, 1000000001, 1000000001, 1000000003, 1000000003],
            'value': ["8", "2", "12", "3", "16", "2", "12"],
            'start_epoch_days': [1461, 3287, 4018, 5479, 7851, 7701, 7957],
            'stop_epoch_days': [8065, 7710, 7700, 7850, 8125, 7956, np.nan]}

        # Rekkefølgen av records her er det samme som i Datastore-API
        # dict = {
        #     'unit_id': [1000000001, 1000000001, 1000000002, 1000000003, 1000000003, 1000000003, 1000000004],
        #     'value': ["3", "16", "8", "12", "2", "12", "2"],
        #     'start_epoch_days': [5479, 7851, 1461, 4018, 7701, 7957, 3287],
        #     'stop_epoch_days': [7850, 8125, 8065, 7700, 7956, np.nan, 7710]}

        expected = Table.from_pydict(dict)

        print('======== EXPECTED ==================')
        print(expected.to_pandas())
        print('==========================')

        actual = filter_by_time_period(parquet_partition_name, 7670, 8034)

        print('======== ACTUAL ==================')
        print(actual.to_pandas())
        print('==========================')

        assert_frame_equal(expected.to_pandas(), actual.to_pandas(), check_dtype=False)
