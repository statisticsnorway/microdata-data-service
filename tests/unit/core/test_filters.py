import glob
import unittest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

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

        # parquet_file = 'TEST_BOSTED__3_2/1a5324c3d6844e1788f6719b934dc069.parquet'
        print(parquet_file)

        expected = pd.DataFrame(
            {'unit_id': pd.Series([1000000001, 1000000001, 1000000002, 1000000003, 1000000003, 1000000003, 1000000004],
                                  dtype='uint64'),
             'value': pd.Series(["3", "16", "8", "12", "2", "12", "2"], dtype='str'),
             'start_epoch_days': pd.Series([5479, 7851, 1461, 4018, 7701, 7957, 3287], dtype='int16'),
             'stop_epoch_days': pd.Series([7850, 8125, 8065, 7700, 7956, np.nan, 7710], dtype='float')})

        result = filter_by_time_period(parquet_file, 7670, 8034)
        assert_frame_equal(result.to_pandas(), expected)
