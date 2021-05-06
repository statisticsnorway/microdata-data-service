import unittest

from tests.unit.util.util import convert_csv_to_parquet


class TestSomething(unittest.TestCase):

    # Skipped! This is for one time parquet file generation

    @unittest.skip("Generates parquet files, do you really want this?")
    def test_convert_to_partitioned_parquet(self):
        csv_file = "tests/resources/datastore_integration_test/dataset/TEST_INNTEKT/TEST_INNTEKT__1_0.csv"
        parquet_partition_name = "tests/resources/datastore_integration_test/dataset/TEST_INNTEKT/TEST_INNTEKT"
        convert_csv_to_parquet(csv_file, parquet_partition_name, True)
        print("Done!")

    @unittest.skip("Generates parquet files, do you really want this?")
    def test_convert_to_single_parquet(self):
        csv_file = "tests/resources/datastore_integration_test/dataset/TEST_UTDANNING/TEST_UTDANNING__1_0.csv"
        parquet_dir = "tests/resources/datastore_integration_test/dataset/TEST_UTDANNING"
        convert_csv_to_parquet(csv_file, parquet_dir, False)
        print("Done!")