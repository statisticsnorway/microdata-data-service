import unittest

from tests.unit.util.util import convert_csv_to_parquet, convert_csv_to_single_parquet_file


class TestSomething(unittest.TestCase):

    @unittest.skip("Generates parquet files, do you really want this?")
    def test_convert_inntekt(self):
        csv_file = "tests/resources/datastore_integration_test/dataset/TEST_INNTEKT/TEST_INNTEKT__1_0.csv"
        parquet_partition_name = "tests/resources/datastore_integration_test/dataset/TEST_INNTEKT/TEST_INNTEKT"
        convert_csv_to_parquet(csv_file, parquet_partition_name)
        print("Done!")

    @unittest.skip("Generates parquet files, do you really want this?")
    def test_convert_utdanning(self):
        csv_file = "tests/resources/datastore_integration_test/dataset/TEST_UTDANNING/TEST_UTDANNING__1_0.csv"
        parquet_dir = "tests/resources/datastore_integration_test/dataset/TEST_UTDANNING"
        convert_csv_to_single_parquet_file(csv_file, parquet_dir)
        print("Done!")