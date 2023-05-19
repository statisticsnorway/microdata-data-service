from tests.unit.util.util import convert_csv_to_parquet


def test_convert_to_partitioned_parquet():
    csv_file = "tests_manual/TEST_INNTEKT__1_0.csv"
    parquet_partition_name = "tests_manual/TEST_INNTEKT"
    convert_csv_to_parquet(csv_file, parquet_partition_name, True)


def test_convert_to_single_parquet():
    csv_file = "tests_manual/TEST_UTDANNING__1_0.csv"
    parquet_dir = "tests_manual/TEST_UTDANNING__1_0"
    convert_csv_to_parquet(csv_file, parquet_dir, False)
