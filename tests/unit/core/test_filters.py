import shutil
import pytest

import numpy as np
from pandas.testing import assert_frame_equal
from pyarrow import Table

from data_service.core.filters import filter_by_fixed
from data_service.core.filters import filter_by_time
from data_service.core.filters import filter_by_time_period
from tests.unit.util.util import convert_csv_to_parquet

DATASTORE_DIR = 'tests/resources/datastore_unit_test'
NO_PARTITION_DIR = 'tests/resources/unit_test_data/NO_PARTITION'
TEST_BOSTED_CSV_FILE = (
    f'{DATASTORE_DIR}/dataset/TEST_BOSTED/TEST_BOSTED__3_2.csv'
)
TEST_BOSTED_PARQUET_DIR = (
    f'{DATASTORE_DIR}/dataset/TEST_BOSTED/TEST_BOSTED__3_2'
)
TEST_PERSON_INCOME_PARQUET_FILE = (
    f'{DATASTORE_DIR}/dataset/TEST_PERSON_INCOME/'
    'TEST_PERSON_INCOME__1_0.parquet'
)


def setup_function():
    convert_csv_to_parquet(
        TEST_BOSTED_CSV_FILE, TEST_BOSTED_PARQUET_DIR, True
    )


def test_by_time_period_from_7670_to_8034():
    expected_data = {
        'unit_id': [
            1000000002, 1000000004, 1000000003, 1000000001,
            1000000001, 1000000003, 1000000003
        ],
        'value': ["8", "2", "12", "3", "16", "2", "12"],
        'start_epoch_days': [1461, 3287, 4018, 5479, 7851, 7701, 7957],
        'stop_epoch_days': [8065, 7710, 7700, 7850, 8125, 7956, np.nan]
    }
    expected = Table.from_pydict(expected_data)
    print_expected(expected)

    actual = filter_by_time_period(
        TEST_BOSTED_PARQUET_DIR, 7670, 8034, None, True
    )
    print_actual(actual)

    assert_frame_equal(
        expected.to_pandas(), actual.to_pandas(), check_dtype=False
    )


def test_by_time_period_from_7670_to_8034_excluding_attributes():
    expected = Table.from_pydict({
        'unit_id': [
            1000000002, 1000000004, 1000000003, 1000000001,
            1000000001, 1000000003, 1000000003
        ],
        'value': ["8", "2", "12", "3", "16", "2", "12"]
    })
    print_expected(expected)

    actual = filter_by_time_period(TEST_BOSTED_PARQUET_DIR, 7670, 8034)
    print_actual(actual)

    assert_frame_equal(
        expected.to_pandas(), actual.to_pandas(), check_dtype=False
    )


def test_by_time_period_from_7670_to_8400():
    expected = Table.from_pydict({
        'unit_id': [
            1000000002, 1000000004, 1000000003, 1000000001,
            1000000001, 1000000003, 1000000003, 1000000001,
            1000000002
        ],
        'value': ["8", "2", "12", "3", "16", "2", "12", "3", "8"],
        'start_epoch_days': [
            1461, 3287, 4018, 5479, 7851, 7701, 7957, 8126, 8066
        ],
        'stop_epoch_days': [
            8065, 7710, 7700, 7850, 8125, 7956, np.nan, np.nan, np.nan
        ]
    })
    print_expected(expected)

    actual = filter_by_time_period(
        TEST_BOSTED_PARQUET_DIR, 7670, 8400, None, True
    )
    print_actual(actual)

    assert_frame_equal(
        expected.to_pandas(), actual.to_pandas(), check_dtype=False
    )


def test_by_time_period_from_7670_to_8400_including_attributes_and_population_filter():
    population_filter = [1000000002, 1000000003]

    expected = Table.from_pydict({
        'unit_id': [
            1000000002, 1000000003, 1000000003, 1000000003, 1000000002
        ],
        'value': ["8", "12", "2", "12", "8"],
        'start_epoch_days': [1461, 4018, 7701, 7957, 8066],
        'stop_epoch_days': [8065, 7700, 7956, np.nan, np.nan]
    })
    print_expected(expected)

    actual = filter_by_time_period(
        TEST_BOSTED_PARQUET_DIR, 7670, 8400, population_filter, True
    )
    print_actual(actual)

    assert_frame_equal(
        expected.to_pandas(), actual.to_pandas(), check_dtype=False
    )


def test_by_time():
    expected = Table.from_pydict({
        'unit_id': [1000000002, 1000000004, 1000000003, 1000000001],
        'value': ["8", "2", "12", "3"],
        'start_epoch_days': [1461, 3287, 4018, 5479],
        'stop_epoch_days': [8065, 7710, 7700, 7850]
    })
    print_expected(expected)

    actual = filter_by_time(TEST_BOSTED_PARQUET_DIR, 7669, None, True)
    print_actual(actual)

    assert_frame_equal(
        expected.to_pandas(), actual.to_pandas(), check_dtype=False
    )


def test_by_time_excluding_attributes():
    expected = Table.from_pydict({
        'unit_id': [1000000002, 1000000004, 1000000003, 1000000001],
        'value': ["8", "2", "12", "3"]
    })
    print_expected(expected)

    actual = filter_by_time(TEST_BOSTED_PARQUET_DIR, 7669)
    print_actual(actual)

    assert_frame_equal(
        expected.to_pandas(), actual.to_pandas(), check_dtype=False
    )


def test_by_time_including_attributes_and_population_filter():
    population_filter = [1000000002, 1000000003]

    expected = Table.from_pydict({
        'unit_id': [1000000002, 1000000003],
        'value': ["8", "12"],
        'start_epoch_days': [1461, 7957],
        'stop_epoch_days': [8065, np.nan]
    })
    print_expected(expected)

    actual = filter_by_time(
        TEST_BOSTED_PARQUET_DIR, 8034, population_filter, True
    )
    print_actual(actual)

    assert_frame_equal(
        expected.to_pandas(), actual.to_pandas(), check_dtype=False
    )


def test_by_fixed():
    expected = Table.from_pydict({
        'unit_id': [
            1000000002, 1000000004, 1000000003, 1000000001,
            1000000001, 1000000003, 1000000003, 1000000001,
            1000000002
        ],
        'value': ["8", "2", "12", "3", "16", "2", "12", "3", "8"],
        'start_epoch_days': [
            1461, 3287, 4018, 5479, 7851, 7701, 7957, 8126, 8066
        ],
        'stop_epoch_days': [
            8065, 7710, 7700, 7850, 8125, 7956, np.nan, np.nan, np.nan
        ]
    })
    print_expected(expected)

    actual = filter_by_fixed(TEST_BOSTED_PARQUET_DIR, None, True)
    print_actual(actual)

    assert_frame_equal(
        expected.to_pandas(), actual.to_pandas(), check_dtype=False
    )


def test_by_fixed_excluding_attributes():
    expected = Table.from_pydict({
        'unit_id': [
            1000000002, 1000000004, 1000000003, 1000000001,
            1000000001, 1000000003, 1000000003, 1000000001,
            1000000002
        ],
        'value': ["8", "2", "12", "3", "16", "2", "12", "3", "8"]
    })
    print_expected(expected)

    actual = filter_by_fixed(TEST_BOSTED_PARQUET_DIR)
    print_actual(actual)

    assert_frame_equal(
        expected.to_pandas(), actual.to_pandas(), check_dtype=False
    )


def test_by_fixed_including_attributes_and_population_filter():
    population_filter = [1000000002, 1000000003]

    expected = Table.from_pydict({
        'unit_id': [
            1000000002, 1000000003, 1000000003, 1000000003, 1000000002
        ],
        'value': ["8", "12", "2", "12", "8"],
        'start_epoch_days': [1461, 4018, 7701, 7957, 8066],
        'stop_epoch_days': [8065, 7700, 7956, np.nan, np.nan]
    })
    print_expected(expected)

    actual = filter_by_fixed(TEST_BOSTED_PARQUET_DIR, population_filter, True)
    print_actual(actual)

    assert_frame_equal(
        expected.to_pandas(), actual.to_pandas(), check_dtype=False
    )


def test_by_fixed_including_population_filter_on_single_parquet():
    parquet_file = TEST_PERSON_INCOME_PARQUET_FILE
    population_filter = [11111113785911, 11111111190644]

    expected = Table.from_pydict({
        'unit_id': [11111113785911, 11111111190644],
        'value': ["16354872", "11331198"],
        'start_epoch_days': [13879, 6209],
        'stop_epoch_days': [14244, 6573]
    })
    print_expected(expected)

    actual = filter_by_fixed(parquet_file, population_filter, True)
    if actual:
        print_actual(actual)

    assert_frame_equal(
        expected.to_pandas(), actual.to_pandas(), check_dtype=False
    )


def test_by_time_period_non_existing_partition():
    with pytest.raises(FileNotFoundError):
        filter_by_time_period(NO_PARTITION_DIR, 7670, 8034, None, True)


def test_by_time_non_existing_partition():
    with pytest.raises(FileNotFoundError):
        filter_by_time(NO_PARTITION_DIR, 7669, None, True)


def teardown_function():
    shutil.rmtree(TEST_BOSTED_PARQUET_DIR)


def print_expected(expected: Table):
    print('==================== EXPECTED ========================')
    print(expected.to_pandas())


def print_actual(actual: Table):
    print('==================== ACTUAL ==========================')
    print(actual.to_pandas())
