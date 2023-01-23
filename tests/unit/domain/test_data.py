import pytest
from pyarrow import Table

from data_service.domain import data
from data_service.exceptions import NotFoundException
from tests.resources import test_resources


def test_valid_event_request():
    file_name = data.process_event_request(
        test_resources.VALID_EVENT_QUERY_PERSON_INCOME_ALL
    )
    assert parquet_table_to_csv_string(file_name) == (
        test_resources.PERSON_INCOME_ALL
    )


def test_valid_event_request_partitioned():
    file_name = data.process_event_request(
        test_resources.VALID_EVENT_QUERY_TEST_STUDIEPOENG_ALL
    )
    assert parquet_table_to_csv_string(file_name) == (
        test_resources.TEST_STUDIEPOENG_ALL
    )


def test_event_request_causing_empty_result():
    result = data.process_event_request(
        test_resources.INVALID_EVENT_QUERY_INVALID_STOP_DATE
    )
    assert isinstance(result, Table)
    assert result.num_columns == 2
    assert result.num_rows == 0


def test_valid_status_request():
    file_name = data.process_status_request(
        test_resources.VALID_STATUS_QUERY_PERSON_INCOME_LAST_ROW
    )
    assert parquet_table_to_csv_string(file_name) == (
        test_resources.PERSON_INCOME_LAST_ROW
    )


def test_invalid_status_request():
    with pytest.raises(NotFoundException) as e:
        data.process_status_request(
            test_resources.INVALID_STATUS_QUERY_NOT_FOUND
        )
    assert str(e.value) == (
        "No NOT_A_DATASET in data_versions file for version 1_0"
    )


def test_valid_fixed_request():
    file_name = data.process_fixed_request(
        test_resources.VALID_FIXED_QUERY_PERSON_INCOME_ALL
    )
    assert parquet_table_to_csv_string(file_name) == (
        test_resources.PERSON_INCOME_ALL
    )


def test_invalid_fixed_request():
    with pytest.raises(NotFoundException) as e:
        data.process_fixed_request(
            test_resources.INVALID_FIXED_QUERY_NOT_FOUND
        )
    assert str(e.value) == (
        "No NOT_A_DATASET in data_versions file for version 1_0"
    )


def parquet_table_to_csv_string(table):
    data_frame = table.to_pandas()
    csv_string = data_frame.to_csv(
        sep=';', encoding='utf-8', lineterminator='\n'
    )
    return csv_string
