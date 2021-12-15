import pytest
import os
import pyarrow.parquet as pq
from tests.resources import test_data
from data_service.config import config
from data_service.core.processor import (
    Processor, NotFoundException, EmptyResultSetException
)


RESULTSET_DIR = 'tests/resources/resultset'

processor = Processor(
    settings=config.LocalFileSettings(
        DATA_SERVICE_URL='https://fake-data-service-url',
        DATASTORE_DIR='tests/resources/datastore_unit_test',
        RESULTSET_DIR=RESULTSET_DIR
    )
)


def test_valid_event_request():
    file_name = processor.process_event_request(
        test_data.VALID_EVENT_QUERY_PERSON_INCOME_ALL
    )
    assert result_set_to_csv_string(file_name) == test_data.PERSON_INCOME_ALL


def test_valid_event_request_partitioned():
    file_name = processor.process_event_request(
        test_data.VALID_EVENT_QUERY_TEST_STUDIEPOENG_ALL
    )
    assert result_set_to_csv_string(file_name) == test_data.TEST_STUDIEPOENG_ALL


def test_invalid_event_request():
    with pytest.raises(EmptyResultSetException) as e:
        processor.process_event_request(
            test_data.INVALID_EVENT_QUERY_INVALID_STOP_DATE
        )
    assert str(e.value) == "Empty result set"


def test_valid_status_request():
    file_name = processor.process_status_request(
        test_data.VALID_STATUS_QUERY_PERSON_INCOME_LAST_ROW
    )
    assert result_set_to_csv_string(file_name) == (
        test_data.PERSON_INCOME_LAST_ROW
    )


def test_invalid_status_request():
    with pytest.raises(NotFoundException) as e:
        processor.process_status_request(
            test_data.INVALID_STATUS_QUERY_NOT_FOUND
        )
    assert str(e.value) == "No such data structure"


def test_valid_fixed_request():
    file_name = processor.process_fixed_request(
        test_data.VALID_FIXED_QUERY_PERSON_INCOME_ALL
    )
    assert result_set_to_csv_string(file_name) == test_data.PERSON_INCOME_ALL


def test_invalid_fixed_request():
    with pytest.raises(NotFoundException) as e:
        processor.process_fixed_request(
            test_data.INVALID_FIXED_QUERY_NOT_FOUND
        )
    assert str(e.value) == "No such data structure"


def teardown_function(file_name):
    result_sets = os.listdir(RESULTSET_DIR)
    test_generated_result_sets = [
        result_set for result_set in result_sets
        if result_set != "1234-1234-1234-1234.parquet"
    ]
    for file_name in test_generated_result_sets:
        os.remove(f'{RESULTSET_DIR}/{file_name}')


def result_set_to_csv_string(file_name):
    data_frame = pq.read_table(f'{RESULTSET_DIR}/{file_name}').to_pandas()
    csv_string = data_frame.to_csv(sep=';', encoding='utf-8')
    return csv_string
