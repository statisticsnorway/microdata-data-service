from typing import List

import pytest

from data_service.api.query_models import (
    InputTimeQuery, InputTimePeriodQuery, InputFixedQuery, InputQuery
)


def test_create_and_validate_minimal_input_time_period_query():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "startDate": 1964,
        "stopDate": 2056
    }
    InputTimePeriodQuery.parse_obj(data)


def test_create_and_validate_full_input_time_period_query():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "startDate": 1964,
        "stopDate": 2056,
        "population": [1, 2, 3],
        "includeAttributes": True
    }
    actual = InputTimePeriodQuery.parse_obj(data)
    assert actual.dataStructureName == "DATASET_NAME"
    assert actual.version == "1.0.0.0"
    assert actual.startDate == 1964
    assert actual.stopDate == 2056
    assert isinstance(actual.population, List)
    assert actual.population == [1, 2, 3]
    assert actual.includeAttributes is True


def test_create_and_validate_input_time_period_query_with_error():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "startDate": 1964
    }
    with pytest.raises(ValueError):
        InputTimePeriodQuery.parse_obj(data)


def test_create_and_validate_minimal_input_time_query():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "date": 1964
    }
    InputTimeQuery.parse_obj(data)


def test_create_and_validate_full_input_time_query():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "date": 1964,
        "population": [1, 2, 3],
        "includeAttributes": True
    }
    InputTimeQuery.parse_obj(data)


def test_create_and_validate_input_time_query_with_error():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.X",
        "date": 1964
    }
    with pytest.raises(ValueError):
        InputTimeQuery.parse_obj(data)


def test_create_and_validate_minimal_input_fixed_query():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0"
    }
    InputFixedQuery.parse_obj(data)


def test_create_and_validate_full_input_fixed_query():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "population": [1, 2, 3],
        "includeAttributes": True
    }
    InputFixedQuery.parse_obj(data)


def test_create_and_validate_input_fixed_query_with_error():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.X"
    }
    with pytest.raises(ValueError):
        InputFixedQuery.parse_obj(data)


def test_population_to_string():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "population": [1, 2, 3]
    }
    actual: InputQuery = InputQuery.parse_obj(data)
    assert str(actual) == \
        "dataStructureName='DATASET_NAME' " \
        "version='1.0.0.0' " \
        "population='<length: 3>' " \
        "includeAttributes=False"
    assert actual.population == data["population"]


def test_population_to_string_input_time_query():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "population": [1, 2, 3],
        "date": 1900
    }
    actual: InputTimeQuery = InputTimeQuery.parse_obj(data)
    assert str(actual) == \
           "dataStructureName='DATASET_NAME' " \
           "version='1.0.0.0' " \
           "population='<length: 3>' " \
           "includeAttributes=False " \
           "date=1900"
    assert actual.population == data["population"]
