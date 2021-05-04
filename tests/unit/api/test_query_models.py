import unittest
from typing import List

from data_service.api.query_models import InputTimeQuery, InputTimePeriodQuery, InputFixedQuery


class TestQueryModels(unittest.TestCase):

    def test_create_and_validate_minimal_input_time_period_query(self):
        data = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0",
            "startDate": 1964,
            "stopDate": 2056
        }
        InputTimePeriodQuery.parse_obj(data)

    def test_create_and_validate_full_input_time_period_query(self):
        data = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0",
            "startDate": 1964,
            "stopDate": 2056,
            "population": [1, 2, 3],
            "include_attributes": True
        }
        actual = InputTimePeriodQuery.parse_obj(data)
        assert "DATASET_NAME" == actual.dataStructureName
        assert "1.0.0.0" == actual.version
        assert 1964 == actual.startDate
        assert 2056 == actual.stopDate
        assert isinstance(actual.population, List)
        assert [1, 2, 3] == actual.population
        assert True is actual.include_attributes

    def test_create_and_validate_input_time_period_query_with_error(self):
        data = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0",
            "startDate": 1964
        }
        with self.assertRaises(ValueError):
            InputTimePeriodQuery.parse_obj(data)

    def test_create_and_validate_minimal_input_time_query(self):
        data = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0",
            "date": 1964
        }
        InputTimeQuery.parse_obj(data)

    def test_create_and_validate_full_input_time_query(self):
        data = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0",
            "date": 1964,
            "population": [1, 2, 3],
            "include_attributes": True
        }
        InputTimeQuery.parse_obj(data)

    def test_create_and_validate_input_time_query_with_error(self):
        data = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.X",
            "date": 1964
        }
        with self.assertRaises(ValueError):
            InputTimeQuery.parse_obj(data)

    def test_create_and_validate_minimal_input_fixed_query(self):
        data = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0"
        }
        InputFixedQuery.parse_obj(data)

    def test_create_and_validate_full_input_fixed_query(self):
        data = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0",
            "population": [1, 2, 3],
            "include_attributes": True
        }
        InputFixedQuery.parse_obj(data)

    def test_create_and_validate_input_fixed_query_with_error(self):
        data = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.X"
        }
        with self.assertRaises(ValueError):
            InputFixedQuery.parse_obj(data)
