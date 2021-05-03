import unittest

from data_service.api.query_models import InputTimeQuery, QueryValidator, InputTimePeriodQuery, InputFixedQuery


class TestQueryModels(unittest.TestCase):

    def test_create_and_validate_minimal_input_time_period_query(self):
        input = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0",
            "startDate": 1964,
            "stopDate": 2056
        }
        QueryValidator.validate(InputTimePeriodQuery.parse_obj(input))

    def test_create_and_validate_full_input_time_period_query(self):
        input = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0",
            "startDate": 1964,
            "stopDate": 2056,
            "population": [1, 2, 3],
            "include_attributes": True
        }
        QueryValidator.validate(InputTimePeriodQuery.parse_obj(input))

    def test_create_and_validate_input_time_period_query_with_error(self):
        input = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0",
            "startDate": 1964
        }
        try:
            QueryValidator.validate(InputTimePeriodQuery.parse_obj(input))
        except Exception:
            assert True
        else:
            assert False

    def test_create_and_validate_minimal_input_time_query(self):
        input = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0",
            "date": 1964
        }
        QueryValidator.validate(InputTimeQuery.parse_obj(input))

    def test_create_and_validate_full_input_time_query(self):
        input = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0",
            "date": 1964,
            "population": [1, 2, 3],
            "include_attributes": True
        }
        QueryValidator.validate(InputTimeQuery.parse_obj(input))

    def test_create_and_validate_input_time_query_with_error(self):
        input = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.X",
            "date": 1964
        }
        try:
            QueryValidator.validate(InputTimeQuery.parse_obj(input))
        except Exception:
            assert True
        else:
            assert False

    def test_create_and_validate_minimal_input_fixed_query(self):
        input = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0"
        }
        QueryValidator.validate(InputFixedQuery.parse_obj(input))

    def test_create_and_validate_full_input_fixed_query(self):
        input = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.0",
            "population": [1, 2, 3],
            "include_attributes": True
        }
        QueryValidator.validate(InputFixedQuery.parse_obj(input))

    def test_create_and_validate_input_fixed_query_with_error(self):
        input = {
            "dataStructureName": "DATASET_NAME",
            "version": "1.0.0.X"
        }
        try:
            QueryValidator.validate(InputFixedQuery.parse_obj(input))
        except Exception:
            assert True
        else:
            assert False
