import unittest
from typing import Final
from unittest.mock import Mock

from fastapi.testclient import TestClient

from application import data_service_app
from data_service.config import config
from data_service.config import dependencies
from data_service.core.processor import Processor

client = TestClient(data_service_app)

FAKE_RESULT_FILE_NAME: Final = "fake_result_file_name"


def get_settings_override():
    return config.LocalFileSettings(
        DATASTORE_ROOT='datastore_unit_test',
        DATA_SERVICE_URL='https://fake-data-service-url',
        FILE_SERVICE_DATASTORE_ROOT_PREFIX='tests/resources',
        STORAGE_ADAPTER="LOCAL"
    )


def get_processor_override():
    mock = Mock(spec=Processor)
    mock.process_event_request.return_value = FAKE_RESULT_FILE_NAME
    mock.process_status_request.return_value = FAKE_RESULT_FILE_NAME
    mock.process_fixed_request.return_value = FAKE_RESULT_FILE_NAME
    return mock


data_service_app.dependency_overrides[config.get_settings] = get_settings_override
data_service_app.dependency_overrides[dependencies.get_processor] = get_processor_override


class TestDataApi(unittest.TestCase):

    def test_data_event(self):
        response = client.post(
            "/data/event",
            json={"version": "1.0.0.0", "dataStructureName": "FAKE_NAME", "startDate": 0, "stopDate": 0}
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(FAKE_RESULT_FILE_NAME, response.json()['dataUrl'])

    def test_data_status(self):
        response = client.post(
            "/data/status",
            json={"version": "1.0.0.0", "dataStructureName": "FAKE_NAME", "date": 0}
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(FAKE_RESULT_FILE_NAME, response.json()['dataUrl'])

    def test_data_fixed(self):
        response = client.post(
            "/data/fixed",
            json={"version": "1.0.0.0", "dataStructureName": "FAKE_NAME"}
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(FAKE_RESULT_FILE_NAME, response.json()['dataUrl'])
