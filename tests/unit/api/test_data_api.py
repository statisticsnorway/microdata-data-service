import os
import unittest

from fastapi.testclient import TestClient

from application import data_service_app
from data_service.config import config

client = TestClient(data_service_app)


def get_settings_override():
    return config.Settings(
        DATASTORE_ROOT='no_ssb_test',
        BUCKET_NAME='fake_bucket_name',
        DATA_SERVICE_URL='http://fake-data-service-url',
        FILE_SERVICE_DATASTORE_ROOT_PREFIX='tests/resources',
        STORAGE_ADAPTER="LOCAL"
    )


data_service_app.dependency_overrides[config.get_settings] = get_settings_override


class TestDataService(unittest.TestCase):

    def test_data_event(self):
        response = client.post(
            "/data/event",
            json={"version": "1.0.0.0", "dataStructureName": "TEST_PERSON_INCOME", "startDate": 11688,
                  "stopDate": 13149}
        )
        assert response.status_code == 200
        assert 'dataUrl' in response.json()
        assert 'http://fake-data-service-url/retrieveResultSet?file_name=' in response.json()['dataUrl']
