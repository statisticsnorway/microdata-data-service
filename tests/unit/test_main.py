import os
import unittest

from fastapi.testclient import TestClient

from data_service import data_service_app

client = TestClient(data_service_app)

os.environ['BUCKET_NAME'] = 'fake_bucket_name'
os.environ['DATASTORE_ROOT'] = 'tests/resources/unit/datastore_root'
os.environ['DATA_SERVICE_URL'] = 'http://fake-data-service-url'


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
