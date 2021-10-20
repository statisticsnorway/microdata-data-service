import pytest
from unittest.mock import Mock
from fastapi.testclient import TestClient

from application import data_service_app
from data_service.config import config, dependencies
from data_service.core.processor import Processor

from tests.unit.util.util import generate_RSA_key_pairs, encode_jwt_payload
from tests.resources import test_data



JWT_PRIVATE_KEY, JWT_PUBLIC_KEY = generate_RSA_key_pairs()
JWT_INVALID_PRIVATE_KEY, _ = generate_RSA_key_pairs()
VALID_JWT_TOKEN = encode_jwt_payload(test_data.valid_jwt_payload, JWT_PRIVATE_KEY)
INVALID_JWT_TOKEN = encode_jwt_payload(test_data.valid_jwt_payload, JWT_INVALID_PRIVATE_KEY)
FAKE_RESULT_FILE_NAME = "fake_result_file_name"

client = TestClient(data_service_app)

def get_processor_override():
    mock = Mock(spec=Processor)
    mock.process_event_request.return_value = FAKE_RESULT_FILE_NAME
    mock.process_status_request.return_value = FAKE_RESULT_FILE_NAME
    mock.process_fixed_request.return_value = FAKE_RESULT_FILE_NAME
    return mock

data_service_app.dependency_overrides[dependencies.get_processor] = get_processor_override
data_service_app.dependency_overrides[config.get_settings] = (
    lambda: config.LocalFileSettings(
        DATASTORE_ROOT='datastore_unit_test',
        DATA_SERVICE_URL='https://fake-data-service-url',
        FILE_SERVICE_DATASTORE_ROOT_PREFIX='tests/resources/datastore_unit_test'
    )
)


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    monkeypatch.setenv(
        'JWT_PUBLIC_KEY', JWT_PUBLIC_KEY.decode('utf-8')
    )


# /data/resultset
def test_get_result_set_valid_request():
    response = client.get(
            "/data/resultSet?file_name=1234-1234-1234-1234.parquet",
            headers={"Authorization": f"Bearer {VALID_JWT_TOKEN}"}
    )
    expected_response = open(
        'tests/resources/datastore_unit_test/resultset/1234-1234-1234-1234.parquet', 'rb'
    ).read()

    assert response.content == expected_response


def test_get_result_set_not_found_dataset():
    response = client.get(
            "/data/resultSet?file_name=does-not-exist.parquet",
            headers={"Authorization": f"Bearer {VALID_JWT_TOKEN}"}
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Result set not found"}


def test_get_result_set_invalid_signature_request():
    response = client.get(
            "/data/resultSet?file_name=1234-1234-1234-1234.parquet",
            headers={"Authorization": f"Bearer {INVALID_JWT_TOKEN}"}
    )

    assert response.status_code == 401
    assert "Unauthorized" in response.json()["detail"]


# /data/event
def test_data_event():
    response = client.post(
        "/data/event",
        json={"version": "1.0.0.0", "dataStructureName": "FAKE_NAME", "startDate": 0, "stopDate": 0},
        headers={"Authorization": f"Bearer {VALID_JWT_TOKEN}"}
    )
    assert response.status_code == 200
    assert FAKE_RESULT_FILE_NAME in response.json()['dataUrl']

# /data/status
def test_data_status():
    response = client.post(
        "/data/status",
        json={"version": "1.0.0.0", "dataStructureName": "FAKE_NAME", "date": 0},
        headers={"Authorization": f"Bearer {VALID_JWT_TOKEN}"}
    )
    assert response.status_code == 200
    assert FAKE_RESULT_FILE_NAME in response.json()['dataUrl']

# /data/fixed
def test_data_fixed():
    response = client.post(
        "/data/fixed",
        json={"version": "1.0.0.0", "dataStructureName": "FAKE_NAME"},
        headers={"Authorization": f"Bearer {VALID_JWT_TOKEN}"}
    )
    assert response.status_code == 200
    assert FAKE_RESULT_FILE_NAME in response.json()['dataUrl']
