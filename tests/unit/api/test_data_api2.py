import pytest

from fastapi.testclient import TestClient
from application import data_service_app
from tests.unit.util.util import generate_RSA_key_pairs, encode_jwt_payload
from tests.resources import test_data
from data_service.config import config


client = TestClient(data_service_app)
JWT_PRIVATE_KEY, JWT_PUBLIC_KEY = generate_RSA_key_pairs()
JWT_INVALID_PRIVATE_KEY, _ = generate_RSA_key_pairs()

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


def test_retrieve_resultset_valid_request():
    token = encode_jwt_payload(test_data.valid_jwt_payload, JWT_PRIVATE_KEY)
    response = client.get(
            "/retrieveResultSet?file_name=1234-1234-1234-1234.parquet",
            headers={"Authorization": f"Bearer {token}"}
    )
    expected_response = open(
        'tests/resources/datastore_unit_test/resultset/1234-1234-1234-1234.parquet', 'rb'
    ).read()

    assert response.content == expected_response

def test_retrieve_resultset_not_found_dataset():
    token = encode_jwt_payload(test_data.valid_jwt_payload, JWT_PRIVATE_KEY)
    response = client.get(
            "/retrieveResultSet?file_name=does-not-exist.parquet",
            headers={"Authorization": f"Bearer {token}"}
    )
    expected_response = open(
        'tests/resources/datastore_unit_test/resultset/1234-1234-1234-1234.parquet', 'rb'
    ).read()

    assert response.status_code == 404
    assert response.json() == {"detail": "Result set not found"}

def test_retrieve_resultset_invalid_signature_request():
    token = encode_jwt_payload(test_data.valid_jwt_payload, JWT_INVALID_PRIVATE_KEY)
    response = client.get(
            "/retrieveResultSet?file_name=1234-1234-1234-1234.parquet",
            headers={"Authorization": f"Bearer {token}"}
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Unauthorized"}


