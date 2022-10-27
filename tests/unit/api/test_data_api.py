from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient
from pytest import MonkeyPatch

from application import data_service_app
from data_service.api import auth
from data_service.config import config, dependencies
from data_service.core.processor import Processor
from tests.resources import test_data
from tests.unit.util.util import generate_RSA_key_pairs, encode_jwt_payload

JWT_PRIVATE_KEY, JWT_PUBLIC_KEY = generate_RSA_key_pairs()
JWT_INVALID_PRIVATE_KEY, _ = generate_RSA_key_pairs()
VALID_JWT_TOKEN = encode_jwt_payload(
    test_data.valid_jwt_payload, JWT_PRIVATE_KEY
)
INVALID_JWT_TOKEN = encode_jwt_payload(
    test_data.valid_jwt_payload, JWT_INVALID_PRIVATE_KEY
)
FAKE_RESULT_FILE_NAME = "fake_result_file_name"
MOCK_RESULTSET = pq.read_table(
    'tests/resources/resultset/1234-1234-1234-1234.parquet'
)

client = TestClient(data_service_app)


def get_processor_override():
    mock = Mock(spec=Processor)
    mock.process_event_request.return_value = MOCK_RESULTSET
    mock.process_status_request.return_value = MOCK_RESULTSET
    mock.process_fixed_request.return_value = MOCK_RESULTSET
    mock.write_table.return_value = FAKE_RESULT_FILE_NAME
    return mock


data_service_app.dependency_overrides[dependencies.get_processor] = (
    get_processor_override
)
data_service_app.dependency_overrides[config.get_settings] = (
    lambda: config.LocalFileSettings(
        DATA_SERVICE_URL='https://fake-data-service-url',
        DATASTORE_DIR='tests/resources/datastore_unit_test',
        RESULTSET_DIR='tests/resources/resultset'
    )
)


@pytest.fixture(autouse=True)
def setup(monkeypatch: MonkeyPatch):
    monkeypatch.setattr(
        auth,
        'get_signing_key',
        lambda _: JWT_PUBLIC_KEY.decode('utf-8')
    )


# /data/event
def test_data_event_generate_file():
    response = client.post(
        "/data/event/generate-file",
        json={
            "version": "1.0.0.0",
            "dataStructureName": "FAKE_NAME",
            "startDate": 0,
            "stopDate": 0
        },
        headers={"Authorization": f"Bearer {VALID_JWT_TOKEN}"}
    )
    assert response.status_code == 200
    assert FAKE_RESULT_FILE_NAME in response.json()['filename']


def test_data_event_stream_result():
    response = client.post(
        "/data/event/stream",
        json={
            "version": "1.0.0.0",
            "dataStructureName": "FAKE_NAME",
            "startDate": 0,
            "stopDate": 0
        },
        headers={"Authorization": f"Bearer {VALID_JWT_TOKEN}"}
    )

    reader = pa.BufferReader(response.content)
    assert response.status_code == 200
    assert pq.read_table(reader) == MOCK_RESULTSET


# /data/status
def test_data_status_generate_file():
    response = client.post(
        "/data/status/generate-file",
        json={
            "version": "1.0.0.0",
            "dataStructureName": "FAKE_NAME",
            "date": 0
        },
        headers={"Authorization": f"Bearer {VALID_JWT_TOKEN}"}
    )
    assert response.status_code == 200
    assert FAKE_RESULT_FILE_NAME in response.json()['filename']


def test_data_status_stream_result():
    response = client.post(
        "/data/status/stream",
        json={
            "version": "1.0.0.0",
            "dataStructureName": "FAKE_NAME",
            "date": 0
        },
        headers={"Authorization": f"Bearer {VALID_JWT_TOKEN}"}
    )

    reader = pa.BufferReader(response.content)
    assert response.status_code == 200
    assert pq.read_table(reader) == MOCK_RESULTSET


# /data/fixed
def test_data_fixed_generate_file():
    response = client.post(
        "/data/fixed/generate-file",
        json={
            "version": "1.0.0.0",
            "dataStructureName": "FAKE_NAME"
        },
        headers={"Authorization": f"Bearer {VALID_JWT_TOKEN}"}
    )
    assert response.status_code == 200
    assert FAKE_RESULT_FILE_NAME in response.json()['filename']


def test_data_fixed_stream_result():
    response = client.post(
        "/data/fixed/stream",
        json={
            "version": "1.0.0.0",
            "dataStructureName": "FAKE_NAME"
        },
        headers={"Authorization": f"Bearer {VALID_JWT_TOKEN}"}
    )

    reader = pa.BufferReader(response.content)
    assert response.status_code == 200
    assert pq.read_table(reader) == MOCK_RESULTSET
