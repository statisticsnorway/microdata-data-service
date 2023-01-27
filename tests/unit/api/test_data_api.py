import pytest
from pytest import MonkeyPatch
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from data_service.app import data_service_app
from data_service.api import auth
from data_service.domain import data
from tests.resources import test_resources
from tests.unit.util.util import generate_RSA_key_pairs, encode_jwt_payload

JWT_PRIVATE_KEY, JWT_PUBLIC_KEY = generate_RSA_key_pairs()
JWT_INVALID_PRIVATE_KEY, _ = generate_RSA_key_pairs()
VALID_JWT_TOKEN = encode_jwt_payload(
    test_resources.valid_jwt_payload, JWT_PRIVATE_KEY
)
INVALID_JWT_TOKEN = encode_jwt_payload(
    test_resources.valid_jwt_payload, JWT_INVALID_PRIVATE_KEY
)
FAKE_RESULT_FILE_NAME = "fake_result_file_name"
MOCK_RESULT = pq.read_table(
    'tests/resources/results/mocked_result.parquet'
)

client = TestClient(data_service_app)


@pytest.fixture(autouse=True)
def setup(monkeypatch: MonkeyPatch):
    monkeypatch.setattr(
        auth,
        'get_signing_key',
        lambda _: JWT_PUBLIC_KEY.decode('utf-8')
    )
    for temporality in ['status', 'event', 'fixed']:
        monkeypatch.setattr(
            data,
            f'process_{temporality}_request',
            lambda _: MOCK_RESULT
        )


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
    assert pq.read_table(reader) == MOCK_RESULT


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
    assert pq.read_table(reader) == MOCK_RESULT


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
    assert pq.read_table(reader) == MOCK_RESULT
