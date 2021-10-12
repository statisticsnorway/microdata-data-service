import pytest

from fastapi.testclient import TestClient
from application import data_service_app
from tests.unit.util.util import generate_RSA_key_pairs, encode_jwt_payload
from tests.resources import test_data

client = TestClient(data_service_app)
JWT_PRIVATE_KEY, JWT_PUBLIC_KEY = generate_RSA_key_pairs()

@pytest.fixture(autouse=True)
def setup(monkeypatch):
    monkeypatch.setenv(
        'JWT_PUBLIC_KEY', JWT_PUBLIC_KEY.decode('utf-8')
    )

def test_meme(self):
    token = encode_jwt_payload(test_data.valid_jwt_payload, JWT_PRIVATE_KEY)
    response = client.get(
            "/retrieveResultSet?file_name=testname",
            headers={"Authorization": f"Bearer {token}"}
    )
    print(response.json())
    assert 1 == 2


