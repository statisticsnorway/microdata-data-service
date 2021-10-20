import pytest

from data_service.api.auth import authorize_user
from tests.unit.util.util import generate_RSA_key_pairs, encode_jwt_payload
from fastapi import HTTPException
from tests.resources import test_data

JWT_PRIVATE_KEY, JWT_PUBLIC_KEY = generate_RSA_key_pairs()
JWT_INVALID_PRIVATE_KEY, _ = generate_RSA_key_pairs()


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    monkeypatch.setenv(
        'JWT_PUBLIC_KEY', JWT_PUBLIC_KEY.decode('utf-8')
    )


def test_auth_valid_token():
    token = encode_jwt_payload(
        test_data.valid_jwt_payload, JWT_PRIVATE_KEY
    )
    user_id = authorize_user(f'Bearer {token}')
    assert user_id == test_data.valid_jwt_payload['user_id']


def test_auth_wrong_audience():
    with pytest.raises(HTTPException) as e:
        token = encode_jwt_payload(
            test_data.jwt_payload_missing_user_id, JWT_PRIVATE_KEY
        )
        authorize_user(f'Bearer {token}')
    assert e.value.status_code == 401
    assert e.value.detail == "Unauthorized"


def test_auth_expired_token():
    with pytest.raises(HTTPException) as e:
        token = encode_jwt_payload(
            test_data.jwt_payload_expired, JWT_PRIVATE_KEY
        )
        authorize_user(f'Bearer {token}')
    assert e.value.status_code == 401
    assert e.value.detail == "Unauthorized"


def test_auth_missing_user_id():
    with pytest.raises(HTTPException) as e:
        token = encode_jwt_payload(
            test_data.jwt_payload_missing_user_id, JWT_PRIVATE_KEY
        )
        authorize_user(f'Bearer {token}')
    assert e.value.status_code == 401
    assert e.value.detail == "Unauthorized"


def test_auth_missing_token():
    with pytest.raises(HTTPException) as e:
        authorize_user(None)
    assert e.value.status_code == 401
    assert e.value.detail == "Unauthorized"


def test_auth_missing_config_public_key(monkeypatch):
    monkeypatch.delenv('JWT_PUBLIC_KEY')
    with pytest.raises(HTTPException) as e:
        token = encode_jwt_payload(
            test_data.valid_jwt_payload, JWT_PRIVATE_KEY
        )
        authorize_user(f'Bearer {token}')
    assert e.value.status_code == 500
    assert e.value.detail == "Internal Server Error"


def test_auth_toggled_off(monkeypatch):
    monkeypatch.setenv('TOGGLE_AUTH', 'OFF')
    user_id = authorize_user(None)
    assert user_id == "default"
