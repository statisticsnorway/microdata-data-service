import pytest
from pytest import MonkeyPatch
from fastapi import HTTPException

from data_service.api import auth
from data_service.api.auth import authorize_user, get_jwks_aud
from tests.resources import test_resources
from tests.unit.util.util import generate_RSA_key_pairs, encode_jwt_payload

JWT_PRIVATE_KEY, JWT_PUBLIC_KEY = generate_RSA_key_pairs()
JWT_INVALID_PRIVATE_KEY, _ = generate_RSA_key_pairs()


@pytest.fixture(autouse=True)
def setup(monkeypatch: MonkeyPatch):
    monkeypatch.setattr(
        auth,
        'get_signing_key',
        lambda *a: JWT_PUBLIC_KEY.decode('utf-8')
    )


def test_auth_valid_token():
    token = encode_jwt_payload(
        test_resources.valid_jwt_payload, JWT_PRIVATE_KEY
    )
    user_id = authorize_user(f'Bearer {token}')
    assert user_id == test_resources.valid_jwt_payload['sub']


def test_auth_wrong_audience():
    with pytest.raises(HTTPException) as e:
        token = encode_jwt_payload(
            test_resources.jwt_payload_missing_user_id, JWT_PRIVATE_KEY
        )
        authorize_user(f'Bearer {token}')
    assert e.value.status_code == 401
    assert "Unauthorized" in e.value.detail


def test_auth_expired_token():
    with pytest.raises(HTTPException) as e:
        token = encode_jwt_payload(
            test_resources.jwt_payload_expired, JWT_PRIVATE_KEY
        )
        authorize_user(f'Bearer {token}')
    assert e.value.status_code == 401
    assert "Unauthorized" in e.value.detail


def test_auth_missing_user_id():
    with pytest.raises(HTTPException) as e:
        token = encode_jwt_payload(
            test_resources.jwt_payload_missing_user_id, JWT_PRIVATE_KEY
        )
        authorize_user(f'Bearer {token}')
    assert e.value.status_code == 401
    assert "Unauthorized" in e.value.detail


def test_auth_missing_token():
    with pytest.raises(HTTPException) as e:
        authorize_user(None)
    assert e.value.status_code == 401
    assert "Unauthorized" in e.value.detail


def test_auth_toggled_off(monkeypatch: MonkeyPatch):
    monkeypatch.setenv('JWT_AUTH', 'false')
    user_id = authorize_user(None)
    assert user_id == "default"


def test_jwt_audience_qa(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("STACK", "qa")
    assert get_jwks_aud() == "datastore-qa"


def test_jwt_audience_prod(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("STACK", "prod")
    assert get_jwks_aud() == "datastore"
