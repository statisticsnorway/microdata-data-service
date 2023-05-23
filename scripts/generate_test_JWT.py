# poetry run python scripts/generate_test_JWT.py

from tests.unit.util.util import generate_RSA_key_pairs, encode_jwt_payload
from datetime import datetime, timedelta


def generate():
    JWT_PRIVATE_KEY, JWT_PUBLIC_KEY = generate_RSA_key_pairs()
    VALID_JWT_PAYLOAD = {
        "aud": "datastore",
        "exp": (datetime.now() + timedelta(hours=24)).timestamp(),
        "sub": "testuser",
    }
    EXPIRED_JWT_PAYLOAD = {
        "aud": "datastore",
        "exp": (datetime.now() - timedelta(hours=24)).timestamp(),
        "sub": "testuser",
    }

    VALID_TOKEN = encode_jwt_payload(VALID_JWT_PAYLOAD, JWT_PRIVATE_KEY)
    EXPIRED_TOKEN = encode_jwt_payload(EXPIRED_JWT_PAYLOAD, JWT_PRIVATE_KEY)

    print()
    print("PUBLIC KEY for use in environment")
    print('export JWT_PUBLIC_KEY="<copy key here>"')
    print()
    print(JWT_PUBLIC_KEY.decode("utf-8"))
    print()
    print()
    print("Valid token for use in authorization header:")
    print(VALID_TOKEN)
    print()
    print()
    print("Expired token for use in authorization header:")
    print(EXPIRED_TOKEN)


if __name__ == "__main__":
    generate()
