from datetime import datetime, timedelta

valid_jwt_payload = {
    "aud": "datastore",
    "exp": (datetime.now() + timedelta(hours=1)).timestamp(),
    "sub": "testuser"
}

jwt_payload_missing_user_id = {
    "aud": "datastore",
    "exp": (datetime.now() + timedelta(hours=1)).timestamp()
}

jwt_payload_wrong_audience = {
    "aud": "wrong",
    "exp": (datetime.now() + timedelta(hours=1)).timestamp(),
    "sub": "testuser"
}

jwt_payload_expired = {
    "aud": "datastore",
    "exp": (datetime.now() - timedelta(hours=1)).timestamp(),
    "sub": "testuser"
}
