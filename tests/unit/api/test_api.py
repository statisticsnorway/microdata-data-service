import json_logging
from fastapi.testclient import TestClient

from application import data_service_app

json_logging.init_fastapi(enable_json=True)
client = TestClient(data_service_app)


def test_client_sends_x_request_id():
    response = client.get(
        "/health/alive",
        headers={"X-Request-ID": "abc123"}
    )
    assert response.status_code == 200
    assert "abc123" == response.headers["X-Request-ID"]


def test_client_does_not_send_x_request_id():
    response = client.get(
        "/health/alive"
    )
    assert response.status_code == 200
    assert response.headers["X-Request-ID"]
