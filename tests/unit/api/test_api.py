from fastapi.testclient import TestClient

from data_service.app import data_service_app

client = TestClient(data_service_app)


def test_client_sends_x_request_id():
    response = client.get("/health/alive", headers={"X-Request-ID": "abc123"})
    assert response.status_code == 200
    assert "abc123" == response.headers["X-Request-ID"]


def test_client_does_not_send_x_request_id():
    response = client.get("/health/alive")
    assert response.status_code == 200
    assert response.headers["X-Request-ID"]
