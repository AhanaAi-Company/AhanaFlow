from fastapi.testclient import TestClient

from backend.stripe_webhook.server import app


client = TestClient(app)


def test_admin_dashboard_page_is_served():
    response = client.get('/admin')

    assert response.status_code == 200
    assert 'Admin Dashboard' in response.text
    assert 'Deploy Kit' in response.text


def test_public_deploy_helper_page_is_served():
    response = client.get('/deploy-helper')

    assert response.status_code == 200
    assert 'Public Deploy Helper' in response.text
    assert 'Download Files' in response.text