import yaml
import pytest
from fastapi.testclient import TestClient

import app as app_module

CONFIG = {"ethscan": "K1", "bscscan": "K2", "level": "INFO", "dbname": "default.db"}


@pytest.fixture
def config_path(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(dict(CONFIG)))
    return path


@pytest.fixture
def client(config_path):
    app_module.app.state.config = dict(CONFIG)
    app_module.app.state.config_path = str(config_path)
    with TestClient(app_module.app) as test_client:
        yield test_client


def test_get_keys_reports_which_explorer_keys_are_set(client):
    response = client.get('/config/keys')

    assert response.status_code == 200
    assert response.json() == {"ethscan": True, "bscscan": True}


def test_get_keys_reports_false_for_missing_keys(client, config_path):
    config_path.write_text(yaml.safe_dump({"level": "INFO", "dbname": "default.db"}))
    client.app.state.config = {"level": "INFO", "dbname": "default.db"}

    response = client.get('/config/keys')

    assert response.json() == {"ethscan": False, "bscscan": False}


def test_get_keys_never_exposes_the_key_value(client):
    response = client.get('/config/keys')

    assert "K1" not in response.text
    assert "K2" not in response.text


def test_post_keys_persists_both_keys_to_config_yaml(client, config_path):
    response = client.post('/config/keys', json={"ethscan": "NEW_ETH", "bscscan": "NEW_BSC"})

    assert response.status_code == 200
    assert response.json() == {"ethscan": True, "bscscan": True}
    assert yaml.safe_load(config_path.read_text()) == {
        "ethscan": "NEW_ETH", "bscscan": "NEW_BSC", "level": "INFO", "dbname": "default.db"}


def test_post_keys_updates_the_in_memory_config_for_new_requests(client):
    client.post('/config/keys', json={"ethscan": "NEW_ETH", "bscscan": "NEW_BSC"})

    assert client.app.state.config["ethscan"] == "NEW_ETH"
    assert client.app.state.config["bscscan"] == "NEW_BSC"


def test_post_keys_updates_only_the_given_key(client, config_path):
    response = client.post('/config/keys', json={"ethscan": "NEW_ETH"})

    assert response.status_code == 200
    assert response.json() == {"ethscan": True, "bscscan": True}
    assert yaml.safe_load(config_path.read_text()) == {
        "ethscan": "NEW_ETH", "bscscan": "K2", "level": "INFO", "dbname": "default.db"}


def test_post_keys_rejects_empty_body(client):
    response = client.post('/config/keys', json={})

    assert response.status_code == 400


def test_post_keys_rejects_blank_key_value(client):
    response = client.post('/config/keys', json={"ethscan": "   "})

    assert response.status_code == 400


def test_post_keys_strips_whitespace(client, config_path):
    client.post('/config/keys', json={"ethscan": "  NEW_ETH  "})

    assert yaml.safe_load(config_path.read_text())["ethscan"] == "NEW_ETH"
