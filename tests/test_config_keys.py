import errno
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


# ---- error handling: unreadable / unwritable / malformed config.yaml ----

def test_post_keys_on_a_read_only_config_answers_409_and_changes_nothing(client, config_path, monkeypatch):
    # INFO: docker-compose mounts config.yaml read-only; simulate it without chmod (root ignores it)
    real_open = open

    def read_only_open(path, mode='r', *args, **kwargs):
        if str(path) == str(config_path) and 'w' in mode:
            raise PermissionError(13, "Permission denied", str(path))
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr("builtins.open", read_only_open)
    before = config_path.read_text()

    response = client.post('/config/keys', json={"ethscan": "NEW_ETH"})

    assert response.status_code == 409
    assert response.json() == {"detail": "config.yaml is not writable (read-only mount?); "
                                         "set the explorer keys in config.yaml on the host and restart"}
    assert config_path.read_text() == before
    assert client.app.state.config["ethscan"] == "K1"   # in-memory config untouched


def test_post_keys_with_a_missing_config_answers_500_with_a_clear_message(client, config_path):
    config_path.unlink()

    response = client.post('/config/keys', json={"ethscan": "NEW_ETH"})

    assert response.status_code == 500
    assert response.json() == {"detail": "config.yaml could not be read"}
    assert client.app.state.config["ethscan"] == "K1"


def test_post_keys_with_a_malformed_config_answers_500_and_does_not_overwrite_it(client, config_path):
    config_path.write_text("ethscan: [unclosed\n")

    response = client.post('/config/keys', json={"ethscan": "NEW_ETH"})

    assert response.status_code == 500
    assert response.json() == {"detail": "config.yaml could not be read"}
    assert config_path.read_text() == "ethscan: [unclosed\n"
    assert client.app.state.config["ethscan"] == "K1"


def test_post_keys_error_never_echoes_the_key(client, config_path):
    config_path.unlink()

    response = client.post('/config/keys', json={"ethscan": "SECRET_NEW_KEY"})

    assert "SECRET_NEW_KEY" not in response.text


def test_post_keys_on_a_read_only_filesystem_answers_409_and_changes_nothing(client, config_path, monkeypatch):
    # INFO: a real docker-compose :ro bind mount raises EROFS, a plain OSError, not PermissionError
    real_open = open

    def erofs_open(path, mode='r', *args, **kwargs):
        if str(path) == str(config_path) and 'w' in mode:
            raise OSError(errno.EROFS, "Read-only file system", str(path))
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr("builtins.open", erofs_open)
    before = config_path.read_text()

    response = client.post('/config/keys', json={"ethscan": "NEW_ETH"})

    assert response.status_code == 409
    assert response.json() == {"detail": "config.yaml is not writable (read-only mount?); "
                                         "set the explorer keys in config.yaml on the host and restart"}
    assert config_path.read_text() == before
