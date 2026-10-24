import json
import logging
import traceback
from pathlib import Path

import pytest
import requests
import yaml
from fastapi.testclient import TestClient

import app as app_module
from core import api_client, bsc, eth, misc

SECRET_CONFIG = {"ethscan": "ETH-SECRET-KEY", "bscscan": "BSC-SECRET-KEY", "level": "INFO", "dbname": "default.db"}
CORE_MODULES = ["core/eth.py", "core/bsc.py", "core/misc.py"]


@pytest.fixture
def client():
    app_module.app.state.config = dict(SECRET_CONFIG)
    with TestClient(app_module.app) as test_client:
        yield test_client


def fake_stream(params):
    yield f"data:{json.dumps({'msg': 'ok', 'end': True})}\n\n"


def test_api_get_uses_timeout(monkeypatch):
    calls = []
    monkeypatch.setattr(api_client._session, "get", lambda url, **kwargs: calls.append((url, kwargs)) or "response")

    response = api_client.api_get("https://api.etherscan.io/api?module=account&apikey=ETH-SECRET-KEY")

    assert response == "response"
    assert calls == [("https://api.etherscan.io/api?module=account&apikey=ETH-SECRET-KEY",
                      {"timeout": api_client.API_TIMEOUT})]
    connect, read = api_client.API_TIMEOUT
    assert connect > 0 and read > 0


@pytest.mark.parametrize("error", [requests.ConnectionError, requests.Timeout])
def test_api_get_errors_hide_api_key(monkeypatch, error):
    def failing_get(url, **kwargs):
        raise error(f"HTTPSConnectionPool(host='api.etherscan.io', port=443): Max retries exceeded with url: {url}")
    monkeypatch.setattr(api_client._session, "get", failing_get)
    # INFO: Same shape as core/*, the url (with apikey) is in a variable, not in the source line
    url = "https://api.etherscan.io/api?module=account&apikey=" + SECRET_CONFIG["ethscan"] + "&page=1"

    with pytest.raises(error) as raised:
        try:
            api_client.api_get(url)
        except error:
            # INFO: Same call used by core/* to stream tracebacks to the front
            traceback_text = traceback.format_exc()
            raise

    assert "ETH-SECRET-KEY" not in str(raised.value)
    assert "apikey=***&page=1" in str(raised.value)
    assert "ETH-SECRET-KEY" not in traceback_text


def test_urllib3_debug_logs_are_disabled():
    # INFO: urllib3 logs every requested url (with apikey) at DEBUG level
    assert not logging.getLogger("urllib3.connectionpool").isEnabledFor(logging.DEBUG)


@pytest.mark.parametrize("path", CORE_MODULES)
def test_core_calls_apis_only_through_api_get(path):
    code_lines = [line for line in Path(path).read_text().splitlines() if not line.strip().startswith("#")]

    assert [line for line in code_lines if "requests.get(" in line] == []


def test_public_params_masks_api_keys_without_mutating_input():
    params = {"address": "0xabc", "config": dict(SECRET_CONFIG)}

    public = api_client.public_params(params)

    assert public == {"address": "0xabc",
                      "config": {"ethscan": "***", "bscscan": "***", "level": "INFO", "dbname": "default.db"}}
    assert params["config"]["ethscan"] == "ETH-SECRET-KEY"


def test_redact_masks_api_key_in_url():
    assert api_client.redact("https://api.bscscan.com/api?address=0xa&apikey=BSC-SECRET-KEY") == \
        "https://api.bscscan.com/api?address=0xa&apikey=***"


def test_stream_routes_do_not_log_api_keys(client, monkeypatch, caplog):
    monkeypatch.setattr(eth, "event_stream_ether", fake_stream)
    monkeypatch.setattr(bsc, "event_stream_bsc", fake_stream)
    monkeypatch.setattr(misc, "event_stream_checking", fake_stream)
    caplog.set_level(logging.DEBUG)

    client.get("/stream-data-info", params={"address": "0xabc", "network": "eth"})
    client.get("/stream-data-info", params={"address": "0xabc", "network": "bsc"})
    client.get("/stream-data-checking", params={"action": "init"})

    assert caplog.text.count("PARAM") == 3
    assert "SECRET-KEY" not in caplog.text


def test_load_config_reads_yaml(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("ethscan: XXX\nbscscan: XXX\nlevel: INFO\ndbname: default.db\n")

    assert app_module.load_config(path) == {"ethscan": "XXX", "bscscan": "XXX", "level": "INFO", "dbname": "default.db"}


def test_load_config_rejects_python_tags(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("ethscan: XXX\nlevel: !!python/name:os.system\n")

    with pytest.raises(yaml.YAMLError):
        app_module.load_config(path)


class FakeTCPServer:
    def __init__(self, address, handler):
        FakeTCPServer.address = address

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def serve_forever(self):
        pass


def built_front(monkeypatch, tmp_path):
    # INFO: front/ is downloaded (just front), never committed; give httpServer a built front
    (tmp_path / "index.html").write_text("<html></html>")
    monkeypatch.setattr(app_module.misc, "front_dir", lambda: str(tmp_path))


def test_front_server_binds_localhost_by_default(monkeypatch, tmp_path):
    built_front(monkeypatch, tmp_path)
    monkeypatch.setattr(app_module.socketserver, "TCPServer", FakeTCPServer)

    app_module.httpServer()

    assert FakeTCPServer.address == ("127.0.0.1", 4200)


def test_front_server_binds_given_ip(monkeypatch, tmp_path):
    built_front(monkeypatch, tmp_path)
    monkeypatch.setattr(app_module.socketserver, "TCPServer", FakeTCPServer)

    app_module.httpServer("10.0.0.5")

    assert FakeTCPServer.address == ("10.0.0.5", 4200)
