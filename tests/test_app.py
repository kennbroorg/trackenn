import json

import pytest
from fastapi.testclient import TestClient

import app as app_module
from core import bsc, eth, misc

CONFIG = {"ethscan": "K1", "bscscan": "K2", "level": "INFO", "dbname": "default.db"}


@pytest.fixture
def client():
    app_module.app.state.config = dict(CONFIG)
    with TestClient(app_module.app) as test_client:
        yield test_client


def fake_stream(calls, name):
    def stream(params):
        calls.append((name, params))
        yield f"data:{json.dumps({'msg': name, 'end': True})}\n\n"
    return stream


def test_stream_data_info_eth_streams_ether_events(client, monkeypatch):
    calls = []
    monkeypatch.setattr(eth, "event_stream_ether", fake_stream(calls, "eth"))
    monkeypatch.setattr(bsc, "event_stream_bsc", fake_stream(calls, "bsc"))

    response = client.get("/stream-data-info", params={"address": "0xabc", "network": "eth", "graph": "Complex"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.text == 'data:{"msg": "eth", "end": true}\n\n'
    assert calls == [("eth", {"address": "0xabc", "network": "eth", "graph": "Complex",
                              "config": CONFIG, "filters": {"filter": False}})]


def test_stream_data_info_bsc_streams_bsc_events(client, monkeypatch):
    calls = []
    monkeypatch.setattr(eth, "event_stream_ether", fake_stream(calls, "eth"))
    monkeypatch.setattr(bsc, "event_stream_bsc", fake_stream(calls, "bsc"))

    response = client.get("/stream-data-info", params={"address": "0xabc", "network": "bsc"})

    assert response.status_code == 200
    assert response.text == 'data:{"msg": "bsc", "end": true}\n\n'
    assert [name for name, _ in calls] == ["bsc"]


def test_stream_data_info_parses_filters_json(client, monkeypatch):
    calls = []
    monkeypatch.setattr(eth, "event_stream_ether", fake_stream(calls, "eth"))

    filters = {"filter": True, "from": 1}
    client.get("/stream-data-info", params={"address": "0xabc", "network": "eth", "filters": json.dumps(filters)})

    assert calls[0][1]["filters"] == filters


def test_stream_data_info_keeps_first_value_of_repeated_params(client, monkeypatch):
    calls = []
    monkeypatch.setattr(eth, "event_stream_ether", fake_stream(calls, "eth"))

    client.get("/stream-data-info?address=0xfirst&address=0xsecond&network=eth")

    assert calls[0][1]["address"] == "0xfirst"


def test_stream_data_checking_defaults_graph_to_star(client, monkeypatch):
    calls = []
    monkeypatch.setattr(misc, "event_stream_checking", fake_stream(calls, "checking"))

    response = client.get("/stream-data-checking", params={"action": "init"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.text == 'data:{"msg": "checking", "end": true}\n\n'
    assert calls == [("checking", {**CONFIG, "action": "init", "graph": "Star", "filters": {"filter": False}})]


def test_stream_data_checking_uses_given_graph_and_filters(client, monkeypatch):
    calls = []
    monkeypatch.setattr(misc, "event_stream_checking", fake_stream(calls, "checking"))

    client.get("/stream-data-checking",
               params={"action": "reload", "graph": "Complex", "filters": json.dumps({"filter": True})})

    config = calls[0][1]
    assert (config["action"], config["graph"], config["filters"]) == ("reload", "Complex", {"filter": True})


def test_cors_allows_only_front_origin(client, monkeypatch):
    monkeypatch.setattr(misc, "event_stream_checking", fake_stream([], "checking"))

    allowed = client.get("/stream-data-checking", params={"action": "init"},
                         headers={"Origin": "http://127.0.0.1:4200"})
    denied = client.get("/stream-data-checking", params={"action": "init"},
                        headers={"Origin": "http://evil.example"})

    assert allowed.headers["access-control-allow-origin"] == "http://127.0.0.1:4200"
    assert "access-control-allow-origin" not in denied.headers


def test_download_db_sends_default_db_as_attachment(client, monkeypatch, tmp_path):
    (tmp_path / "default.db").write_bytes(b"SQLite format 3\x00data")
    monkeypatch.chdir(tmp_path)

    response = client.get("/download_db")

    assert response.status_code == 200
    assert response.content == b"SQLite format 3\x00data"
    assert response.headers["content-disposition"].startswith("attachment;")
    assert 'filename="default.db"' in response.headers["content-disposition"]


def test_reset_db_streams_reset_investigation(client, monkeypatch):
    calls = []
    monkeypatch.setattr(misc, "event_reset_investigation", fake_stream(calls, "reset"), raising=False)

    response = client.get("/reset_db")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert calls == [("reset", "./default.db")]


def test_recreatedb_streams_generator_result(client, monkeypatch):
    calls = []
    monkeypatch.setattr(eth, "recreate_db", fake_stream(calls, "recreate"))

    response = client.get("/recreatedb", params={"address": "0xabc"})

    assert response.status_code == 200
    assert response.text == 'data:{"msg": "recreate", "end": true}\n\n'
    assert calls == [("recreate", {"address": "0xabc", "config": CONFIG})]


def test_internal_test_1_streams_generator_result(client, monkeypatch):
    monkeypatch.setattr(eth, "test_function_1", fake_stream([], "test1"))

    response = client.get("/internal-test-1", params={"address": "0xabc"})

    assert response.status_code == 200
    assert response.text == 'data:{"msg": "test1", "end": true}\n\n'


def test_internal_test_2_returns_dict_as_json(client, monkeypatch):
    monkeypatch.setattr(eth, "test_function_2", lambda params: {"stat": {"n": 1}, "address": params["address"]})

    response = client.get("/internal-test-2", params={"address": "0xabc"})

    assert response.status_code == 200
    assert response.json() == {"stat": {"n": 1}, "address": "0xabc"}
