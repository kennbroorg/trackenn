import json
import sqlite3
from contextlib import closing

import pytest

from core import api_client, bsc, eth, misc
from test_sql_injection import insert, run_stream, seed_activity, stream_params

UNAVAILABLE = {"balance": [], "tokens": [], "gas": [], "warning": "Balance not available"}
WARNING_MSG = "<strong>BALANCE</strong> - Balance not available"
SECRET = "SECRETKEY"


def response(status, body):
    result = api_client.requests.Response()
    result.status_code = status
    result._content = body.encode()
    return result


def raises(error):
    def get(url):
        raise error
    return get


def answers(status, body):
    return lambda url: response(status, body)


FAILURES = [
    pytest.param(raises(api_client.requests.Timeout(f"timed out apikey={SECRET}")), id="timeout"),
    pytest.param(raises(api_client.requests.ConnectionError(f"refused apikey={SECRET}")), id="connection"),
    pytest.param(answers(502, "<html>Bad Gateway</html>"), id="non-json"),
    pytest.param(answers(200, '{"status": "0", "message": "NOTOK", "result": "Invalid API Key"}'), id="invalid-key"),
    pytest.param(answers(200, '{"status": "1"}'), id="missing-result"),
    pytest.param(answers(200, '{"status": "0", "result": null}'), id="null-result"),
]
MODULES = [pytest.param(eth, "eth", "ETH", "Ether", id="eth"), pytest.param(bsc, "bsc", "BNB", "BNB", id="bsc")]


@pytest.mark.parametrize("module, chain, token, token_name", MODULES)
@pytest.mark.parametrize("get", FAILURES)
def test_contract_balance_unavailable(db, monkeypatch, module, chain, token, token_name, get):
    monkeypatch.setattr(api_client, "api_get", get)

    with closing(sqlite3.connect(db)) as conn:
        result = module.get_balance_and_gas(conn, (chain, "0xc", "central"), "contract", SECRET)

    assert result == UNAVAILABLE


@pytest.mark.parametrize("module, chain, token, token_name", MODULES)
def test_contract_balance_available_is_unchanged(db, monkeypatch, module, chain, token, token_name):
    monkeypatch.setattr(api_client, "api_get", answers(200, '{"status": "1", "result": "2500000000000000000"}'))

    with closing(sqlite3.connect(db)) as conn:
        result = module.get_balance_and_gas(conn, (chain, "0xc", "central"), "contract", SECRET)

    assert result == {"balance": [{"blockChain": chain, "balance": 2.5, "token": token, "tokenName": token_name}],
                      "tokens": [], "gas": []}


def seed_contract(path, chain):
    insert(path, "t_address_detail", blockChain=chain, address="0xc", type="contract")
    seed_activity(path, chain, "0xc")


def balance_messages(messages):
    return [m for m in messages if "Balance" in m.get("msg", "")]


def assert_degraded(messages, last_msg):
    assert "exception" not in json.dumps(messages)
    assert SECRET not in json.dumps(messages)
    warning, data = balance_messages(messages)
    assert warning == {"msg": WARNING_MSG, "end": False, "error": False, "content": {}}
    assert data["content"] == {"balance": [], "tokens": [], "gas": []}
    assert data["error"] is False and data["end"] is False
    assert any("tags" in m["content"] for m in messages)
    assert messages[-1] == {"msg": last_msg, "end": True, "error": False, "content": {}}


STREAMS = [pytest.param(eth.event_stream_ether, "eth", id="eth"), pytest.param(bsc.event_stream_bsc, "bsc", id="bsc")]


@pytest.mark.parametrize("stream, chain", STREAMS)
@pytest.mark.parametrize("get", FAILURES)
def test_stream_goes_on_without_contract_balance(db, monkeypatch, stream, chain, get):
    seed_contract(db, chain)
    monkeypatch.setattr(api_client, "api_get", get)

    messages = run_stream(stream, stream_params(db, "0xc", chain))

    assert_degraded(messages, "<strong>End collection</strong>")


@pytest.mark.parametrize("chain", ["eth", "bsc"])
@pytest.mark.parametrize("get", FAILURES)
def test_checking_goes_on_without_contract_balance(db, monkeypatch, chain, get):
    seed_contract(db, chain)
    insert(db, "t_tags", blockChain=chain, address="0xc", tag="central")
    monkeypatch.setattr(api_client, "api_get", get)
    config = {"ethscan": SECRET, "bscscan": SECRET, "level": "INFO", "dbname": str(db),
              "action": "init", "graph": "Star", "filters": {"filter": False}}

    messages = run_stream(misc.event_stream_checking, config)

    assert_degraded(messages, "End checking...\n")


@pytest.mark.parametrize("stream, chain", STREAMS)
def test_stream_with_contract_balance_has_no_warning(db, monkeypatch, stream, chain):
    seed_contract(db, chain)
    monkeypatch.setattr(api_client, "api_get", answers(200, '{"status": "1", "result": "1000000000000000000"}'))

    messages = run_stream(stream, stream_params(db, "0xc", chain))

    [data] = balance_messages(messages)
    assert data["content"]["balance"][0]["balance"] == 1.0
    assert messages[-1]["end"] is True and messages[-1]["error"] is False
