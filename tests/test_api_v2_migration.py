"""Etherscan shut down API V1 (returns NOTOK/'deprecated V1 endpoint'). This module pins the
migration to API V2: a single host (api.etherscan.io/v2/api) with chainid=1 (ETH) / chainid=56
(BSC), the same module/action/param names, and the Etherscan key used for both chains (bscscan
becomes an optional legacy fallback). See core/api_client.py: explorer_url, ETH_CHAIN_ID, BSC_CHAIN_ID.
"""

import json
import sqlite3
from contextlib import closing
from urllib.parse import parse_qsl, urlsplit

import pytest

from core import api_client, bsc, eth
from test_eth_logic import CENTRAL, CHAINS, EXPLORER, tx
from test_sql_injection import api_calls, run_stream, stream_params

V2_BASE = "https://api.etherscan.io/v2/api"


def parse_url(url):
    """Split a url into (base, query-dict); every query key is expected once."""
    parts = urlsplit(url)
    base = f"{parts.scheme}://{parts.netloc}{parts.path}"
    query = dict(parse_qsl(parts.query))
    return base, query


def fake_response(body):
    response = api_client.requests.Response()
    response.status_code = 200
    response._content = json.dumps(body).encode()
    return response


# ---------------------------------------------------------------------------
# Unit tests for the explorer_url helper (core/api_client.py)
# ---------------------------------------------------------------------------


def test_chain_ids_match_the_etherscan_v2_chainlist():
    assert api_client.ETH_CHAIN_ID == 1
    assert api_client.BSC_CHAIN_ID == 56


def test_explorer_url_builds_v2_host_and_chainid():
    url = api_client.explorer_url(api_client.ETH_CHAIN_ID, module="account", action="balance", address="0xabc", apikey="K")

    base, query = parse_url(url)

    assert base == V2_BASE
    assert query == {"chainid": "1", "module": "account", "action": "balance", "address": "0xabc", "apikey": "K"}


def test_explorer_url_uses_the_given_chainid_for_bsc():
    url = api_client.explorer_url(api_client.BSC_CHAIN_ID, module="contract", action="getsourcecode", address="0xc", apikey="K")

    base, query = parse_url(url)

    assert base == V2_BASE
    assert query["chainid"] == "56"


# ---------------------------------------------------------------------------
# Every call site in the wallet collection flow must hit V2 (grouped by action)
# ---------------------------------------------------------------------------


@pytest.fixture
def explorer_urls(monkeypatch):
    """Same behavior as test_eth_logic.explorer, but records full urls, not just the action."""
    calls = []

    def get(url):
        action = parse_url(url)[1]["action"]
        calls.append(url)
        return fake_response(EXPLORER[action])

    monkeypatch.setattr(api_client, "api_get", get)
    return calls


@pytest.mark.parametrize("stream, chain, coin, coin_name, api_actions", CHAINS)
def test_wallet_flow_queries_v2_urls_for_every_action(db, explorer_urls, stream, chain, coin, coin_name, api_actions):
    config = {"ethscan": "ETH-KEY", "bscscan": "BSC-KEY", "level": "INFO", "dbname": str(db)}
    params = {"address": CENTRAL, "network": chain, "graph": "Star", "filters": {"filter": False}, "config": config}
    expected_chainid = str(api_client.ETH_CHAIN_ID if chain == "eth" else api_client.BSC_CHAIN_ID)

    run_stream(stream, params)

    assert [parse_url(url)[1]["action"] for url in explorer_urls] == api_actions
    for url in explorer_urls:
        base, query = parse_url(url)
        assert base == V2_BASE
        assert query["chainid"] == expected_chainid
        assert query["apikey"] == "ETH-KEY"  # INFO: both chains use the Etherscan key


# ---------------------------------------------------------------------------
# Contract branch call sites (getsourcecode, creation txlist) also hit V2
# ---------------------------------------------------------------------------

CONTRACT_CREATOR = "0xf00000000000000000000000000000000000f00f"
CONTRACT_SOURCE = [{"ContractName": "Token", "SourceCode": "", "ABI": "", "CompilerVersion": "", "OptimizationUsed": "",
                     "Runs": "", "ConstructorArguments": "", "EVMVersion": "", "Library": "", "LicenseType": "",
                     "Proxy": "", "Implementation": "", "SwarmSource": ""}]
CONTRACT_EXPLORER = {
    "getcontractcreation": {"status": "1", "message": "OK", "result": [{"contractCreator": CONTRACT_CREATOR, "txHash": "0xc1"}]},
    "getsourcecode": {"status": "1", "message": "OK", "result": CONTRACT_SOURCE},
    "txlist": {"status": "1", "message": "OK", "result": [tx(1, CONTRACT_CREATOR, CENTRAL, 0)]},
    # INFO: a contract address also gets its balance fetched later in the same stream
    "balance": {"status": "1", "message": "OK", "result": "1000000000000000000"},
}


@pytest.fixture
def contract_explorer_urls(monkeypatch):
    calls = []

    def get(url):
        action = parse_url(url)[1]["action"]
        calls.append(url)
        return fake_response(CONTRACT_EXPLORER[action])

    monkeypatch.setattr(api_client, "api_get", get)
    return calls


@pytest.mark.parametrize("stream, chain", [(eth.event_stream_ether, "eth"), (bsc.event_stream_bsc, "bsc")])
def test_contract_branch_queries_v2_urls(db, contract_explorer_urls, stream, chain):
    config = {"ethscan": "ETH-KEY", "bscscan": "BSC-KEY", "level": "INFO", "dbname": str(db)}
    params = {"address": CENTRAL, "network": chain, "graph": "Star", "filters": {"filter": False}, "config": config}
    expected_chainid = str(api_client.ETH_CHAIN_ID if chain == "eth" else api_client.BSC_CHAIN_ID)

    run_stream(stream, params)

    actions = [parse_url(url)[1]["action"] for url in contract_explorer_urls]
    # INFO: the first three calls are the contract branch; "balance" follows later in the same stream
    assert actions[:3] == ["getcontractcreation", "getsourcecode", "txlist"]
    for url, action in zip(contract_explorer_urls, actions):
        base, query = parse_url(url)
        assert base == V2_BASE
        assert query["chainid"] == expected_chainid
        assert query["apikey"] == "ETH-KEY"
    # INFO: the creation-specific txlist call still asks for a single page/trx
    _, txlist_query = parse_url(contract_explorer_urls[2])
    assert txlist_query["page"] == "1" and txlist_query["offset"] == "1"


# ---------------------------------------------------------------------------
# Balance call site (eth.get_balance_and_gas / bsc.get_balance_and_gas)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("module, chain", [(eth, "eth"), (bsc, "bsc")])
def test_balance_endpoint_uses_v2_url(db, monkeypatch, module, chain):
    calls = []

    def get(url):
        calls.append(url)
        return fake_response({"status": "1", "result": "1000000000000000000"})

    monkeypatch.setattr(api_client, "api_get", get)

    with closing(sqlite3.connect(db)) as conn:
        module.get_balance_and_gas(conn, (chain, "0xc", "central"), "contract", "BAL-KEY")

    assert len(calls) == 1
    base, query = parse_url(calls[0])
    assert base == V2_BASE
    assert query["chainid"] == str(api_client.ETH_CHAIN_ID if chain == "eth" else api_client.BSC_CHAIN_ID)
    assert query["module"] == "account"
    assert query["action"] == "balance"
    assert query["apikey"] == "BAL-KEY"


# ---------------------------------------------------------------------------
# Error surfacing: the NOTOK message (e.g. "deprecated V1 endpoint", invalid key,
# chain not in plan) must still reach the front through the existing SSE path.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("stream, chain", [(eth.event_stream_ether, "eth"), (bsc.event_stream_bsc, "bsc")])
def test_notok_message_reaches_the_front(db, monkeypatch, stream, chain):
    def get(url):
        return fake_response({"status": "0", "message": "NOTOK",
                               "result": "Free API access is not supported for this chain"})

    monkeypatch.setattr(api_client, "api_get", get)
    params = stream_params(db, "0xabc", chain)

    messages = run_stream(stream, params)

    assert any("Free API access is not supported for this chain" in message.get("msg", "") for message in messages)
    assert messages[-1]["end"] is True and messages[-1]["error"] is True


# ---------------------------------------------------------------------------
# BSC key resolution: Etherscan key first, bscscan only as a legacy fallback,
# never crash if bscscan is missing from config.
# ---------------------------------------------------------------------------


def test_bsc_uses_the_etherscan_key_when_both_are_configured(db, api_calls):
    config = {"ethscan": "ETH-KEY", "bscscan": "BSC-KEY", "level": "INFO", "dbname": str(db)}
    params = {"address": "0xabc", "network": "bsc", "graph": "Star", "filters": {"filter": False}, "config": config}

    run_stream(bsc.event_stream_bsc, params)

    assert len(api_calls) == 1
    _, query = parse_url(api_calls[0])
    assert query["apikey"] == "ETH-KEY"


def test_bsc_falls_back_to_bscscan_key_when_ethscan_is_missing(db, api_calls):
    config = {"bscscan": "BSC-KEY", "level": "INFO", "dbname": str(db)}  # INFO: no ethscan key at all
    params = {"address": "0xabc", "network": "bsc", "graph": "Star", "filters": {"filter": False}, "config": config}

    run_stream(bsc.event_stream_bsc, params)

    assert len(api_calls) == 1
    _, query = parse_url(api_calls[0])
    assert query["apikey"] == "BSC-KEY"


def test_bsc_falls_back_to_bscscan_key_when_ethscan_is_empty(db, api_calls):
    config = {"ethscan": "", "bscscan": "BSC-KEY", "level": "INFO", "dbname": str(db)}
    params = {"address": "0xabc", "network": "bsc", "graph": "Star", "filters": {"filter": False}, "config": config}

    run_stream(bsc.event_stream_bsc, params)

    assert len(api_calls) == 1
    _, query = parse_url(api_calls[0])
    assert query["apikey"] == "BSC-KEY"


def test_bsc_never_crashes_when_bscscan_key_is_absent(db, api_calls):
    config = {"ethscan": "ETH-KEY", "level": "INFO", "dbname": str(db)}  # INFO: no bscscan key at all
    params = {"address": "0xabc", "network": "bsc", "graph": "Star", "filters": {"filter": False}, "config": config}

    messages = run_stream(bsc.event_stream_bsc, params)

    assert not any("exception" in message for message in messages)
    assert len(api_calls) == 1
    _, query = parse_url(api_calls[0])
    assert query["apikey"] == "ETH-KEY"
