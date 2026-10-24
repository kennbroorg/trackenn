"""Characterization of the collection flow (core/eth.py, core/bsc.py): pins what reaches the front and the DB today.

INFO: Safety net before deduplicating eth.py/bsc.py. Expected values are the current behavior, quirks included
(marked CURRENT), not a statement that they are right.
"""
import json
import sqlite3
from contextlib import closing

import pytest

from core import api_client, bsc, eth
from test_sql_injection import run_stream, stream_params

CENTRAL = "0xa0000000000000000000000000000000000000aa"
FUNDER = "0xf0000000000000000000000000000000000000ff"
FRIEND = "0xb0000000000000000000000000000000000000bb"
TOKEN = "0xc0000000000000000000000000000000000000cc"
T0 = 1_700_000_000
GAS = 2 * 21000 * 10**9 * 1e-18  # INFO: Two outgoing trxs paid by CENTRAL


def tx(n, frm, to, value, input="0x"):
    return {"blockNumber": str(100 + n), "timeStamp": str(T0 + n * 60), "hash": f"0xt{n}", "nonce": str(n),
            "blockHash": f"0xbh{n}", "transactionIndex": "0", "from": frm, "to": to, "value": str(value),
            "gas": "21000", "gasPrice": "1000000000", "isError": "0", "txreceipt_status": "1", "input": input,
            "contractAddress": "", "cumulativeGasUsed": "21000", "gasUsed": "21000", "confirmations": "10",
            "methodId": input[:10], "functionName": ""}


def internal(n, frm, to, value):
    return {"blockNumber": str(100 + n), "timeStamp": str(T0 + n * 60), "hash": f"0xt{n}", "from": frm, "to": to,
            "value": str(value), "contractAddress": "", "input": "", "type": "call", "gas": "0", "gasUsed": "0",
            "traceId": "0", "isError": "0", "errCode": ""}


def transfer(n, frm, to, value):
    return {"blockNumber": str(100 + n), "timeStamp": str(T0 + n * 60), "hash": f"0xt{n}", "nonce": str(n),
            "blockHash": f"0xbh{n}", "from": frm, "contractAddress": TOKEN, "to": to, "value": str(value),
            "tokenName": "Tether", "tokenSymbol": "USDT", "tokenDecimal": "6", "transactionIndex": "0", "gas": "60000",
            "gasPrice": "1000000000", "gasUsed": "50000", "cumulativeGasUsed": "50000", "input": "deprecated",
            "confirmations": "10"}


# INFO: FUNDER funds CENTRAL before its first outgoing trx; CENTRAL pays FRIEND, calls TOKEN, gets ETH back and USDT
EXPLORER = {
    "getcontractcreation": {"status": "0", "message": "No data found", "result": None},
    "txlist": {"status": "1", "message": "OK", "result": [
        tx(1, FUNDER, CENTRAL, 2 * 10**18), tx(2, CENTRAL, FRIEND, 10**18), tx(3, CENTRAL, TOKEN, 0, input="0xa9059cbb00")]},
    "txlistinternal": {"status": "1", "message": "OK", "result": [internal(3, TOKEN, CENTRAL, 5 * 10**17)]},
    "tokentx": {"status": "1", "message": "OK", "result": [transfer(4, FRIEND, CENTRAL, 2_500_000)]},
    "tokennfttx": {"status": "0", "message": "No transactions found", "result": []},
    "token1155tx": {"status": "0", "message": "No transactions found", "result": []},
}

CHAINS = [
    pytest.param(eth.event_stream_ether, "eth", "ETH", "Ether",
                 ["getcontractcreation", "txlist", "txlistinternal", "tokentx", "tokennfttx", "token1155tx"], id="eth"),
    # INFO: bsc does not query ERC1155 transfers (commented out in bsc.py)
    pytest.param(bsc.event_stream_bsc, "bsc", "BNB", "BNB",
                 ["getcontractcreation", "txlist", "txlistinternal", "tokentx", "tokennfttx"], id="bsc"),
]


@pytest.fixture
def explorer(monkeypatch):
    actions = []

    def get(url):
        action = url.split("action=")[1].split("&")[0]
        actions.append(action)
        response = api_client.requests.Response()
        response.status_code = 200
        response._content = json.dumps(EXPLORER[action]).encode()
        return response

    monkeypatch.setattr(api_client, "api_get", get)
    return actions


def collect(stream, db, chain, graph="Star"):
    params = stream_params(db, CENTRAL, chain)
    params["graph"] = graph
    messages = run_stream(stream, params)
    content = {key: value for message in messages for key, value in message.get("content", {}).items()}
    return messages, content


def rows(db, query):
    # INFO: Test-only reads, every query is a literal in this module
    with closing(sqlite3.connect(db)) as conn:
        return conn.execute(query).fetchall()


def links(content):
    return sorted((link["source"], link["target"]) for link in content["graph"]["links"])


@pytest.mark.parametrize("stream, chain, coin, coin_name, api_actions", CHAINS)
def test_new_address_queries_explorer_and_ends_ok(db, explorer, stream, chain, coin, coin_name, api_actions):
    messages, content = collect(stream, db, chain)

    assert explorer == api_actions
    assert all("exception" not in message and message["error"] is False for message in messages)
    assert messages[-1] == {"msg": "End collection", "end": True, "error": False, "content": {}}
    assert sorted(content) == ["address", "addresses", "balance", "funders", "gas", "graph", "labels", "list", "stat",
                               "tags", "tokens", "wallet_detail"]
    assert content["address"] == [chain, CENTRAL, "central"]
    assert content["addresses"] == [CENTRAL]


@pytest.mark.parametrize("stream, chain, coin, coin_name, api_actions", CHAINS)
def test_new_address_stores_collected_data(db, explorer, stream, chain, coin, coin_name, api_actions):
    collect(stream, db, chain)

    assert rows(db, "SELECT blockChain, hash FROM t_transactions ORDER BY hash") == [(chain, "0xt1"), (chain, "0xt2"), (chain, "0xt3")]
    assert rows(db, "SELECT blockChain, hash, `from`, `to` FROM t_internals") == [(chain, "0xt3", TOKEN, CENTRAL)]
    assert rows(db, "SELECT blockChain, hash, tokenSymbol, value FROM t_transfers") == [(chain, "0xt4", "USDT", 2_500_000)]
    assert rows(db, "SELECT * FROM t_blocks") == [(chain, CENTRAL, 101, 103, T0 + 60, T0 + 180, 1)]
    assert rows(db, "SELECT blockChain, address, first_block, first_hash, first_from, type FROM t_address_detail") == \
        [(chain, CENTRAL, 101, "0xt1", FUNDER, "wallet")]


@pytest.mark.parametrize("stream, chain, coin, coin_name, api_actions", CHAINS)
def test_new_address_tags_wallets_contracts_and_funders(db, explorer, stream, chain, coin, coin_name, api_actions):
    collect(stream, db, chain)

    assert rows(db, "SELECT blockChain, address, tag FROM t_tags ORDER BY address, tag") == [
        (chain, "", "wallet"),  # CURRENT: the empty contractAddress of plain trxs is tagged as a wallet
        (chain, CENTRAL, "central"),
        (chain, CENTRAL, "path"),
        (chain, CENTRAL, "wallet"),
        (chain, FRIEND, "wallet"),
        (chain, TOKEN, "contract"),
        (chain, FUNDER, "funder"),
        (chain, FUNDER, "wallet"),
    ]
    assert rows(db, "SELECT blockChain, type, hash, `from`, `to`, tokenSymbol FROM t_funders_creators") == \
        [(chain, "transaction", "0xt1", FUNDER, CENTRAL, coin)]


@pytest.mark.parametrize("stream, chain, coin, coin_name, api_actions", CHAINS)
def test_star_graph_list_and_stats(db, explorer, stream, chain, coin, coin_name, api_actions):
    _, content = collect(stream, db, chain)

    assert sorted(node["id"] for node in content["graph"]["nodes"]) == sorted([CENTRAL, FRIEND, TOKEN, FUNDER])
    assert links(content) == sorted([(FUNDER, CENTRAL), (CENTRAL, FRIEND), (CENTRAL, TOKEN), (TOKEN, CENTRAL), (FRIEND, CENTRAL)])
    trx_list = json.loads(content["list"])
    assert [(row["hash"], row["type"], row["timeStamp"]) for row in trx_list] == [
        ("0xt1", "transaction", (T0 + 60) * 1000),
        ("0xt2", "transaction", (T0 + 120) * 1000),
        ("0xt3", "transaction", (T0 + 180) * 1000),
        ("0xt3", "internals", (T0 + 180) * 1000),
        ("0xt4", "transfers", (T0 + 240) * 1000),
    ]
    stat_wal = 3 if chain == "eth" else 4  # CURRENT: bsc counts one more wallet than eth for the same data
    assert content["stat"] == {"stat_trx": 3, "stat_int": 1, "stat_tra": 1, "stat_wal": stat_wal, "stat_tot": 5,
                               "stat_con": 1, "stat_err": 0, "stat_coo": 1}


@pytest.mark.parametrize("stream, chain, coin, coin_name, api_actions", CHAINS)
def test_funders_balance_and_tags_content(db, explorer, stream, chain, coin, coin_name, api_actions):
    _, content = collect(stream, db, chain)

    funders = json.loads(content["funders"]["funders"])
    assert [(f["hash"], f["from"], f["to"], f["tokenSymbol"]) for f in funders] == [("0xt1", FUNDER, CENTRAL, coin)]
    assert content["balance"] == [{"blockChain": chain, "address": CENTRAL, "token": coin, "tokenName": coin_name,
                                   "balance": pytest.approx(2 - 1 + 0.5 - GAS)}]
    assert content["tokens"] == [{"blockChain": chain, "address": CENTRAL, "token": "USDT", "tokenName": "Tether",
                                  "balance": pytest.approx(2.5)}]
    assert content["gas"] == pytest.approx(GAS)
    assert content["tags"] == [{"blockChain": chain, "address": CENTRAL, "tag": tag} for tag in ["central", "path", "wallet"]]
    assert content["labels"] == []


def test_complex_graph_classifies_nodes_and_links(db, explorer):
    _, content = collect(eth.event_stream_ether, db, "eth", graph="Complex")

    assert rows(db, "SELECT count(*) FROM t_nodes_classification") == [(4,)]
    assert rows(db, "SELECT count(*) FROM t_links_classification") == [(4,)]
    # CURRENT: unlike Star, Complex has no link for the zero-value contract call CENTRAL -> TOKEN
    assert links(content) == sorted([(FUNDER, CENTRAL), (CENTRAL, FRIEND), (TOKEN, CENTRAL), (FRIEND, CENTRAL)])
    assert [row["timeStamp"] for row in json.loads(content["list"])] == \
        [(T0 + 60) * 1000, (T0 + 120) * 1000, (T0 + 180) * 1000, (T0 + 180) * 1000, (T0 + 240) * 1000]
    assert content["stat"] == {"stat_tot": 5, "stat_trx": 3, "stat_tra": 1, "stat_err": 0, "stat_int": 1, "stat_wal": 3,
                               "stat_con": 1, "stat_coo": 0, "stat_nft": 0, "stat_mul": 0}


@pytest.mark.parametrize("graph", ["Star", "Complex"])
def test_cached_address_is_served_from_db_without_explorer(db, explorer, graph):
    _, first = collect(eth.event_stream_ether, db, "eth", graph=graph)
    explorer.clear()
    stored = rows(db, "SELECT count(*) FROM t_transactions")

    messages, again = collect(eth.event_stream_ether, db, "eth", graph=graph)

    assert explorer == []
    assert messages[-1]["end"] is True and messages[-1]["error"] is False
    assert rows(db, "SELECT count(*) FROM t_transactions") == stored
    for key in ["graph", "list", "stat", "funders", "balance", "tokens", "gas", "tags", "labels"]:
        assert again[key] == first[key], key


@pytest.mark.parametrize("stream, chain, coin, coin_name, api_actions", CHAINS)
def test_sse_messages_are_plain_text(db, explorer, stream, chain, coin, coin_name, api_actions):
    # INFO: the front renders log lines as text (T4.21); messages must not carry
    # HTML markup like <strong> or it shows up literally in the log panel.
    messages, _ = collect(stream, db, chain)
    offenders = [m["msg"] for m in messages if "<strong>" in m["msg"]]
    assert offenders == []


@pytest.fixture
def rate_limited_explorer(monkeypatch):
    # INFO: Live bug found manually (mitest stack, real Etherscan API, address
    # 0x99b9c2b49ff03dd1f4d6e537c4d94d73a75e7c4f, 2026-10-24): Etherscan answers NOTOK
    # (e.g. rate limit, invalid key) with 'result' as an error STRING, not a list. No
    # call site validated status/message before feeding 'result' into pd.DataFrame,
    # crashing with an opaque "DataFrame constructor not properly called!" instead of
    # surfacing the explorer's own error text. Pre-existing gap, independent of any
    # concurrency change -- reproducible with plain sequential calls once the free-tier
    # quota is spent.
    NOTOK = {"status": "0", "message": "NOTOK", "result": "Max rate limit reached"}

    def get(url):
        action = url.split("action=")[1].split("&")[0]
        payload = NOTOK if action == "tokentx" else EXPLORER[action]
        response = api_client.requests.Response()
        response.status_code = 200
        response._content = json.dumps(payload).encode()
        return response

    monkeypatch.setattr(api_client, "api_get", get)


@pytest.mark.parametrize("stream, chain", [pytest.param(eth.event_stream_ether, "eth", id="eth"),
                                            pytest.param(bsc.event_stream_bsc, "bsc", id="bsc")])
def test_notok_response_surfaces_explorer_error_not_a_dataframe_crash(db, rate_limited_explorer, stream, chain):
    messages, _ = collect(stream, db, chain)

    assert messages[-1]["error"] is True
    assert messages[-1]["end"] is True
    assert any("Max rate limit reached" in m["msg"] for m in messages)
    assert not any("DataFrame constructor not properly called" in m["msg"] for m in messages)
