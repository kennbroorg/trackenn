"""odd/tasks/backend-backlog-c.md, T5 S5 (R7): GET /balances computed from stored data,
spam section, NFT/1155 counts, partial/negative decisions and the optional on-chain check."""
import json
import sqlite3
from contextlib import closing

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app as app_module
from core import api_client, balances, decisions

CENTRAL = "0xc2222222222222222222222222222222222222cc"
FRIEND = "0xf1111111111111111111111111111111111111ff"
REAL_WBTC = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
FAKE_WBTC = "0xfa0000000000000000000000000000000000000b"
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
NFT = "0x4e00000000000000000000000000000000000001"
MULTI = "0x1155000000000000000000000000000000000001"

CONFIG = {"ethscan": "SECRETKEY", "bscscan": "K2", "level": "INFO"}


@pytest.fixture
def client(db):
    app_module.app.state.config = {**CONFIG, "dbname": str(db)}
    with TestClient(app_module.app) as test_client:
        yield test_client


def insert(path, table, **values):
    with closing(sqlite3.connect(path)) as conn:
        columns = [row[0] for row in conn.execute(f"SELECT name FROM pragma_table_info('{table}')")]
        row = {column: values.get(column, 0) for column in columns}
        row.setdefault("blockChain", "eth")
        row["blockChain"] = values.get("blockChain", "eth")
        pd.DataFrame([row]).to_sql(table, conn, if_exists="append", index=False)
        conn.commit()


def token(path, contract, frm, to, value, symbol, decimals=8, name=None):
    insert(path, "t_transfers", contractAddress=contract, **{"from": frm, "to": to}, value=str(value),
           tokenSymbol=symbol, tokenName=name or symbol, tokenDecimal=str(decimals), hash=f"0x{contract[-4:]}{frm[-4:]}{value}")


def tx(path, frm, to, value, gas_used=0, gas_price=0, is_error=0):
    insert(path, "t_transactions", **{"from": frm, "to": to}, value=str(value), gasUsed=str(gas_used),
           gasPrice=str(gas_price), isError=str(is_error), hash=f"0xt{value}{gas_used}")


def tag(path, contract, value):
    insert(path, "t_tags", address=contract, tag=value)


def table_counts(path):
    with closing(sqlite3.connect(path)) as conn:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        return {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables}


def by_contract(rows):
    return {row["contract"]: row for row in rows}


def codes(body):
    return [d["code"] for d in body["decisions"]]


def test_erc20_net_is_inflow_minus_outflow_per_real_token(client, db):
    token(db, USDC, FRIEND, CENTRAL, 5_000_000, "USDC", 6)
    token(db, USDC, CENTRAL, FRIEND, 2_000_000, "USDC", 6)
    body = client.get("/balances", params={"address": CENTRAL}).json()
    usdc = by_contract(body["tokens"])[USDC]
    assert (usdc["inflow"], usdc["outflow"], usdc["net"]) == ("5", "2", "3")
    assert (usdc["transfers_in"], usdc["transfers_out"], usdc["flagged_tags"]) == (1, 1, [])
    assert body["spam"] == [] and body["decisions"] == []


def test_fake_namesake_is_separated_from_real_token_with_its_tags(client, db):
    token(db, REAL_WBTC, FRIEND, CENTRAL, 100_000_000, "WBTC")
    token(db, FAKE_WBTC, FRIEND, CENTRAL, 100_000_000, "WBTC")
    tag(db, FAKE_WBTC, "spam-phishing")
    tag(db, FAKE_WBTC, "spam-unsolicited")
    body = client.get("/balances", params={"address": CENTRAL}).json()
    assert list(by_contract(body["tokens"])) == [REAL_WBTC]
    assert by_contract(body["spam"])[FAKE_WBTC]["flagged_tags"] == ["spam-phishing", "spam-unsolicited"]


def test_eth_net_adds_transactions_and_internals_and_subtracts_fees(client, db):
    tx(db, FRIEND, CENTRAL, 10 * 10**18)
    tx(db, CENTRAL, FRIEND, 3 * 10**18, gas_used=21000, gas_price=10**9)
    tx(db, CENTRAL, FRIEND, 7 * 10**18, gas_used=21000, gas_price=10**9, is_error=1)
    insert(db, "t_internals", **{"from": FRIEND, "to": CENTRAL}, value=str(2 * 10**18), isError="0")
    insert(db, "t_internals", **{"from": FRIEND, "to": CENTRAL}, value=str(5 * 10**18), isError="1")
    eth = client.get("/balances", params={"address": CENTRAL}).json()["eth"]
    assert (eth["inflow"], eth["outflow"], eth["fees"]) == ("12", "3", "0.000042")
    assert eth["net"] == "8.999958"


def test_partial_history_and_negative_balance_leave_decisions(client, db):
    token(db, USDC, CENTRAL, FRIEND, 1_000_000, "USDC", 6)
    with closing(sqlite3.connect(db)) as conn:
        decisions.persist_new(conn, "/stream-data-info", CENTRAL, "eth", [decisions.make(
            "history_incomplete", CENTRAL, "MAX_ROWS_PER_LIST=10000", "tokentx hit its row cap",
            {"action": "tokentx"})])
    body = client.get("/balances", params={"address": CENTRAL}).json()
    assert codes(body) == ["balance_partial", "balance_negative"]
    assert body["decisions"][0]["details"] == {"actions": ["tokentx"]}
    assert body["decisions"][1]["scope"] == USDC and body["decisions"][1]["details"]["net"] == "-1"


def test_nfts_and_multitokens_are_counted_outside_the_monetary_total(client, db):
    insert(db, "t_nfts", contractAddress=NFT, tokenID="7", **{"from": FRIEND, "to": CENTRAL}, tokenSymbol="APE")
    insert(db, "t_multitoken", contractAddress=MULTI, tokenID="1", tokenValue="5",
           **{"from": FRIEND, "to": CENTRAL}, tokenSymbol="ITEM")
    insert(db, "t_multitoken", contractAddress=MULTI, tokenID="1", tokenValue="2",
           **{"from": CENTRAL, "to": FRIEND}, tokenSymbol="ITEM")
    body = client.get("/balances", params={"address": CENTRAL}).json()
    nfts = {(n["contract"], n["token_id"]): n for n in body["nfts"]}
    assert nfts[(NFT, "7")] == {"contract": NFT, "token_id": "7", "symbol": "APE", "standard": "erc721",
                                "units_in": 1, "units_out": 0, "held": 1}
    assert (nfts[(MULTI, "1")]["standard"], nfts[(MULTI, "1")]["held"]) == ("erc1155", 3)
    assert body["tokens"] == [] and body["spam"] == []


def test_computed_balances_make_no_api_call_and_no_write(client, db, monkeypatch):
    token(db, USDC, FRIEND, CENTRAL, 5_000_000, "USDC", 6)
    monkeypatch.setattr(api_client, "api_get", lambda url: pytest.fail("no API call expected"))
    before = table_counts(db)
    first = client.get("/balances", params={"address": CENTRAL}).json()
    second = client.get("/balances", params={"address": CENTRAL}).json()
    assert first == second and table_counts(db) == before
    assert "onchain" not in first


class FakeResponse:
    def __init__(self, result):
        self.result = result

    def json(self):
        return {"status": "1", "message": "OK", "result": self.result}


def fake_chain(balances_by_contract, calls):
    def api_get(url):
        calls.append(url)
        for contract, value in balances_by_contract.items():
            if (contract is None and "action=balance&" in url) or (contract and f"contractaddress={contract}" in url):
                return FakeResponse(str(value))
        raise AssertionError(url)
    return api_get


def test_onchain_shows_both_values_and_a_mismatch_decision(client, db, monkeypatch):
    tx(db, FRIEND, CENTRAL, 10**18)
    token(db, USDC, FRIEND, CENTRAL, 5_000_000, "USDC", 6)
    calls = []
    monkeypatch.setattr(api_client, "api_get", fake_chain({None: 10**18, USDC: 4_000_000}, calls))
    response = client.get("/balances", params={"address": CENTRAL, "onchain": "true"})
    body = response.json()
    assert body["onchain"]["eth"] == {"computed": "1", "chain": "1", "difference": "0"}
    assert body["onchain"]["tokens"][USDC] == {"computed": "5", "chain": "4", "difference": "-1"}
    mismatch = [d for d in body["decisions"] if d["code"] == "balance_mismatch"]
    assert [d["scope"] for d in mismatch] == [USDC]
    assert all("SECRETKEY" not in json.dumps(d) for d in body["decisions"])
    assert "SECRETKEY" not in response.text and len(calls) == 2


def test_spam_tokens_are_never_queried_on_chain(client, db, monkeypatch):
    token(db, FAKE_WBTC, FRIEND, CENTRAL, 1, "WBTC")
    tag(db, FAKE_WBTC, "spam-phishing")
    calls = []
    monkeypatch.setattr(api_client, "api_get", fake_chain({None: 0}, calls))
    client.get("/balances", params={"address": CENTRAL, "onchain": "true"})
    assert len(calls) == 1


def test_call_cap_compares_eth_only_and_lists_the_rest(client, db, monkeypatch):
    token(db, USDC, FRIEND, CENTRAL, 5_000_000, "USDC", 6)
    token(db, REAL_WBTC, FRIEND, CENTRAL, 1, "WBTC")
    calls = []
    monkeypatch.setattr(api_client, "api_get", fake_chain({None: 0}, calls))
    body = client.get("/balances", params={"address": CENTRAL, "onchain": "true", "max_calls": "1"}).json()
    assert list(body["onchain"]["tokens"]) == [] and len(calls) == 1
    cap = [d for d in body["decisions"] if d["code"] == "call_cap_reached"]
    assert cap[0]["details"] == {"uncompared": sorted([USDC, REAL_WBTC]), "max_calls": 1}


def test_explorer_failure_keeps_computed_result_with_upstream_error(client, db, monkeypatch):
    token(db, USDC, FRIEND, CENTRAL, 5_000_000, "USDC", 6)

    def broken(url):
        raise ConnectionError("https://api.etherscan.io/v2/api?apikey=SECRETKEY down")

    monkeypatch.setattr(api_client, "api_get", broken)
    response = client.get("/balances", params={"address": CENTRAL, "onchain": "true"})
    assert response.status_code == 200
    body = response.json()
    assert by_contract(body["tokens"])[USDC]["net"] == "5"
    assert "upstream_error" in codes(body) and "SECRETKEY" not in response.text


@pytest.mark.parametrize("value", ["0", "-1", "abc"])
def test_invalid_max_calls_is_rejected(client, value):
    response = client.get("/balances", params={"address": CENTRAL, "onchain": "true", "max_calls": value})
    assert response.status_code == 400


def test_address_is_required(client):
    assert client.get("/balances").status_code == 400


def test_existing_explorer_url_is_unchanged():
    assert api_client.explorer_url(1, module="account", action="balance") == \
        "https://api.etherscan.io/v2/api?chainid=1&module=account&action=balance"
    assert balances.DEFAULT_MAX_CALLS >= 1


def test_max_calls_above_ceiling_is_clamped_and_reported(client, db, monkeypatch):
    for i in range(3):
        token(db, f"0x{i:040x}", FRIEND, CENTRAL, 1, f"T{i}")
    calls = []
    monkeypatch.setattr(balances, "MAX_CALLS_CEILING", 2)
    monkeypatch.setattr(api_client, "api_get", fake_chain({None: 0, **{f"0x{i:040x}": 1 for i in range(3)}}, calls))
    body = client.get("/balances", params={"address": CENTRAL, "onchain": "true", "max_calls": "100000"}).json()
    assert len(calls) == 2 and body["onchain"]["max_calls"] == 2
    cap = [d for d in body["decisions"] if d["code"] == "call_cap_reached"]
    assert cap[0]["details"]["max_calls"] == 2


def test_max_calls_within_ceiling_is_applied_as_given(client, db, monkeypatch):
    token(db, USDC, FRIEND, CENTRAL, 5_000_000, "USDC", 6)
    calls = []
    monkeypatch.setattr(api_client, "api_get", fake_chain({None: 0, USDC: 5_000_000}, calls))
    body = client.get("/balances", params={"address": CENTRAL, "onchain": "true", "max_calls": "5"}).json()
    assert len(calls) == 2 and body["onchain"]["max_calls"] == 5
    assert balances.MAX_CALLS_CEILING == 200
