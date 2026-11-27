"""odd/tasks/backend-backlog-c.md, T13 S12 (R12): GET /calls decodes stored transaction
`input` into named calls (K1-K8), with a fake Etherscan for getabi (no network)."""
import json
import sqlite3
from contextlib import closing

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app as app_module
from core import api_client, calls

CENTRAL = "0xb2698c2d99ad2c302a95a8db26b08d17a77cedd4"
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
UNKNOWN_CONTRACT = "0x036cec1a199234fc02f72d29e596a09440825f1c"
VICTIM = "0xf1111111111111111111111111111111111111ff"
CONFIG = {"ethscan": "SECRETKEY", "bscscan": "K2", "level": "INFO"}


def word(value):
    """32-byte big-endian hex word (no 0x) for an int, or a left-padded address string."""
    if isinstance(value, str) and value.startswith("0x"):
        return value[2:].rjust(64, "0")
    return format(value & (2**256 - 1), "064x")


def calldata(selector, *words):
    return selector + "".join(words)


TRANSFER_CALLDATA = calldata("a9059cbb", word(USDC), word(1000))


def insert(path, table, **values):
    with closing(sqlite3.connect(path)) as conn:
        columns = [row[0] for row in conn.execute(f"SELECT name FROM pragma_table_info('{table}')")]
        row = {column: values.get(column, 0) for column in columns}
        row["blockChain"] = values.get("blockChain", "eth")
        pd.DataFrame([row]).to_sql(table, conn, if_exists="append", index=False)
        conn.commit()


def tx(path, h, frm, to, input_hex, method_id="", function_name="", blockchain="eth", timestamp=1):
    insert(path, "t_transactions", hash=h, **{"from": frm, "to": to}, input=input_hex,
           methodId=method_id, functionName=function_name, blockChain=blockchain, timeStamp=str(timestamp))


@pytest.fixture
def client(db):
    app_module.app.state.config = {**CONFIG, "dbname": str(db)}
    with TestClient(app_module.app) as test_client:
        yield test_client


class FakeAbiFetch:
    """table: contract (lower) -> ("verified", abi_list) | ("unverified", None) | ("error", msg)."""

    def __init__(self, table):
        self.table = table
        self.calls = []

    def __call__(self, chainid, key, contract):
        self.calls.append(contract)
        status, payload = self.table.get(contract.lower(), ("unverified", None))
        if status == "error":
            raise calls.UpstreamError(payload)
        return status == "verified", payload


USDC_ABI = [{
    "type": "function", "name": "transfer",
    "inputs": [{"type": "address", "name": "to"}, {"type": "uint256", "name": "value"}],
}]


# ---- K1: stored functionName wins (source etherscan) ----

def test_k1_stored_function_name_used_as_source_etherscan(client, db):
    tx(db, "0x1", UNKNOWN_CONTRACT, CENTRAL, "0x385ad0a4", method_id="0x385ad0a4", function_name="unwrapWETH()")
    tx(db, "0x2", UNKNOWN_CONTRACT, CENTRAL, "0x0bd868e1", method_id="0x0bd868e1", function_name="ethToDeployer()")
    tx(db, "0x3", UNKNOWN_CONTRACT, CENTRAL, "0xcb7baedc", method_id="0xcb7baedc", function_name="swapUSDC()")
    tx(db, "0x4", UNKNOWN_CONTRACT, CENTRAL, "0x3ccfd60b", method_id="0x3ccfd60b", function_name="withdraw()")

    body = client.get("/calls", params={"address": CENTRAL}).json()

    by_hash = {c["hash"]: c for c in body["calls"]}
    assert by_hash["0x1"]["function"] == "unwrapWETH()"
    assert by_hash["0x1"]["source"] == "etherscan"
    assert by_hash["0x2"]["function"] == "ethToDeployer()"
    assert by_hash["0x3"]["function"] == "swapUSDC()"
    assert by_hash["0x4"]["function"] == "withdraw()"
    assert all(c["source"] == "etherscan" for c in by_hash.values())


# ---- K2: custom selectors unknown, never invented ----

def test_k2_unknown_selector_gives_unknown_raw_params_and_one_decision(client, db):
    tx(db, "0x1", CENTRAL, UNKNOWN_CONTRACT, calldata("a91864d4", word(1), word(2)))
    tx(db, "0x2", CENTRAL, UNKNOWN_CONTRACT, calldata("a91864d4", word(3), word(4)))

    body = client.get("/calls", params={"address": CENTRAL}).json()

    for call in body["calls"]:
        assert call["function"] is None
        assert call["source"] is None
        assert all(p["type"] == "raw" for p in call["params"])
    unknown = [d for d in body["decisions"] if d["code"] == "selector_unknown"]
    assert len(unknown) == 1
    assert unknown[0]["scope"] == "0xa91864d4"
    assert unknown[0]["details"]["count"] == 2
    assert unknown[0]["details"]["hashes"] == ["0x1", "0x2"]


# ---- K3: creation and message kinds, not decoded ----

def test_k3_creation_and_message_are_not_decoded(client, db):
    tx(db, "0xcreate", CENTRAL, "", "0x6080604052")
    good_text = "Good job bro. I didn't see a hack like this for a while."
    tx(db, "0xmsg", UNKNOWN_CONTRACT, CENTRAL, "0x" + good_text.encode("utf-8").hex())

    body = client.get("/calls", params={"address": CENTRAL}).json()

    by_hash = {c["hash"]: c for c in body["calls"]}
    assert by_hash["0xcreate"]["kind"] == "creation"
    assert by_hash["0xcreate"]["function"] is None
    assert by_hash["0xmsg"]["kind"] == "message"
    assert by_hash["0xmsg"]["function"] is None


# ---- K4: verified contract via ABI, cached on second query ----

def test_k4_verified_contract_abi_decodes_and_second_query_is_cached(client, db):
    tx(db, "0x1", CENTRAL, USDC, TRANSFER_CALLDATA)
    fetch = FakeAbiFetch({USDC: ("verified", USDC_ABI)})

    first = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                            fetch_abi_fn=fetch)
    assert fetch.calls == [USDC]
    call = first["calls"][0]
    assert call["function"] == "transfer(address,uint256)"
    assert call["source"] == "abi"
    assert call["params"][0] == {"type": "address", "value": USDC}
    assert call["params"][1] == {"type": "uint256", "value": 1000}

    second_fetch = FakeAbiFetch({USDC: ("verified", USDC_ABI)})
    second = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                             fetch_abi_fn=second_fetch)
    assert second_fetch.calls == []  # served from cache, no call
    assert second["calls"][0]["function"] == "transfer(address,uint256)"
    assert any(d["code"] == "served_from_cache" for d in second["decisions"])


def test_k4_unverified_contract_is_cached_as_such(client, db):
    tx(db, "0x1", CENTRAL, UNKNOWN_CONTRACT, calldata("a91864d4", word(1)))
    fetch = FakeAbiFetch({UNKNOWN_CONTRACT: ("unverified", None)})

    calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                    fetch_abi_fn=fetch)
    second_fetch = FakeAbiFetch({UNKNOWN_CONTRACT: ("unverified", None)})
    calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                    fetch_abi_fn=second_fetch)

    assert second_fetch.calls == []  # unverified status itself is cached


# ---- K5: max_calls budget and ceiling ----

def test_k5_max_calls_one_queries_one_contract_rest_capped(client, db):
    contract_a = "0xaaaa00000000000000000000000000000000aa"
    contract_b = "0xbbbb00000000000000000000000000000000bb"
    tx(db, "0x1", CENTRAL, contract_a, calldata("a91864d4", word(1)))
    tx(db, "0x2", CENTRAL, contract_b, calldata("a91864d4", word(1)))
    fetch = FakeAbiFetch({contract_a: ("unverified", None), contract_b: ("unverified", None)})

    result = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                             max_calls=1, fetch_abi_fn=fetch)

    assert len(fetch.calls) == 1
    cap = [d for d in result["decisions"] if d["code"] == "call_cap_reached"]
    assert len(cap) == 1
    assert len(cap[0]["details"]["pending"]) == 1


def test_k5_max_calls_above_200_is_clamped_and_reported(client, db):
    tx(db, "0x1", CENTRAL, UNKNOWN_CONTRACT, calldata("a91864d4", word(1)))

    body = client.get("/calls", params={"address": CENTRAL, "max_calls": 500}).json()

    assert body["max_calls"] == 200


# ---- K6: Etherscan failure keeps resolved results + upstream_error ----

def test_k6_getabi_failure_keeps_resolved_results_and_adds_upstream_error(client, db):
    tx(db, "0x1", CENTRAL, USDC, TRANSFER_CALLDATA)
    tx(db, "0x2", UNKNOWN_CONTRACT, CENTRAL, "0x385ad0a4", method_id="0x385ad0a4", function_name="unwrapWETH()")
    fetch = FakeAbiFetch({USDC: ("error", "Max rate limit reached")})

    result = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                             fetch_abi_fn=fetch)

    by_hash = {c["hash"]: c for c in result["calls"]}
    assert by_hash["0x2"]["function"] == "unwrapWETH()"  # kept despite the other failure
    upstream = [d for d in result["decisions"] if d["code"] == "upstream_error"]
    assert len(upstream) == 1
    assert upstream[0]["reason"] == "Max rate limit reached"
    assert "SECRETKEY" not in json.dumps(result)


# ---- K7 (refined): shape filter resolves the real call despite colliding signatures ----

def test_k7_real_transfer_resolves_despite_six_colliding_signatures(client, db):
    tx(db, "0x1", CENTRAL, USDC, TRANSFER_CALLDATA)  # no functionName/ABI: forces the signatures table

    body = client.get("/calls", params={"address": CENTRAL}).json()

    call = body["calls"][0]
    assert call["function"] == "transfer(address,uint256)"
    assert call["source"] == "signatures"
    assert "selector_ambiguous" not in [d["code"] for d in body["decisions"]]


def test_k7_true_ambiguity_lists_every_fitting_candidate(client, db):
    signatures = {"0xdeadbeef": ["a(uint256,uint256)", "b(uint256,uint256)"]}
    data = calldata("deadbeef", word(1), word(2))
    tx(db, "0x1", CENTRAL, UNKNOWN_CONTRACT, data)

    result = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                             signatures=signatures)

    call = result["calls"][0]
    assert call["source"] is None
    assert sorted(call["candidates"]) == ["a(uint256,uint256)", "b(uint256,uint256)"]
    ambiguous = [d for d in result["decisions"] if d["code"] == "selector_ambiguous"]
    assert len(ambiguous) == 1
    assert sorted(ambiguous[0]["details"]["candidates"]) == ["a(uint256,uint256)", "b(uint256,uint256)"]


def test_k7_discards_candidates_whose_shape_does_not_fit(client, db):
    # static-only length mismatch: 2-arg signature against 1-word calldata never fits
    signatures = {"0xdeadbeef": ["onearg(uint256)", "twoargs(uint256,uint256)"]}
    data = calldata("deadbeef", word(1))
    tx(db, "0x1", CENTRAL, UNKNOWN_CONTRACT, data)

    result = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                             signatures=signatures)

    call = result["calls"][0]
    assert call["function"] == "onearg(uint256)"
    assert call["source"] == "signatures"


# ---- K8: no writes to investigation tables, only ABI cache + decisions ----

def test_k8_no_writes_to_investigation_tables(client, db):
    def table_counts(path):
        with closing(sqlite3.connect(path)) as conn:
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('t_decisions', 't_abi_cache')")]
            return {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables}

    tx(db, "0x1", CENTRAL, USDC, TRANSFER_CALLDATA)
    before = table_counts(db)

    client.get("/calls", params={"address": CENTRAL})

    after = table_counts(db)
    assert before == after


# ---- Missing address/hash, invalid network ----

def test_missing_address_and_hash_is_400(client):
    response = client.get("/calls")
    assert response.status_code == 400


def test_invalid_network_is_400(client):
    response = client.get("/calls", params={"address": CENTRAL, "network": "xrp"})
    assert response.status_code == 400


def test_hash_query_resolves_a_single_transaction(client, db):
    tx(db, "0xsingle", UNKNOWN_CONTRACT, CENTRAL, "0x385ad0a4", method_id="0x385ad0a4", function_name="unwrapWETH()")

    body = client.get("/calls", params={"hash": "0xsingle"}).json()

    assert len(body["calls"]) == 1
    assert body["calls"][0]["function"] == "unwrapWETH()"


# ---- Decoder unit tests ----

def test_decode_static_address():
    raw_word = bytes.fromhex(word(USDC))
    assert calls.decode_static("address", raw_word) == USDC


def test_decode_static_uint256():
    raw_word = bytes.fromhex(word(123456))
    assert calls.decode_static("uint256", raw_word) == 123456


def test_decode_static_negative_int():
    raw_word = (-5 & (2**256 - 1)).to_bytes(32, "big")
    assert calls.decode_static("int256", raw_word) == -5


def test_decode_static_bool_true_and_false():
    assert calls.decode_static("bool", (0).to_bytes(32, "big")) is False
    assert calls.decode_static("bool", (1).to_bytes(32, "big")) is True


def test_decode_static_bytes32():
    raw = bytes(range(32))
    assert calls.decode_static("bytes32", raw) == "0x" + raw.hex()


def test_dynamic_type_returned_raw_with_value_undecoded():
    data = bytes.fromhex(word(32)) + bytes.fromhex(word(0))  # offset head word, then length 0
    params = calls.decode_params(data, ["string"])
    assert params[0]["value_undecoded"] is True
    assert params[0]["value"] == "0x" + word(32)


# ---- Keccak-256 vendored implementation ----

def test_keccak256_known_vectors():
    assert calls.keccak256(b"").hex() == "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
    assert calls.keccak256(b"abc").hex() == "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45"


def test_keccak256_transfer_selector_matches_the_real_one():
    selector = calls.keccak256(b"transfer(address,uint256)")[:4].hex()
    assert selector == "a9059cbb"
