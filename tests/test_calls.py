"""odd/tasks/backend-backlog-c.md, T13 S12 (R12): GET /calls decodes stored transaction
`input` into named calls (K1-K8), with a fake Etherscan for getabi (no network)."""
import json
import os
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


def tx_with_null_input(path, h, frm, to, blockchain="eth", timestamp=1):
    """R3-004: a stored row can have NULL input (schema allows it in practice, even though
    t_transactions declares `input text NOT NULL`: relax the constraint to reproduce it)."""
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("PRAGMA writable_schema=1")
        conn.execute("UPDATE sqlite_master SET sql = REPLACE(sql, 'input text NOT NULL', 'input text') "
                     "WHERE name='t_transactions'")
        conn.commit()
        conn.execute("PRAGMA writable_schema=0")
    with closing(sqlite3.connect(path)) as conn:
        columns = [row[0] for row in conn.execute("SELECT name FROM pragma_table_info('t_transactions')")]
        values: dict = {column: 0 for column in columns}
        values["hash"], values["from"], values["to"] = h, frm, to
        values["blockChain"], values["timeStamp"] = blockchain, str(timestamp)
        values["methodId"], values["functionName"], values["input"] = "", "", None
        colnames = ", ".join(f"`{c}`" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        conn.execute(f"INSERT INTO t_transactions ({colnames}) VALUES ({placeholders})",
                     [values[c] for c in columns])
        conn.commit()


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


# ---- R3-001 (review-cddac27874c93070): Etherscan's functionName carries parameter names ----

def test_r3_001_named_param_function_name_decodes_by_stripped_types(client, db):
    tx(db, "0x1", CENTRAL, USDC, TRANSFER_CALLDATA,
       function_name="transfer(address _to, uint256 _value)")

    body = client.get("/calls", params={"address": CENTRAL}).json()

    call = body["calls"][0]
    assert call["source"] == "etherscan"
    assert call["function"] == "transfer(address _to, uint256 _value)"
    assert call["params"][0] == {"type": "address", "value": USDC}
    assert call["params"][1] == {"type": "uint256", "value": 1000}


# 0x42842e0e has a single unambiguous pinned signature, unlike 0xa9059cbb (see R2-specificity below)
SAFE_TRANSFER_FROM_CALLDATA = calldata("42842e0e", word(CENTRAL), word(USDC), word(1))


def test_r3_001_malformed_function_name_falls_back_instead_of_500(client, db):
    tx(db, "0x1", CENTRAL, UNKNOWN_CONTRACT, SAFE_TRANSFER_FROM_CALLDATA, function_name="not a signature")

    response = client.get("/calls", params={"address": CENTRAL})

    assert response.status_code == 200
    call = response.json()["calls"][0]
    assert call["source"] == "signatures"
    assert call["function"] == "safeTransferFrom(address,address,uint256)"


def test_r3_001_function_name_missing_closing_paren_falls_back(client, db):
    tx(db, "0x1", CENTRAL, UNKNOWN_CONTRACT, SAFE_TRANSFER_FROM_CALLDATA,
       function_name="safeTransferFrom(address,address,uint256")

    response = client.get("/calls", params={"address": CENTRAL})

    assert response.status_code == 200
    call = response.json()["calls"][0]
    assert call["source"] == "signatures"
    assert call["function"] == "safeTransferFrom(address,address,uint256)"


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


# ---- R3-004 (review-cddac27874c93070): a NULL/empty stored input is not a call, never a 500 ----

def test_null_input_row_is_skipped_not_a_500(client, db):
    tx_with_null_input(db, "0xnullinput", CENTRAL, UNKNOWN_CONTRACT)
    tx(db, "0xok", CENTRAL, UNKNOWN_CONTRACT, calldata("a91864d4", word(1)))

    response = client.get("/calls", params={"address": CENTRAL})

    assert response.status_code == 200
    hashes = [c["hash"] for c in response.json()["calls"]]
    assert "0xnullinput" not in hashes
    assert "0xok" in hashes


def test_empty_input_row_is_skipped_not_a_500(client, db):
    tx(db, "0xemptyinput", CENTRAL, UNKNOWN_CONTRACT, "")

    response = client.get("/calls", params={"address": CENTRAL})

    assert response.status_code == 200
    assert response.json()["calls"] == []


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


# ---- S13(a): unverified ABI cache entries expire after 7 days; verified never expires ----

DAY = 24 * 3600


def test_s13a_unverified_cache_entry_older_than_7_days_is_refetched(client, db):
    tx(db, "0x1", CENTRAL, UNKNOWN_CONTRACT, calldata("a91864d4", word(1)))
    t0 = [1_700_000_000]
    fetch = FakeAbiFetch({UNKNOWN_CONTRACT: ("unverified", None)})

    calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                    fetch_abi_fn=fetch, now=lambda: t0[0])
    assert fetch.calls == [UNKNOWN_CONTRACT]

    # within the 7-day window: still cached, no call
    t0[0] += 6 * DAY
    still_fresh = FakeAbiFetch({UNKNOWN_CONTRACT: ("unverified", None)})
    calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                    fetch_abi_fn=still_fresh, now=lambda: t0[0])
    assert still_fresh.calls == []

    # past 7 days: re-fetched, now reporting verified (the contract was verified later)
    t0[0] += 2 * DAY
    now_verified = FakeAbiFetch({UNKNOWN_CONTRACT: ("verified", USDC_ABI)})
    result = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                             fetch_abi_fn=now_verified, now=lambda: t0[0])
    assert now_verified.calls == [UNKNOWN_CONTRACT]
    assert result["calls"][0]["source"] is None  # calldata doesn't match transfer, stays unresolved by name


def test_s13a_verified_cache_entry_never_expires(client, db):
    tx(db, "0x1", CENTRAL, USDC, TRANSFER_CALLDATA)
    t0 = [1_700_000_000]
    fetch = FakeAbiFetch({USDC: ("verified", USDC_ABI)})

    calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                    fetch_abi_fn=fetch, now=lambda: t0[0])
    assert fetch.calls == [USDC]

    t0[0] += 3650 * DAY  # far beyond any TTL
    much_later = FakeAbiFetch({USDC: ("verified", USDC_ABI)})
    result = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                             fetch_abi_fn=much_later, now=lambda: t0[0])
    assert much_later.calls == []  # never expires once verified
    assert result["calls"][0]["source"] == "abi"


def test_s13a_migrates_an_existing_cache_table_without_the_status_column(client, db):
    # An ABI cache table created before S13(a) added `status`: no column, old rows are
    # either verified (never expire) or unverified (treated as already past their TTL).
    with closing(sqlite3.connect(db)) as conn:
        conn.execute(
            "CREATE TABLE t_abi_cache (blockChain text, address text, verified integer, "
            "abi text, fetched_at integer, UNIQUE(blockChain, address))")
        conn.execute("INSERT INTO t_abi_cache VALUES ('eth', ?, 1, ?, 1)",
                     (USDC, json.dumps(USDC_ABI)))
        conn.execute("INSERT INTO t_abi_cache VALUES ('eth', ?, 0, NULL, 1)",
                     (UNKNOWN_CONTRACT,))
        conn.commit()
    tx(db, "0x1", CENTRAL, USDC, TRANSFER_CALLDATA)
    tx(db, "0x2", CENTRAL, UNKNOWN_CONTRACT, calldata("a91864d4", word(1)))
    fetch = FakeAbiFetch({UNKNOWN_CONTRACT: ("unverified", None)})

    result = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                             fetch_abi_fn=fetch)

    by_hash = {c["hash"]: c for c in result["calls"]}
    assert by_hash["0x1"]["source"] == "abi"  # old verified row still served from cache
    assert fetch.calls == [UNKNOWN_CONTRACT]  # old unverified row, no fetched_at, treated as expired


# ---- S13(b): a failed getabi is cached as "failed" for 10 minutes ----

def test_s13b_failed_getabi_is_cached_for_ten_minutes_then_retried(client, db):
    tx(db, "0x1", CENTRAL, USDC, TRANSFER_CALLDATA)
    t0 = [1_700_000_000]
    failing = FakeAbiFetch({USDC: ("error", "Max rate limit reached")})

    first = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                            fetch_abi_fn=failing, now=lambda: t0[0])
    assert failing.calls == [USDC]
    assert "upstream_error" in [d["code"] for d in first["decisions"]]

    # within 10 minutes: served from the failed cache, no call spent
    t0[0] += 5 * 60
    still_failing = FakeAbiFetch({USDC: ("error", "Max rate limit reached")})
    second = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                             fetch_abi_fn=still_failing, now=lambda: t0[0])
    assert still_failing.calls == []
    upstream = [d for d in second["decisions"] if d["code"] == "upstream_error"]
    assert len(upstream) == 1
    assert upstream[0]["details"]["skipped_recent_failure"] == [USDC]

    # past 10 minutes: retried
    t0[0] += 6 * 60
    recovered = FakeAbiFetch({USDC: ("verified", USDC_ABI)})
    third = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                            fetch_abi_fn=recovered, now=lambda: t0[0])
    assert recovered.calls == [USDC]
    assert third["calls"][0]["source"] == "abi"


# ---- S13(c): cache_unavailable on /calls (read-only / corrupt t_abi_cache) ----

@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file permissions")
def test_s13c_read_only_db_file_gives_cache_unavailable_and_still_200(client, db):
    tx(db, "0x1", CENTRAL, UNKNOWN_CONTRACT, calldata("a91864d4", word(1)))
    os.chmod(db, 0o444)
    try:
        response = client.get("/calls", params={"address": CENTRAL})
    finally:
        os.chmod(db, 0o644)

    assert response.status_code == 200
    body = response.json()
    assert "cache_unavailable" in [d["code"] for d in body["decisions"]]


def test_s13c_corrupt_abi_cache_table_gives_cache_unavailable_and_still_200(client, db):
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("CREATE TABLE t_abi_cache (just_one_column text)")
        conn.commit()
    tx(db, "0x1", CENTRAL, UNKNOWN_CONTRACT, calldata("a91864d4", word(1)))

    response = client.get("/calls", params={"address": CENTRAL})

    assert response.status_code == 200
    body = response.json()
    assert "cache_unavailable" in [d["code"] for d in body["decisions"]]


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


# ---- R1-002/K6 (review-cddac27874c93070): getabi transport failure, never a 500, key never leaked ----

def test_r1_002_getabi_transport_exception_keeps_results_and_redacts_key(client, db, monkeypatch):
    tx(db, "0x1", CENTRAL, USDC, TRANSFER_CALLDATA)
    tx(db, "0x2", UNKNOWN_CONTRACT, CENTRAL, "0x385ad0a4", method_id="0x385ad0a4", function_name="unwrapWETH()")

    def broken(url):
        raise ConnectionError("https://api.etherscan.io/v2/api?apikey=SECRETKEY down")

    monkeypatch.setattr(api_client, "api_get", broken)

    response = client.get("/calls", params={"address": CENTRAL})

    assert response.status_code == 200
    body = response.json()
    by_hash = {c["hash"]: c for c in body["calls"]}
    assert by_hash["0x2"]["function"] == "unwrapWETH()"  # kept despite the other failure
    upstream = [d for d in body["decisions"] if d["code"] == "upstream_error"]
    assert len(upstream) == 1
    assert "SECRETKEY" not in response.text
    assert "SECRETKEY" not in upstream[0]["reason"]


def test_r1_002_getabi_non_json_body_keeps_results_and_adds_upstream_error(client, db, monkeypatch):
    tx(db, "0x1", CENTRAL, USDC, TRANSFER_CALLDATA)

    class FakeResponse:
        def json(self):
            raise ValueError("not JSON")

    monkeypatch.setattr(api_client, "api_get", lambda url: FakeResponse())

    response = client.get("/calls", params={"address": CENTRAL})

    assert response.status_code == 200
    assert "upstream_error" in [d["code"] for d in response.json()["decisions"]]


# ---- K7 (refined), R2-specificity (review-cddac27874c93070): shape alone, no preference tie-break ----
#
# The real transfer(address,uint256) -> 0xa9059cbb collides in data/functionSignatures.json with
# workMyDirefulOwner(uint256,uint256): an address is a valid uint256 by construction (it is just the
# low 160 bits), so any calldata that fits transfer(address,uint256) also fits two unconstrained
# uint256 params. No strengthening of shape_fits (which only inspects the calldata bytes, never
# the selector's real intent) can tell these two apart; the old specificity tie-break silently chose
# transfer anyway, contradicting K7's "none chosen" rule for true shape ties. Per K7, this case is now
# honestly selector_ambiguous (see odd/tasks/backend-backlog-c.md L43-L44, T14 report).

def test_k7_usdc_transfer_six_colliding_signatures_is_honestly_ambiguous(client, db):
    tx(db, "0x1", CENTRAL, USDC, TRANSFER_CALLDATA)  # no functionName/ABI: forces the signatures table

    body = client.get("/calls", params={"address": CENTRAL}).json()

    call = body["calls"][0]
    assert call["source"] is None
    assert sorted(call["candidates"]) == ["transfer(address,uint256)", "workMyDirefulOwner(uint256,uint256)"]
    ambiguous = [d for d in body["decisions"] if d["code"] == "selector_ambiguous"]
    assert len(ambiguous) == 1
    assert ambiguous[0]["details"]["count"] == 1


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


# ---- R2-discarded (review-cddac27874c93070): details report the real discarded count ----

def test_r2_discarded_count_in_ambiguous_decision_is_real(client, db):
    # 3 candidates share the selector; one is discarded by shape (3-arg, too long), 2 remain tied
    signatures = {"0xdeadbeef": ["a(uint256,uint256)", "b(uint256,uint256)", "c(uint256,uint256,uint256)"]}
    data = calldata("deadbeef", word(1), word(2))
    tx(db, "0x1", CENTRAL, UNKNOWN_CONTRACT, data)

    result = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                             signatures=signatures)

    ambiguous = [d for d in result["decisions"] if d["code"] == "selector_ambiguous"]
    assert len(ambiguous) == 1
    assert ambiguous[0]["details"]["discarded"] == 1
    assert sorted(ambiguous[0]["details"]["candidates"]) == ["a(uint256,uint256)", "b(uint256,uint256)"]


def test_r2_discarded_count_is_zero_when_every_candidate_fits(client, db):
    signatures = {"0xdeadbeef": ["a(uint256,uint256)", "b(uint256,uint256)"]}
    data = calldata("deadbeef", word(1), word(2))
    tx(db, "0x1", CENTRAL, UNKNOWN_CONTRACT, data)

    result = calls.get_calls(str(db), "eth", chainid=api_client.ETH_CHAIN_ID, key="K", address=CENTRAL,
                             signatures=signatures)

    ambiguous = [d for d in result["decisions"] if d["code"] == "selector_ambiguous"]
    assert ambiguous[0]["details"]["discarded"] == 0


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
