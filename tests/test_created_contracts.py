"""R11 creation hierarchy (odd/tasks/created-contracts.md, T1, S1-S6): pure walk logic in
core/created_contracts.py, fully offline (fetch_all_pages is injected, tests never call
Etherscan), plus the GET /created-contracts route."""
import sqlite3
from contextlib import closing

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app as app_module
from core import api_client, created_contracts

CENTRAL = "0xa0000000000000000000000000000000000000aa"
ORCHESTRATOR = "0x036cec1a199234fc02f72d29e596a09440825f1c"
VIOLATOR = "0x0b812c74729b6abc723f22986c61d95344ff7aba"
LIQUIDATOR = "0xd3b7cea28feb5e537fca4e657e3f60129456eaf3"
CHAIN_ID = api_client.ETH_CHAIN_ID
KEY = "K"


def internal_row(n, frm, to, contract_address, row_type="create"):
    return {"blockNumber": str(100 + n), "timeStamp": str(1_700_000_000 + n), "hash": f"0xi{n}",
            "from": frm, "to": to, "contractAddress": contract_address, "type": row_type}


def fetch_from(table):
    """Fake fetch_all_pages: table maps contract address (lower) -> list of internal rows."""
    calls = []

    def fetch(chainid, action, address, key):
        calls.append((action, address))
        assert action == "txlistinternal"
        return table.get(address.lower(), []), True

    return fetch, calls


# ---- S1: direct creation from stored txlist ----

def insert_transaction(path, **values):
    with closing(sqlite3.connect(path)) as conn:
        columns = [row[0] for row in conn.execute("SELECT name FROM pragma_table_info('t_transactions')")]
        row = {column: values.get(column, 0) for column in columns}
        pd.DataFrame([row]).to_sql("t_transactions", conn, if_exists="append", index=False)
        conn.commit()


def insert_internal(path, **values):
    with closing(sqlite3.connect(path)) as conn:
        columns = [row[0] for row in conn.execute("SELECT name FROM pragma_table_info('t_internals')")]
        row = {column: values.get(column, 0) for column in columns}
        pd.DataFrame([row]).to_sql("t_internals", conn, if_exists="append", index=False)
        conn.commit()


def mark_collected(path, address):
    # INFO: the collector writes a t_blocks row for every address it downloaded
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("INSERT INTO t_blocks VALUES ('eth', ?, 0, 100, 0, 0, 1)", (address,))
        conn.commit()


def test_level1_direct_creation_from_stored_txlist(db):
    insert_transaction(db, blockChain="eth", hash="0xcreate1", blockNumber=100, timeStamp=1_700_000_000,
                       **{"from": CENTRAL, "to": ""}, contractAddress=ORCHESTRATOR)

    rows = created_contracts.level1_from_db(str(db), CENTRAL, "eth")

    assert len(rows) == 1
    assert rows[0]["contractAddress"] == ORCHESTRATOR
    assert rows[0]["hash"] == "0xcreate1"


def test_level1_internal_create_from_stored_internals(db):
    insert_internal(db, blockChain="eth", hash="0xi1", blockNumber=100, timeStamp=1_700_000_000,
                    **{"from": CENTRAL, "to": ""}, contractAddress=VIOLATOR, type="create2")

    rows = created_contracts.level1_from_db(str(db), CENTRAL, "eth")

    assert len(rows) == 1
    assert rows[0]["contractAddress"] == VIOLATOR
    assert rows[0]["type"] == "create2"


def test_eoa_with_no_creations_returns_empty_list(db):
    insert_transaction(db, blockChain="eth", hash="0xnotacreate", blockNumber=100, timeStamp=1_700_000_000,
                       **{"from": CENTRAL, "to": "0xsomeone"}, contractAddress="")
    mark_collected(db, CENTRAL)

    result = created_contracts.get_created_contracts(str(db), CENTRAL, "eth", CHAIN_ID, KEY)
    contracts, truncated, limit = result['contracts'], result['truncated'], result['limit']

    assert contracts == []
    assert truncated is False
    assert limit is None


# ---- S1/S3: recursive walk, dedup, retried CREATE ----

def test_internal_creates_at_levels_2_and_3():
    level1 = [{"hash": "0xcreate1", "timeStamp": "1", "contractAddress": ORCHESTRATOR, "type": "create"}]
    fetch, calls = fetch_from({
        ORCHESTRATOR.lower(): [internal_row(1, ORCHESTRATOR, "", VIOLATOR)],
        VIOLATOR.lower(): [internal_row(2, VIOLATOR, "", LIQUIDATOR)],
    })

    result = created_contracts.find_created_contracts(CENTRAL, level1, CHAIN_ID, KEY, fetch)
    contracts, truncated, limit = result['contracts'], result['truncated'], result['limit']

    by_address = {c["address"]: c for c in contracts}
    assert by_address[ORCHESTRATOR]["level"] == 1
    assert by_address[VIOLATOR]["level"] == 2
    assert by_address[LIQUIDATOR]["level"] == 3
    assert by_address[LIQUIDATOR]["creator"] == VIOLATOR
    assert truncated is False
    assert limit is None


def test_retried_create_at_same_address_yields_one_contract_two_creations():
    level1 = [{"hash": "0xcreate1", "timeStamp": "1", "contractAddress": ORCHESTRATOR, "type": "create"}]
    fetch, calls = fetch_from({
        ORCHESTRATOR.lower(): [
            internal_row(1, ORCHESTRATOR, "", VIOLATOR),
            internal_row(2, ORCHESTRATOR, "", VIOLATOR),
        ],
    })

    result = created_contracts.find_created_contracts(CENTRAL, level1, CHAIN_ID, KEY, fetch)
    contracts = result['contracts']

    by_address = {c["address"]: c for c in contracts}
    assert len(contracts) == 2  # orchestrator + violator, not duplicated
    assert len(by_address[VIOLATOR]["creations"]) == 2
    assert {c["hash"] for c in by_address[VIOLATOR]["creations"]} == {"0xi1", "0xi2"}


def test_contract_seen_twice_is_expanded_once():
    # Two different level-1 contracts both "create" the same child address (cycle/dup safety)
    level1 = [
        {"hash": "0xc1", "timeStamp": "1", "contractAddress": "0xaaa", "type": "create"},
        {"hash": "0xc2", "timeStamp": "2", "contractAddress": "0xbbb", "type": "create"},
    ]
    fetch, calls = fetch_from({
        "0xaaa": [internal_row(1, "0xaaa", "", "0xccc")],
        "0xbbb": [internal_row(2, "0xbbb", "", "0xccc")],
        "0xccc": [internal_row(3, "0xccc", "", "0xddd")],
    })

    result = created_contracts.find_created_contracts(CENTRAL, level1, CHAIN_ID, KEY, fetch)
    contracts = result['contracts']

    expansions_of_ccc = [action for action, address in calls if address.lower() == "0xccc"]
    assert len(expansions_of_ccc) == 1
    by_address = {c["address"]: c for c in contracts}
    assert len(by_address["0xccc"]["creations"]) == 2


# ---- S2: limits ----

def test_depth_limit_cuts_and_reports_truncated():
    chain = {}
    for level in range(1, created_contracts.MAX_DEPTH + 3):
        addr = f"0xlevel{level}"
        child = f"0xlevel{level + 1}"
        chain[addr] = [internal_row(level, addr, "", child)]
    level1 = [{"hash": "0xc0", "timeStamp": "0", "contractAddress": "0xlevel1", "type": "create"}]
    fetch, calls = fetch_from(chain)

    result = created_contracts.find_created_contracts(CENTRAL, level1, CHAIN_ID, KEY, fetch)
    contracts, truncated, limit = result['contracts'], result['truncated'], result['limit']

    assert truncated is True
    assert limit == "depth"
    levels_found = {c["level"] for c in contracts}
    assert max(levels_found) == created_contracts.MAX_DEPTH


def test_total_contracts_limit_cuts_and_makes_no_further_fetch_calls():
    # One contract per level-1 creation, each with a child: enough width to exceed MAX_CONTRACTS
    level1 = [
        {"hash": f"0xc{i}", "timeStamp": str(i), "contractAddress": f"0xwide{i}", "type": "create"}
        for i in range(created_contracts.MAX_CONTRACTS + 5)
    ]
    chain = {f"0xwide{i}": [internal_row(i, f"0xwide{i}", "", f"0xchild{i}")]
             for i in range(created_contracts.MAX_CONTRACTS + 5)}
    fetch, calls = fetch_from(chain)

    result = created_contracts.find_created_contracts(CENTRAL, level1, CHAIN_ID, KEY, fetch)
    contracts, truncated, limit = result['contracts'], result['truncated'], result['limit']

    assert truncated is True
    assert limit == "contracts"
    assert len(contracts) <= created_contracts.MAX_CONTRACTS
    calls_before = len(calls)
    assert calls_before <= created_contracts.MAX_CONTRACTS


# ---- endpoint (S4) ----

CONFIG = {"ethscan": "K1", "bscscan": "K2", "level": "INFO"}


@pytest.fixture
def client(db):
    app_module.app.state.config = {**CONFIG, "dbname": str(db)}
    with TestClient(app_module.app) as test_client:
        yield test_client


def dump_transactions(db):
    with closing(sqlite3.connect(db)) as conn:
        return sorted(conn.execute("SELECT hash FROM t_transactions").fetchall())


def test_endpoint_missing_address_is_400(client):
    response = client.get("/created-contracts")

    assert response.status_code == 400


def test_endpoint_response_shape(client, db, monkeypatch):
    insert_transaction(db, blockChain="eth", hash="0xcreate1", blockNumber=100, timeStamp=1_700_000_000,
                       **{"from": CENTRAL, "to": ""}, contractAddress=ORCHESTRATOR)
    mark_collected(db, CENTRAL)
    monkeypatch.setattr(api_client, "fetch_all_pages", lambda *a, **k: ([], True))

    response = client.get("/created-contracts", params={"address": CENTRAL})

    assert response.status_code == 200
    body = response.json()
    assert body["address"] == CENTRAL
    assert body["truncated"] is False
    assert body["limit"] is None
    assert body["contracts"][0]["address"] == ORCHESTRATOR
    assert body["contracts"][0]["creator"] == CENTRAL
    assert body["contracts"][0]["level"] == 1
    assert body["contracts"][0]["creations"][0]["hash"] == "0xcreate1"


def test_endpoint_does_not_write_to_db(client, db, monkeypatch):
    insert_transaction(db, blockChain="eth", hash="0xcreate1", blockNumber=100, timeStamp=1_700_000_000,
                       **{"from": CENTRAL, "to": ""}, contractAddress=ORCHESTRATOR)
    monkeypatch.setattr(api_client, "fetch_all_pages", lambda *a, **k: ([], True))
    before = dump_transactions(db)

    client.get("/created-contracts", params={"address": CENTRAL})

    assert dump_transactions(db) == before


def test_uncollected_address_gets_level1_from_the_api(db):
    # INFO: an address never collected has nothing stored; level 1 must come from its own
    #       txlist (creation txs) and txlistinternal (create rows), not silently answer []
    calls = []

    def fetch(chainid, action, address, key):
        calls.append((action, address.lower()))
        if address.lower() == CENTRAL and action == "txlist":
            return [{"hash": "0xc1", "timeStamp": "1700000000", "from": CENTRAL, "to": "",
                     "contractAddress": ORCHESTRATOR}], True
        if address.lower() == CENTRAL and action == "txlistinternal":
            return [internal_row(2, CENTRAL, "", VIOLATOR)], True
        return [], True

    result = created_contracts.get_created_contracts(
        str(db), CENTRAL, "eth", CHAIN_ID, KEY, fetch_all_pages=fetch)
    contracts, truncated, limit = result['contracts'], result['truncated'], result['limit']

    assert sorted(c["address"] for c in contracts) == sorted([ORCHESTRATOR, VIOLATOR])
    assert all(c["level"] == 1 and c["creator"] == CENTRAL for c in contracts)
    assert ("txlist", CENTRAL) in calls and ("txlistinternal", CENTRAL) in calls
    assert (truncated, limit) == (False, None)


def test_collected_address_reads_level1_from_db_without_api_calls(db):
    insert_transaction(db, blockChain="eth", hash="0xcreate1", blockNumber=100, timeStamp=1_700_000_000,
                       **{"from": CENTRAL, "to": ""}, contractAddress=ORCHESTRATOR)
    mark_collected(db, CENTRAL)
    fetch, calls = fetch_from({})

    created_contracts.get_created_contracts(str(db), CENTRAL, "eth", CHAIN_ID, KEY, fetch_all_pages=fetch)

    assert ("txlist", CENTRAL) not in [(a, x.lower()) for a, x in calls]
    assert ("txlistinternal", CENTRAL) not in [(a, x.lower()) for a, x in calls]
