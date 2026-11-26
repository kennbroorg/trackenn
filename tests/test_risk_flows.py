"""odd/tasks/backend-backlog-c.md, T10 S10 (R5): GET /risk-flows lists the direct
counterparties of an address that are mixers or sanctioned, from stored data only."""
import json
import sqlite3
from contextlib import closing

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app as app_module

CENTRAL = "0xc2222222222222222222222222222222222222cc"
FRIEND = "0xf1111111111111111111111111111111111111ff"
POOL = "0xa160cdab225685da1d56aa342ad8841c3b53f291"       # Tornado.Cash: 100 ETH
TORN = "0x77777feddddffc19ff86db637967013e6c6a116c"       # Tornado.Cash: TORN Token
SDN = "0x0330070fd38ec3bb94f58fa55d40368271e9e54a"        # OFAC SDN
MIXER_SDN = "0x94a1b5cdb22c43faab4abeb5c74999895464ddaf"  # Tornado.Cash: Mixer 1 (mixer + sanctioned)
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

CONFIG = {"ethscan": "K", "bscscan": "K2", "level": "INFO"}


@pytest.fixture
def client(db):
    app_module.app.state.config = {**CONFIG, "dbname": str(db)}
    with TestClient(app_module.app) as test_client:
        yield test_client


def insert(path, table, **values):
    with closing(sqlite3.connect(path)) as conn:
        columns = [row[0] for row in conn.execute(f"SELECT name FROM pragma_table_info('{table}')")]
        row = {column: values.get(column, 0) for column in columns}
        row["blockChain"] = values.get("blockChain", "eth")
        pd.DataFrame([row]).to_sql(table, conn, if_exists="append", index=False)
        conn.commit()


def label(path, address, name, labels, chain="ethereum"):
    insert(path, "t_labels", blockChain=chain, source="etherscan_label", address=address,
           name=name, labels=json.dumps(labels))


def tx(path, frm, to, value, h, ts=1, table="t_transactions", is_error=0):
    insert(path, table, **{"from": frm, "to": to}, value=str(value), hash=h, timeStamp=str(ts),
           isError=str(is_error))


def labels_fixture(path):
    label(path, POOL, "Tornado.Cash: 100 ETH", ["ofac-sanctions-lists", "tornado-cash"])
    label(path, TORN, "Tornado.Cash: TORN Token", ["tornado-cash"])
    label(path, SDN, "AMNOKGANG", ["ofac-sanctions-lists", "ofac-sdn"])
    label(path, MIXER_SDN, "Tornado.Cash: Mixer 1", ["ethereum-mixer", "ofac-sanctions-lists", "tornado-cash"])


def table_counts(path):
    with closing(sqlite3.connect(path)) as conn:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                  if r[0] != "t_decisions"]
        return {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables}


def stored_decisions(path):
    with closing(sqlite3.connect(path)) as conn:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='t_decisions'").fetchone():
            return []
        return conn.execute("SELECT code, scope, details FROM t_decisions ORDER BY rowid").fetchall()


def by_key(body):
    return {(c["counterparty"], c["direction"]): c for c in body["counterparties"]}


def test_outflows_to_a_mixer_pool_are_grouped_with_amount_and_every_hash(client, db):
    labels_fixture(db)
    for i in range(11):
        tx(db, CENTRAL, POOL, 100 * 10**18, f"0xout{i}", ts=10 + i)
    tx(db, CENTRAL, POOL, 5 * 10**18, "0xfailed", ts=50, is_error=1)  # reverted: no value moved
    body = client.get("/risk-flows", params={"address": CENTRAL}).json()
    assert body["address"] == CENTRAL
    entry = by_key(body)[(POOL, "out")]
    assert entry["kinds"] == ["mixer", "sanctioned"]
    assert entry["name"] == "Tornado.Cash: 100 ETH"
    assert entry["per_token"] == [{"contract": "eth", "symbol": "ETH", "amount": "1100", "count": 11}]
    assert entry["hashes"] == [f"0xout{i}" for i in range(11)]
    assert (entry["first_timeStamp"], entry["last_timeStamp"]) == (10, 20)
    flow = [d for d in body["decisions"] if d["code"] == "flow_risky"]
    assert len(flow) == 1 and flow[0]["scope"] == POOL
    assert flow[0]["details"] == {"direction": "out", "kinds": ["mixer", "sanctioned"],
                                  "name": "Tornado.Cash: 100 ETH", "count": 11,
                                  "hashes": [f"0xout{i}" for i in range(5)]}
    assert "mixer_trail_lost" not in [d["code"] for d in body["decisions"]]


def test_inflow_from_a_mixer_leaves_trail_lost_decision_including_internals(client, db):
    labels_fixture(db)
    tx(db, MIXER_SDN, CENTRAL, 968100000000000000, "0xgas", ts=5, table="t_internals")
    body = client.get("/risk-flows", params={"address": CENTRAL}).json()
    entry = by_key(body)[(MIXER_SDN, "in")]
    assert entry["per_token"] == [{"contract": "eth", "symbol": "ETH", "amount": "0.9681", "count": 1}]
    lost = [d for d in body["decisions"] if d["code"] == "mixer_trail_lost"]
    assert len(lost) == 1 and lost[0]["scope"] == MIXER_SDN
    assert lost[0]["details"] == {"count": 1, "hashes": ["0xgas"]}


def test_sanctioned_counterparty_and_erc20_amounts(client, db):
    labels_fixture(db)
    insert(db, "t_transfers", contractAddress=USDC, **{"from": SDN, "to": CENTRAL}, value="2500000",
           tokenSymbol="USDC", tokenDecimal="6", hash="0xusdc", timeStamp="7")
    body = client.get("/risk-flows", params={"address": CENTRAL}).json()
    entry = by_key(body)[(SDN, "in")]
    assert entry["kinds"] == ["sanctioned"]
    assert entry["labels"] == ["ofac-sanctions-lists", "ofac-sdn"]
    assert entry["per_token"] == [{"contract": USDC, "symbol": "USDC", "amount": "2.5", "count": 1}]
    assert [d["code"] for d in body["decisions"]] == ["flow_risky"]  # sanctioned only: no trail lost


def test_tornado_governance_token_is_not_a_mixer(client, db):
    labels_fixture(db)
    insert(db, "t_transfers", contractAddress=TORN, **{"from": TORN, "to": CENTRAL}, value="1",
           tokenSymbol="TORN", tokenDecimal="18", hash="0xtorn", timeStamp="1")
    tx(db, TORN, CENTRAL, 1, "0xtorn2")
    body = client.get("/risk-flows", params={"address": CENTRAL}).json()
    assert body == {"address": CENTRAL, "counterparties": [], "decisions": []}


def test_clean_address_has_no_counterparties_and_no_decisions(client, db):
    labels_fixture(db)
    tx(db, FRIEND, CENTRAL, 10, "0xclean")
    body = client.get("/risk-flows", params={"address": CENTRAL.upper().replace("0X", "0x")}).json()
    assert body == {"address": CENTRAL, "counterparties": [], "decisions": []}
    assert stored_decisions(db) == []


def test_no_writes_to_investigation_tables_and_idempotent_decisions(client, db):
    labels_fixture(db)
    tx(db, CENTRAL, POOL, 100 * 10**18, "0xout")
    tx(db, MIXER_SDN, CENTRAL, 1, "0xin")
    before = table_counts(db)
    first = client.get("/risk-flows", params={"address": CENTRAL}).json()
    stored = stored_decisions(db)
    second = client.get("/risk-flows", params={"address": CENTRAL}).json()
    assert first == second
    assert table_counts(db) == before
    assert len(stored) == 3 and stored_decisions(db) == stored


def test_invalid_network_is_rejected_and_bsc_uses_binance_labels(client, db):
    response = client.get("/risk-flows", params={"address": CENTRAL, "network": "etth"})
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid 'network' value: use eth or bsc"
    label(db, SDN, "AMNOKGANG", ["ofac-sdn"], chain="binance")
    tx(db, CENTRAL, SDN, 10**18, "0xbsc")
    insert(db, "t_transactions", blockChain="bsc", **{"from": CENTRAL, "to": SDN}, value=str(10**18),
           hash="0xbsc2", timeStamp="1", isError="0")
    body = client.get("/risk-flows", params={"address": CENTRAL, "network": "bsc"}).json()
    entry = by_key(body)[(SDN, "out")]
    assert entry["hashes"] == ["0xbsc2"]
    assert entry["per_token"] == [{"contract": "eth", "symbol": "BNB", "amount": "1", "count": 1}]


def test_missing_address_is_rejected(client):
    assert client.get("/risk-flows").status_code == 400
