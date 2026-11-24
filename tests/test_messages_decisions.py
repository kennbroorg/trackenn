"""odd/tasks/decision-evidence-retrofit.md, T3 S4/S6: /messages keeps returning the same
list of messages, but when it skips non-readable transactions it leaves one summary decision
behind, and /decisions lets the UI read it back."""
import sqlite3
from contextlib import closing

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app as app_module
from core import decisions, onchain_messages

CENTRAL = "0xa0000000000000000000000000000000000000aa"
OTHER = "0xb0000000000000000000000000000000000000bb"

MESSAGE_INPUT = "0x" + "48656c6c6f21".ljust(64, "0")  # "Hello!"
CONTRACT_CALL_INPUT = (
    "0xa9059cbb000000000000000000000000b66cd966670d962c227b3eaba30a872dbfb995db"
    "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
)

CONFIG = {"ethscan": "K1", "bscscan": "K2", "level": "INFO"}
NON_MESSAGE_CODE = "non_message_skipped"


@pytest.fixture(autouse=True)
def allow_non_message_skipped_code(monkeypatch):
    # INFO: core/decisions.py CODES is owned by another worker; the real entry is added there.
    monkeypatch.setattr(decisions, "CODES", {**decisions.CODES, NON_MESSAGE_CODE: "x"})


@pytest.fixture
def client(db):
    app_module.app.state.config = {**CONFIG, "dbname": str(db)}
    with TestClient(app_module.app) as test_client:
        yield test_client


def insert_transaction(path, **values):
    with closing(sqlite3.connect(path)) as conn:
        columns = [row[0] for row in conn.execute("SELECT name FROM pragma_table_info('t_transactions')")]
        row = {column: values.get(column, 0) for column in columns}
        pd.DataFrame([row]).to_sql("t_transactions", conn, if_exists="append", index=False)
        conn.commit()


def seed_message_and_contract_call(db, address=CENTRAL):
    insert_transaction(db, blockChain="eth", hash="0xmessage", blockNumber=1, timeStamp=100,
                       **{"from": address, "to": OTHER}, input=MESSAGE_INPUT)
    insert_transaction(db, blockChain="eth", hash="0xcontractcall", blockNumber=2, timeStamp=101,
                       **{"from": address, "to": OTHER}, input=CONTRACT_CALL_INPUT)


def seed_message_only(db, address=CENTRAL):
    insert_transaction(db, blockChain="eth", hash="0xmessage", blockNumber=1, timeStamp=100,
                       **{"from": address, "to": OTHER}, input=MESSAGE_INPUT)


@pytest.fixture
def locked(db):
    """Hold an exclusive lock, like a collection write in progress."""
    connection = sqlite3.connect(db, timeout=0)
    connection.execute("BEGIN EXCLUSIVE")
    yield db
    connection.rollback()
    connection.close()


@pytest.fixture
def no_wait(monkeypatch):
    real_connect = sqlite3.connect
    monkeypatch.setattr(sqlite3, "connect", lambda *a, **k: real_connect(*a, **{**k, "timeout": 0}))


def read_decisions(db):
    with closing(sqlite3.connect(db)) as conn:
        try:
            return conn.execute(
                "SELECT endpoint, query_address, code, scope, rule, reason, details FROM t_decisions"
            ).fetchall()
        except sqlite3.OperationalError:
            return []


# ---- /messages body unchanged ----

def test_messages_body_is_unchanged_with_skipped_transactions(client, db):
    seed_message_and_contract_call(db)

    response = client.get("/messages", params={"address": CENTRAL})

    assert response.status_code == 200
    body = response.json()
    assert [message["hash"] for message in body] == ["0xmessage"]
    assert body[0]["text"] == "Hello!"


def test_messages_body_unchanged_when_nothing_skipped(client, db):
    seed_message_only(db)

    response = client.get("/messages", params={"address": CENTRAL})

    assert response.status_code == 200
    assert [message["hash"] for message in response.json()] == ["0xmessage"]


# ---- summary decision ----

def test_messages_with_skipped_transactions_persists_one_summary_decision(client, db):
    seed_message_and_contract_call(db)

    client.get("/messages", params={"address": CENTRAL})

    rows = read_decisions(db)
    assert len(rows) == 1
    endpoint, query_address, code, scope, rule, reason, details = rows[0]
    assert endpoint == "/messages"
    assert query_address == CENTRAL
    assert code == NON_MESSAGE_CODE
    assert scope == CENTRAL
    assert str(onchain_messages.MIN_MESSAGE_LENGTH) in rule
    assert str(onchain_messages.MIN_PRINTABLE_RATIO) in rule
    assert reason == "1 transactions were not shown because their input is not readable text"
    import json
    assert json.loads(details) == {"skipped": 1, "shown": 1}


def test_messages_with_nothing_skipped_persists_no_decision(client, db):
    seed_message_only(db)

    client.get("/messages", params={"address": CENTRAL})

    assert read_decisions(db) == []


# ---- /decisions ----

def test_decisions_endpoint_returns_the_summary_decision_parsed(client, db):
    seed_message_and_contract_call(db)
    client.get("/messages", params={"address": CENTRAL})

    response = client.get("/decisions", params={"address": CENTRAL, "network": "eth"})

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    entry = body[0]
    assert entry["code"] == NON_MESSAGE_CODE
    assert entry["endpoint"] == "/messages"
    assert entry["details"] == {"skipped": 1, "shown": 1}
    assert isinstance(entry["created_at"], int)


def test_decisions_endpoint_is_case_insensitive(client, db):
    seed_message_and_contract_call(db)
    client.get("/messages", params={"address": CENTRAL})

    response = client.get("/decisions", params={"address": CENTRAL.upper()})

    assert len(response.json()) == 1


def test_decisions_endpoint_filters_by_endpoint(client, db):
    seed_message_and_contract_call(db)
    client.get("/messages", params={"address": CENTRAL})

    response = client.get("/decisions", params={"address": CENTRAL, "endpoint": "/created-contracts"})

    assert response.json() == []


def test_decisions_endpoint_missing_address_is_400(client):
    response = client.get("/decisions")

    assert response.status_code == 400


def test_decisions_endpoint_fresh_db_is_empty_list(client):
    response = client.get("/decisions", params={"address": CENTRAL})

    assert response.status_code == 200
    assert response.json() == []


def test_decisions_endpoint_locked_db_is_503(client, locked, no_wait):
    response = client.get("/decisions", params={"address": CENTRAL})

    assert response.status_code == 503
    assert response.json()["detail"] == "Investigation database could not be read"


def test_decisions_row_with_broken_details_does_not_break_the_list(client, db):
    import sqlite3
    from contextlib import closing
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS t_decisions (created_at integer, endpoint text, "
                     "query_address text, blockChain text, code text, scope text, rule text, "
                     "reason text, details text)")
        conn.execute("INSERT INTO t_decisions VALUES (1, '/messages', ?, 'eth', 'non_message_skipped', ?, 'r', 'x', '{bad')",
                     (CENTRAL, CENTRAL))
        conn.commit()

    body = client.get("/decisions", params={"address": CENTRAL}).json()

    assert body[0]["details"] == {"unparsed": "{bad"}
