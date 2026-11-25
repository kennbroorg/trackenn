"""odd/tasks/backend-backlog-c.md, T2 S2 (R21): /messages group=true, pagination,
and the index-backed query (C1-C5)."""
import sqlite3
from contextlib import closing

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app as app_module
from core import misc

SENDER = "0xa1111111111111111111111111111111111111aa"
CENTRAL = "0xc2222222222222222222222222222222222222cc"
OTHER_SENDER = "0xb3333333333333333333333333333333333333bb"

MESSAGE_INPUT = "0x" + "48656c6c6f21".ljust(64, "0")  # "Hello!"
OTHER_MESSAGE_INPUT = "0x" + "486579746865726521".ljust(64, "0")  # "Heythere!"

CONFIG = {"ethscan": "K1", "bscscan": "K2", "level": "INFO"}


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


def seed_repeated_messages(db, sender=SENDER, to=CENTRAL, count=10, input_hex=MESSAGE_INPUT):
    for i in range(count):
        insert_transaction(db, blockChain="eth", hash=f"0xhash{i}", blockNumber=i, timeStamp=100 + i,
                            **{"from": sender, "to": to}, input=input_hex)


# ---- C1: repeated messages collapse into one group with count ----

def test_group_true_collapses_repeated_messages_into_one_group_with_count(client, db):
    seed_repeated_messages(db)

    response = client.get("/messages", params={"address": CENTRAL, "group": "true"})

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    group = body[0]
    assert group["from"] == SENDER
    assert group["to"] == CENTRAL
    assert group["text"] == "Hello!"
    assert group["count"] == 10
    assert len(group["hashes"]) == 10
    assert group["hashes"] == [f"0xhash{i}" for i in range(10)]
    assert group["first_timeStamp"] == 100
    assert group["last_timeStamp"] == 109


# ---- C2: different directions with the same text are two groups, from/to preserved ----

def test_group_true_keeps_different_directions_as_separate_groups(client, db):
    insert_transaction(db, blockChain="eth", hash="0xa", blockNumber=1, timeStamp=100,
                        **{"from": SENDER, "to": CENTRAL}, input=MESSAGE_INPUT)
    insert_transaction(db, blockChain="eth", hash="0xb", blockNumber=2, timeStamp=101,
                        **{"from": CENTRAL, "to": SENDER}, input=MESSAGE_INPUT)

    response = client.get("/messages", params={"address": CENTRAL, "group": "true"})

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    directions = {(group["from"], group["to"]) for group in body}
    assert directions == {(SENDER, CENTRAL), (CENTRAL, SENDER)}


def test_group_true_mixed_messages_keep_their_own_from_to(client, db):
    insert_transaction(db, blockChain="eth", hash="0xa", blockNumber=1, timeStamp=100,
                        **{"from": SENDER, "to": CENTRAL}, input=MESSAGE_INPUT)
    insert_transaction(db, blockChain="eth", hash="0xb", blockNumber=2, timeStamp=101,
                        **{"from": OTHER_SENDER, "to": CENTRAL}, input=OTHER_MESSAGE_INPUT)

    response = client.get("/messages", params={"address": CENTRAL, "group": "true"})

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    by_from = {group["from"]: group for group in body}
    assert by_from[SENDER]["text"] == "Hello!"
    assert by_from[OTHER_SENDER]["text"] == "Heythere!"


# ---- C3: sum of counts == ungrouped length ----

def test_group_true_sum_of_counts_equals_ungrouped_length(client, db):
    insert_transaction(db, blockChain="eth", hash="0xa", blockNumber=1, timeStamp=100,
                        **{"from": SENDER, "to": CENTRAL}, input=MESSAGE_INPUT)
    insert_transaction(db, blockChain="eth", hash="0xb", blockNumber=2, timeStamp=101,
                        **{"from": SENDER, "to": CENTRAL}, input=MESSAGE_INPUT)
    insert_transaction(db, blockChain="eth", hash="0xc", blockNumber=3, timeStamp=102,
                        **{"from": OTHER_SENDER, "to": CENTRAL}, input=OTHER_MESSAGE_INPUT)

    ungrouped = client.get("/messages", params={"address": CENTRAL}).json()
    grouped = client.get("/messages", params={"address": CENTRAL, "group": "true"}).json()

    assert sum(group["count"] for group in grouped) == len(ungrouped) == 3


# ---- C4: query plan uses both expression indexes, no full table scan ----

def test_messages_query_plan_uses_the_lower_indexes(db):
    seed_repeated_messages(db)
    address = CENTRAL.lower()
    with closing(sqlite3.connect(db)) as conn:
        plan_rows = conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT hash, blockNumber, timeStamp, `from`, `to`, input FROM t_transactions "
            "WHERE blockChain = ? AND lower(`from`) = ? "
            "UNION "
            "SELECT hash, blockNumber, timeStamp, `from`, `to`, input FROM t_transactions "
            "WHERE blockChain = ? AND lower(`to`) = ?",
            ("eth", address, "eth", address),
        ).fetchall()
    plan_text = " | ".join(str(row) for row in plan_rows)
    assert "idx_transactions_from_lower" in plan_text
    assert "idx_transactions_to_lower" in plan_text
    assert "SCAN t_transactions" not in plan_text


def test_ensure_message_indexes_is_idempotent_on_an_old_db(tmp_path):
    path = tmp_path / "old.db"
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(
            "CREATE TABLE t_transactions ("
            "blockChain text NOT NULL, blockNumber integer NOT NULL, timeStamp datetime NOT NULL, "
            "hash text NOT NULL, `from` text NOT NULL, `to` text NOT NULL, input text NOT NULL)"
        )
        conn.commit()

    with closing(sqlite3.connect(path)) as conn:
        misc.ensure_message_indexes(conn)
        misc.ensure_message_indexes(conn)
        names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}

    assert "idx_transactions_from_lower" in names
    assert "idx_transactions_to_lower" in names


# ---- C5: byte-identical plain list without group/limit/offset ----

def test_messages_without_params_is_the_plain_list_unchanged(client, db):
    seed_repeated_messages(db, count=3)

    response = client.get("/messages", params={"address": CENTRAL})

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, list)
    assert len(body) == 3
    assert "total" not in (body[0] if body else {})


# ---- limit/offset pagination ----

def test_limit_offset_envelope_without_group(client, db):
    seed_repeated_messages(db, count=5)

    response = client.get("/messages", params={"address": CENTRAL, "limit": 2, "offset": 1})

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 5
    assert body["limit"] == 2
    assert body["offset"] == 1
    assert len(body["items"]) == 2
    assert body["items"][0]["hash"] == "0xhash1"
    assert body["items"][1]["hash"] == "0xhash2"


def test_limit_offset_envelope_with_group_counts_groups(client, db):
    seed_repeated_messages(db, count=5)
    insert_transaction(db, blockChain="eth", hash="0xother", blockNumber=99, timeStamp=200,
                        **{"from": OTHER_SENDER, "to": CENTRAL}, input=OTHER_MESSAGE_INPUT)

    response = client.get("/messages", params={"address": CENTRAL, "group": "true", "limit": 1})

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert body["limit"] == 1
    assert body["offset"] == 0
    assert len(body["items"]) == 1
    assert body["items"][0]["count"] == 5


def test_limit_alone_defaults_offset_to_zero(client, db):
    seed_repeated_messages(db, count=3)

    response = client.get("/messages", params={"address": CENTRAL, "limit": 2})

    assert response.status_code == 200
    body = response.json()
    assert body["offset"] == 0
    assert len(body["items"]) == 2


def test_offset_alone_triggers_the_envelope(client, db):
    seed_repeated_messages(db, count=3)

    response = client.get("/messages", params={"address": CENTRAL, "offset": 1})

    assert response.status_code == 200
    body = response.json()
    assert body["limit"] is None
    assert body["total"] == 3
    assert len(body["items"]) == 2


@pytest.mark.parametrize("params", [
    {"limit": "0"},
    {"limit": "-1"},
    {"limit": "abc"},
    {"limit": "1001"},
    {"offset": "-1"},
    {"offset": "abc"},
])
def test_invalid_limit_or_offset_is_400(client, db, params):
    seed_repeated_messages(db, count=1)

    response = client.get("/messages", params={"address": CENTRAL, **params})

    assert response.status_code == 400
    assert "detail" in response.json()
