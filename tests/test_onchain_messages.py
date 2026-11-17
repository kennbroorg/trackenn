import sqlite3
from contextlib import closing

import pandas as pd
import pytest
import yaml
from fastapi.testclient import TestClient

import app as app_module
from core import api_client, onchain_messages

EULER_MESSAGE_TX = "0x539c6fff0fce70e02dddd80a5534acf3df57deafbdc40f41abb20aa8f94a6d0d"
EXPLOITER_2 = "0xb66cd966670d962c227b3eaba30a872dbfb995db"
EULER_SUPPORT = "0xee009faf00cf54c1b4387829af7a8dc5f0c8c8c5"
EULER_MESSAGE_TEXT = (
    "We understand that you are responsible for this morning's attack on the Euler "
    "platform. We are writing to see whether you would be open to speaking with us "
    "about any potential next steps."
)

CONTRACT_CALL_INPUT = (
    "0xa9059cbb000000000000000000000000b66cd966670d962c227b3eaba30a872dbfb995db"
    "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
)


def euler_message_input_hex():
    # INFO: Reproduce the exact bytes of tx 0x539c... from the real explorer, not a hardcoded hex
    with open("config.yaml") as file:
        config = yaml.safe_load(file)
    url = api_client.explorer_url(api_client.ETH_CHAIN_ID, module="proxy",
                                   action="eth_getTransactionByHash", txhash=EULER_MESSAGE_TX,
                                   apikey=config["ethscan"])
    response = api_client.api_get(url)
    return response.json()["result"]["input"]


# ---- S1: decoder ----

def test_decodes_readable_utf8_text():
    assert onchain_messages.decode_message("0x" + "48656c6c6f21".rstrip()) == "Hello!"


def test_euler_message_decodes_to_its_text():
    assert onchain_messages.decode_message(euler_message_input_hex()) == EULER_MESSAGE_TEXT


def test_ordinary_contract_call_is_not_a_message():
    assert onchain_messages.decode_message(CONTRACT_CALL_INPUT) is None


def test_empty_input_is_not_a_message():
    assert onchain_messages.decode_message("0x") is None


def test_invalid_utf8_is_not_a_message():
    assert onchain_messages.decode_message("0xff fe".replace(" ", "")) is None


def test_trailing_nul_bytes_are_stripped():
    hex_with_nul = "48656c6c6f21".ljust(64, "0")  # "Hello!" padded with NUL bytes
    assert onchain_messages.decode_message("0x" + hex_with_nul) == "Hello!"


def test_too_short_is_not_a_message():
    assert onchain_messages.decode_message("0x616263") is None  # "abc", 3 chars


# ---- S2/S3: query ----

def insert_transaction(path, **values):
    with closing(sqlite3.connect(path)) as conn:
        columns = [row[0] for row in conn.execute("SELECT name FROM pragma_table_info('t_transactions')")]
        row = {column: values.get(column, 0) for column in columns}
        pd.DataFrame([row]).to_sql("t_transactions", conn, if_exists="append", index=False)
        conn.commit()


def seed_messages(db):
    insert_transaction(db, blockChain="eth", hash=EULER_MESSAGE_TX, blockNumber=16818800,
                       timeStamp=1678714000, **{"from": EULER_SUPPORT, "to": EXPLOITER_2},
                       input=euler_message_input_hex())
    insert_transaction(db, blockChain="eth", hash="0xcontractcall", blockNumber=16818801,
                       timeStamp=1678714100, **{"from": "0xc", "to": EXPLOITER_2},
                       input=CONTRACT_CALL_INPUT)


def dump_transactions(db):
    with closing(sqlite3.connect(db)) as conn:
        return sorted(conn.execute("SELECT hash FROM t_transactions").fetchall())


def test_get_messages_returns_only_the_message_ordered_case_insensitive(db):
    seed_messages(db)

    messages = onchain_messages.get_messages(str(db), EXPLOITER_2.upper(), "eth")

    assert [message["hash"] for message in messages] == [EULER_MESSAGE_TX]
    assert messages[0]["text"] == EULER_MESSAGE_TEXT


def test_get_messages_unknown_address_returns_empty(db):
    seed_messages(db)

    assert onchain_messages.get_messages(str(db), "0xnotpresent", "eth") == []


def test_get_messages_does_not_write_to_db(db):
    seed_messages(db)
    before = dump_transactions(db)

    onchain_messages.get_messages(str(db), EXPLOITER_2, "eth")

    assert dump_transactions(db) == before


# ---- endpoint ----

CONFIG = {"ethscan": "K1", "bscscan": "K2", "level": "INFO"}


@pytest.fixture
def client(db):
    app_module.app.state.config = {**CONFIG, "dbname": str(db)}
    with TestClient(app_module.app) as test_client:
        yield test_client


def test_messages_endpoint_returns_only_the_message(client, db):
    seed_messages(db)

    response = client.get("/messages", params={"address": EXPLOITER_2})

    assert response.status_code == 200
    body = response.json()
    assert [message["hash"] for message in body] == [EULER_MESSAGE_TX]
    assert body[0]["text"] == EULER_MESSAGE_TEXT


def test_messages_endpoint_missing_address_is_400(client):
    response = client.get("/messages")

    assert response.status_code == 400


def test_messages_endpoint_unknown_address_returns_empty_list(client, db):
    seed_messages(db)

    response = client.get("/messages", params={"address": "0xnotpresent"})

    assert response.status_code == 200
    assert response.json() == []


def test_messages_endpoint_does_not_write_to_db(client, db):
    seed_messages(db)
    before = dump_transactions(db)

    client.get("/messages", params={"address": EXPLOITER_2})

    assert dump_transactions(db) == before


def test_messages_on_a_database_without_tables_returns_empty_list(tmp_path):
    # INFO: a fresh install that never initialised its DB must not answer 500
    assert onchain_messages.get_messages(str(tmp_path / "missing.db"), "0xabc") == []
    assert not (tmp_path / "missing.db").exists()
