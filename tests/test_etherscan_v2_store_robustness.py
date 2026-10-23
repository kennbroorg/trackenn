"""Etherscan API V2 returns extra fields the V1-shaped table schemas do not have (e.g.
transactionIndex in txlistinternal). Pins three related fixes:

1. insert_with_ignore (core/eth.py, core/bsc.py) drops unknown columns instead of raising
   sqlite3.OperationalError ("table ... has no column named ...").
2. Every except-then-continue block in event_stream_ether/event_stream_bsc that closes the
   connection on a store failure must stop the generator (return), never fall through to
   later code that still uses the now-closed connection (sqlite3.ProgrammingError).
3. core/misc.py event_stream_checking must not raise IndexError/UnboundLocalError when the
   central address is tagged but t_address_detail has no row, or when the db file cannot be
   opened at all; both must surface as a clean SSE error event instead.
"""

import json
import sqlite3
from contextlib import closing

import pandas as pd
import pytest

from core import bsc, eth, misc
from test_eth_logic import CENTRAL, EXPLORER, FRIEND, FUNDER, TOKEN
from test_sql_injection import insert, run_stream, stream_params

MODULES = [pytest.param(eth, "eth", id="eth"), pytest.param(bsc, "bsc", id="bsc")]
STREAMS = [pytest.param(eth.event_stream_ether, "eth", id="eth"), pytest.param(bsc.event_stream_bsc, "bsc", id="bsc")]


# ---------------------------------------------------------------------------
# Fix A: insert_with_ignore drops unknown columns instead of crashing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("module, chain", MODULES)
def test_insert_with_ignore_drops_unknown_api_columns(db, module, chain):
    # INFO: "transactionIndex" is not a column of t_internals (see core/misc.py schema), but
    # Etherscan API V2's txlistinternal response includes it.
    row = {"blockChain": chain, "blockNumber": 1, "timeStamp": "0", "hash": "0xabc", "from": FUNDER,
           "to": CENTRAL, "value": 1, "contractAddress": "", "input": "", "type": "call", "gas": "0",
           "gasUsed": "0", "traceId": "0", "isError": "0", "errCode": "", "methodId": "", "functionName": "",
           "transactionIndex": "0"}  # INFO: unknown column, V2-only field

    with closing(sqlite3.connect(db)) as conn:
        pd.DataFrame([row]).to_sql("t_internals", conn, if_exists="append", index=False, method=module.insert_with_ignore)
        conn.commit()
        stored = conn.execute("SELECT blockChain, hash, `from`, `to` FROM t_internals").fetchall()

    assert stored == [(chain, "0xabc", FUNDER, CENTRAL)]


@pytest.mark.parametrize("module, chain", MODULES)
def test_insert_with_ignore_still_works_with_exact_columns(db, module, chain):
    # INFO: Differential safety net - the known-columns path must behave exactly as before.
    row = {"blockChain": chain, "blockNumber": 1, "timeStamp": "0", "hash": "0xdef", "from": FUNDER,
           "to": CENTRAL, "value": 2, "contractAddress": "", "input": "", "type": "call", "gas": "0",
           "gasUsed": "0", "traceId": "0", "isError": "0", "errCode": "", "methodId": "", "functionName": ""}

    with closing(sqlite3.connect(db)) as conn:
        pd.DataFrame([row]).to_sql("t_internals", conn, if_exists="append", index=False, method=module.insert_with_ignore)
        conn.commit()
        stored = conn.execute("SELECT blockChain, hash, value FROM t_internals").fetchall()

    assert stored == [(chain, "0xdef", 2)]


# ---------------------------------------------------------------------------
# Fix B: a store-step exception must stop the generator, never touch a closed connection
# ---------------------------------------------------------------------------


def drain_fully(stream, params):
    # INFO: Unlike run_stream (test_sql_injection.py), this keeps calling next() past the
    # first "end": True chunk - exactly what Flask's WSGI layer does when streaming a
    # response. run_stream's early break on "end": True would hide a generator that keeps
    # executing (and crashing) after its terminal error event.
    messages = []
    generator = stream(params)
    try:
        for chunk in generator:
            messages.append(json.loads(chunk[len("data:"):]))
    except Exception as error:
        messages.append({"exception": repr(error)})
    finally:
        generator.close()
    return messages


@pytest.mark.parametrize("stream, chain", STREAMS)
def test_store_failure_ends_stream_cleanly_without_touching_closed_connection(db, monkeypatch, stream, chain):
    module = eth if chain == "eth" else bsc
    tagging_calls = []
    monkeypatch.setattr(module, "db_store_tagging_opt", lambda *a, **k: tagging_calls.append(a))

    def get(url):
        action = url.split("action=")[1].split("&")[0]
        response = eth.api_client.requests.Response()
        response.status_code = 200
        response._content = json.dumps(EXPLORER[action]).encode()
        return response

    monkeypatch.setattr(eth.api_client, "api_get", get)

    def failing_insert(table, conn, keys, data_iter):
        if table.name == "t_internals":
            raise RuntimeError("simulated store failure")

    monkeypatch.setattr(module, "insert_with_ignore", failing_insert)

    messages = drain_fully(stream, stream_params(db, CENTRAL, chain))

    assert "exception" not in json.dumps(messages)  # INFO: no ProgrammingError leaked out of the generator
    assert "ProgrammingError" not in json.dumps(messages)
    terminal = [m for m in messages if m["end"]]
    assert len(terminal) == 1 and terminal[0]["error"] is True  # INFO: exactly one terminal event, no more chunks after it
    assert tagging_calls == []  # INFO: db_store_tagging_opt must never run after the store failure


# ---------------------------------------------------------------------------
# Fix C: misc.event_stream_checking - incomplete central data must not raise IndexError
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", ["eth", "bsc"])
def test_checking_with_central_tag_but_no_address_detail_yields_clean_error(db, chain):
    # INFO: Reproduces the aftermath of a partial store (central tagged, detail row never written)
    insert(db, "t_tags", blockChain=chain, address=CENTRAL, tag="central")
    config = {"ethscan": "K", "bscscan": "K", "level": "INFO", "dbname": str(db),
              "action": "init", "graph": "Star", "filters": {"filter": False}}

    messages = run_stream(misc.event_stream_checking, config)

    assert "exception" not in json.dumps(messages)
    assert messages[-1]["end"] is True and messages[-1]["error"] is True
    assert "reset" in messages[-1]["msg"].lower()


# ---------------------------------------------------------------------------
# Fix C: misc.event_stream_checking - an unopenable db must not raise UnboundLocalError
# ---------------------------------------------------------------------------


def test_checking_with_unopenable_db_yields_clean_error(tmp_path):
    bad_path = tmp_path / "missing-dir" / "test.db"  # INFO: parent dir does not exist
    config = {"ethscan": "K", "bscscan": "K", "level": "INFO", "dbname": str(bad_path),
              "action": "init", "graph": "Star", "filters": {"filter": False}}

    messages = run_stream(misc.event_stream_checking, config)

    assert "exception" not in json.dumps(messages)  # INFO: no UnboundLocalError leaked out of the generator
    assert "UnboundLocalError" not in json.dumps(messages)
    assert messages[-1]["end"] is True and messages[-1]["error"] is True
    assert any("unable to open database file" in m.get("msg", "") for m in messages)


# ---------------------------------------------------------------------------
# Fix D (2026-10-22 live run): V2 transfer-like responses carry methodId/
# functionName themselves; the merge with the tx columns creates _x/_y suffixes,
# the real columns vanish and the INSERT fails with NOT NULL constraint
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("stream, chain", STREAMS)
def test_store_survives_v2_tokentx_methodid_collision(db, monkeypatch, stream, chain):
    import copy
    # INFO: same shape the live V2 run returned: tokentx rows include methodId/functionName
    v2 = copy.deepcopy(EXPLORER)
    transfer_row = v2["tokentx"]["result"][0]
    transfer_row["hash"] = "0xt3"  # INFO: a tx that exists in txlist, methodId 0xa9059cbb
    transfer_row["methodId"] = "0xdeadbeef"
    transfer_row["functionName"] = "colliding(v2)"

    def get(url):
        action = url.split("action=")[1].split("&")[0]
        response = eth.api_client.requests.Response()
        response.status_code = 200
        response._content = json.dumps(v2[action]).encode()
        return response

    monkeypatch.setattr(eth.api_client, "api_get", get)

    messages = drain_fully(stream, stream_params(db, CENTRAL, chain))

    assert "exception" not in json.dumps(messages)
    terminal = [m for m in messages if m["end"]]
    assert len(terminal) == 1 and terminal[0]["error"] is False
    with closing(sqlite3.connect(db)) as conn:
        stored = conn.execute("SELECT methodId, functionName FROM t_transfers").fetchall()
    # INFO: the tx-join stays the source of truth (V1 behavior); V2's own field is dropped
    assert stored == [("0xa9059cbb", "")]
