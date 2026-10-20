import json
import sqlite3
from contextlib import closing

import pytest

from core import bsc, eth, misc
from test_sql_injection import insert, run_stream

TIMESTAMP = 1_700_000_000
TIMESTAMP_MS = TIMESTAMP * 1000  # INFO: Contract with the front: list timeStamp in epoch milliseconds


def seed_transactions(path, chain, address):
    insert(path, "t_transactions", blockChain=chain, hash="0x1", timeStamp=TIMESTAMP, isError=0,
           value=10**18, **{"from": address, "to": "0xb"})
    insert(path, "t_transfers", blockChain=chain, hash="0x2", timeStamp=TIMESTAMP + 1, tokenDecimal=6,
           value=2_500_000, tokenSymbol="USDT", tokenName="Tether", **{"from": "0xc", "to": address})


def seed_classification(path):
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("INSERT INTO t_nodes_classification VALUES ('0xa', '0xa', ?, '[]')", (json.dumps(["central"]),))
        conn.execute("INSERT INTO t_links_classification VALUES ('0xa0xb', '0xa', '0xb', 'ETH', 'Ether', '', 1, 1.0, ?, 'transaction')",
                     (json.dumps(["send"]),))
        conn.execute("DELETE FROM t_stats")
        columns = [row[0] for row in conn.execute("SELECT name FROM pragma_table_info('t_stats')")]
        conn.execute(f"INSERT INTO t_stats ({', '.join(columns)}) VALUES ({', '.join('1' for _ in columns)})")
        conn.commit()


def list_timestamps(result):
    return [row["timeStamp"] for row in json.loads(result["list"])]


@pytest.mark.parametrize("module, chain", [pytest.param(eth, "eth", id="eth"), pytest.param(bsc, "bsc", id="bsc")])
def test_star_list_keeps_epoch_milliseconds(db, module, chain):
    seed_transactions(db, chain, "0xa")

    with closing(sqlite3.connect(db)) as conn:
        result = module.get_trx_from_addresses_opt(conn, (chain, "0xa", "central"))

    assert list_timestamps(result) == [TIMESTAMP_MS, TIMESTAMP_MS + 1000]


def test_complex_list_keeps_epoch_milliseconds(db):
    seed_transactions(db, "eth", "0xa")
    seed_classification(db)

    with closing(sqlite3.connect(db)) as conn:
        result = eth.get_nodes_links_bd(conn, ("eth", "0xa", "central"), params={"filters": {"filter": False}})

    assert list_timestamps(result) == [TIMESTAMP_MS, TIMESTAMP_MS + 1000]
    assert result["stat"]["stat_tot"] == 1



@pytest.mark.parametrize("module, chain, complex_graph", [(eth, "eth", False), (bsc, "bsc", False), (eth, "eth", True)])
def test_list_missing_timestamp_is_null(db, module, chain, complex_graph):
    # INFO: NOT NULL still admits '', which to_datetime turns into NaT; pandas 2.3.3 serialized it as null
    seed_transactions(db, chain, "0xa")
    insert(db, "t_transactions", blockChain=chain, hash="0x3", timeStamp="", isError=0, value=10**18, **{"from": "0xa", "to": "0xd"})
    seed_classification(db)

    with closing(sqlite3.connect(db)) as conn:
        if complex_graph:
            result = eth.get_nodes_links_bd(conn, (chain, "0xa", "central"), params={"filters": {"filter": False}})
        else:
            result = module.get_trx_from_addresses_opt(conn, (chain, "0xa", "central"))

    assert list_timestamps(result) == [TIMESTAMP_MS, TIMESTAMP_MS + 1000, None]


def stats_columns(path):
    with closing(sqlite3.connect(path)) as conn:
        return [row[0] for row in conn.execute("SELECT name FROM pragma_table_info('t_stats')")]


def test_complex_without_stats_returns_zero_stats(db):
    # INFO: DBs collected before t_stats existed (or partial collections) have no stats row
    seed_transactions(db, "eth", "0xa")

    with closing(sqlite3.connect(db)) as conn:
        result = eth.get_nodes_links_bd(conn, ("eth", "0xa", "central"), params={"filters": {"filter": False}})

    assert result["stat"] == {column: 0 for column in stats_columns(db)}
    assert result["transactions"] == {"nodes": [], "links": []}
    assert list_timestamps(result) == [TIMESTAMP_MS, TIMESTAMP_MS + 1000]


def test_checking_complex_without_stats_ends_ok(db):
    insert(db, "t_tags", blockChain="eth", address="0xa", tag="central")
    insert(db, "t_address_detail", blockChain="eth", address="0xa", type="wallet")
    seed_transactions(db, "eth", "0xa")
    config = {"ethscan": "K", "bscscan": "K", "level": "INFO", "dbname": str(db),
              "action": "init", "graph": "Complex", "filters": {"filter": False}}

    messages = run_stream(misc.event_stream_checking, config)

    assert "exception" not in json.dumps(messages)
    assert [m["content"]["stat"] for m in messages if "stat" in m["content"]] == [{column: 0 for column in stats_columns(db)}]
    assert messages[-1] == {"msg": "End checking...\n", "end": True, "error": False, "content": {}}
