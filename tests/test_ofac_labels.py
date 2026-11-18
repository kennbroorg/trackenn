import json
import sqlite3
from contextlib import closing

from core import misc

RONIN_EXPLOITER = "0x098b716b8aaf21512996dc57eb0615e2383e2f96"


def labels_for(path, address):
    with closing(sqlite3.connect(path)) as conn:
        rows = conn.execute(
            "SELECT source, name, labels FROM t_labels WHERE blockChain = 'ethereum' AND address = ?",
            (address,),
        ).fetchall()
    return rows


def test_new_ofac_address_gets_a_row(db):
    with open("data/ofacSdnCryptoAddresses.json") as f:
        ofac = json.load(f)
    with open("data/etherscanCombinedAllLabels.json") as f:
        etherscan = json.load(f)
    new_address = next(address for address in ofac if address not in etherscan)

    with closing(sqlite3.connect(db)) as conn:
        misc.apply_ofac_labels(conn)

    rows = labels_for(db, new_address)
    assert len(rows) == 1
    source, name, labels_json = rows[0]
    assert source == "ofac_sdn"
    assert name == ofac[new_address]["name"]
    assert json.loads(labels_json) == ofac[new_address]["labels"]


def test_overlapping_address_keeps_one_row_and_unions_labels(db):
    with open("data/etherscanCombinedAllLabels.json") as f:
        etherscan = json.load(f)
    etherscan_entry = etherscan[RONIN_EXPLOITER]

    with closing(sqlite3.connect(db)) as conn:
        conn.execute(
            "INSERT INTO t_labels (blockChain, source, address, name, labels) VALUES (?, ?, ?, ?, ?)",
            ("ethereum", "etherscan_label", RONIN_EXPLOITER, etherscan_entry["name"], json.dumps(etherscan_entry["labels"])),
        )
        conn.commit()
        misc.apply_ofac_labels(conn)

    rows = labels_for(db, RONIN_EXPLOITER)
    assert len(rows) == 1
    source, name, labels_json = rows[0]
    assert source == "etherscan_label"
    assert name == etherscan_entry["name"]
    labels = json.loads(labels_json)
    assert "ofac-sanctions-lists" in labels
    assert "ofac-sdn" in labels
    assert set(labels) == set(etherscan_entry["labels"]) | {"ofac-sanctions-lists", "ofac-sdn"}


def test_running_loader_twice_changes_nothing(db):
    with closing(sqlite3.connect(db)) as conn:
        misc.apply_ofac_labels(conn)
        first = conn.execute(
            "SELECT blockChain, source, address, name, labels FROM t_labels WHERE blockChain = 'ethereum' ORDER BY address"
        ).fetchall()
        misc.apply_ofac_labels(conn)
        second = conn.execute(
            "SELECT blockChain, source, address, name, labels FROM t_labels WHERE blockChain = 'ethereum' ORDER BY address"
        ).fetchall()

    assert first == second


def test_existing_db_without_ofac_labels_gets_them_once(schema_db, tmp_path):
    import shutil

    path = tmp_path / "preexisting.db"
    shutil.copy(schema_db, path)

    # INFO: Simulate a database created before OFAC support existed: schema_db already
    # has t_labels with no OFAC rows (etherscan/bscscan loaded, then wiped by conftest).
    with closing(sqlite3.connect(path)) as conn:
        before = conn.execute("SELECT COUNT(*) FROM t_labels WHERE source = 'ofac_sdn'").fetchone()[0]
        assert before == 0

        config = {"ethscan": "XXX", "bscscan": "XXX", "level": "INFO", "dbname": str(path), "action": "init"}
        conn.close()

    import os

    cwd = os.getcwd()
    from pathlib import Path

    os.chdir(Path(__file__).resolve().parent.parent)
    try:
        list(misc.event_stream_checking(config))
    finally:
        os.chdir(cwd)

    with closing(sqlite3.connect(path)) as conn:
        after = conn.execute("SELECT COUNT(*) FROM t_labels WHERE source = 'ofac_sdn'").fetchone()[0]
    assert after > 0

    # INFO: Running it again must not duplicate rows (idempotent across separate init calls)
    os.chdir(Path(__file__).resolve().parent.parent)
    try:
        list(misc.event_stream_checking(config))
    finally:
        os.chdir(cwd)

    with closing(sqlite3.connect(path)) as conn:
        after_second = conn.execute("SELECT COUNT(*) FROM t_labels WHERE source = 'ofac_sdn'").fetchone()[0]
    assert after_second == after


def test_ronin_exploiter_ends_with_a_single_row_and_the_ofac_label(db):
    with closing(sqlite3.connect(db)) as conn:
        misc.apply_ofac_labels(conn)

    rows = labels_for(db, RONIN_EXPLOITER)
    assert len(rows) == 1
    labels = json.loads(rows[0][2])
    assert "ofac-sanctions-lists" in labels


def test_labels_dict_construction_still_works_with_unique_addresses(db):
    import pandas as pd

    with closing(sqlite3.connect(db)) as conn:
        misc.apply_ofac_labels(conn)
        df_labels = pd.read_sql_query("SELECT * FROM t_labels WHERE blockChain = 'ethereum'", conn)

    labels_dict = df_labels.set_index("address").to_dict("index")
    assert RONIN_EXPLOITER in labels_dict
