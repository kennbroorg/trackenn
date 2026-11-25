"""R19: stale spam-unsolicited tags (old rule) are removed on re-collection with evidence."""
import json
import sqlite3
from contextlib import closing

from core import eth
from test_sql_injection import insert
from test_token_classifier import CENTRAL, DAI, FRIEND, insert_transfer, stored_tags, transfer

STALE_A = "0xa000000000000000000000000000000000000001"
STALE_B = "0xa000000000000000000000000000000000000002"
STRONG_OTHER = "0xa000000000000000000000000000000000000003"
UNRELATED = "0xa000000000000000000000000000000000000004"


def seed_old_db(db):
    # INFO: tags written by the old rule: unsolicited alone, no strong signal
    insert_transfer(db, transfer(STALE_A, FRIEND, CENTRAL, "Plain Token A", "PTA", 1))
    insert_transfer(db, transfer(STALE_B, FRIEND, CENTRAL, "Plain Token B", "PTB", 2))
    insert_transfer(db, transfer(STRONG_OTHER, FRIEND, CENTRAL, "Plain Token C", "PTC", 3))
    insert_transfer(db, transfer(DAI, FRIEND, CENTRAL, "Dai Stablecoin", "DAI", 4))
    insert_transfer(db, transfer(DAI, CENTRAL, FRIEND, "Dai Stablecoin", "DAI", 5))
    for contract in (STALE_A, STALE_B, STRONG_OTHER, UNRELATED):
        insert(db, "t_tags", blockChain="eth", address=contract, tag="spam-unsolicited")
    # INFO: strong signal recorded by another investigation for the same contract
    insert(db, "t_tags", blockChain="eth", address=STRONG_OTHER, tag="spam-phishing")
    insert(db, "t_tags", blockChain="eth", address=STALE_A, tag="token")
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("INSERT INTO t_nodes_classification (id, address, tag, label) VALUES (?, ?, ?, ?)",
                     ("n1", STALE_A, json.dumps(["token", "spam-unsolicited"]), "A"))
        conn.commit()


def recollect(db):
    with closing(sqlite3.connect(db)) as conn:
        eth.classify_cached_tokens(conn, ("eth", CENTRAL, "central"))
        conn.commit()


def all_tags(db):
    with closing(sqlite3.connect(db)) as conn:
        return sorted(conn.execute("SELECT address, tag FROM t_tags").fetchall())


def removals(db):
    with closing(sqlite3.connect(db)) as conn:
        return conn.execute(
            "SELECT endpoint, query_address, scope, rule, reason, details FROM t_decisions "
            "WHERE code = 'tag_removed' ORDER BY scope").fetchall()


def test_stale_unsolicited_tags_are_removed_and_others_kept(db):
    seed_old_db(db)

    recollect(db)

    assert stored_tags(db, STALE_A) == [("token",)]
    assert stored_tags(db, STALE_B) == []
    # INFO: not in this address's transfers -> out of scope, untouched
    assert stored_tags(db, UNRELATED) == [("spam-unsolicited",)]
    with closing(sqlite3.connect(db)) as conn:
        node_tags = json.loads(conn.execute(
            "SELECT tag FROM t_nodes_classification WHERE address = ?", (STALE_A,)).fetchone()[0])
    assert node_tags == ["token"]


def test_unsolicited_kept_when_strong_signal_comes_from_another_investigation(db):
    seed_old_db(db)

    recollect(db)

    assert stored_tags(db, STRONG_OTHER) == [("spam-phishing",), ("spam-unsolicited",)]


def test_every_removal_leaves_a_tag_removed_decision(db):
    seed_old_db(db)

    recollect(db)

    rows = removals(db)
    assert [row[2] for row in rows] == [STALE_A, STALE_B]
    for endpoint, query_address, _, rule, reason, details in rows:
        assert endpoint == "/stream-data-info"
        assert query_address == CENTRAL
        assert rule == "unsolicited-without-strong-signal"
        assert reason == ("spam-unsolicited alone is no longer a signal: "
                          "no spam-phishing, spam-spoofed-sender, spam-native-impersonation or spam-homoglyph tag")
        assert json.loads(details) == {"previous_tag": "spam-unsolicited", "central": CENTRAL}


def test_cleanup_is_idempotent(db):
    seed_old_db(db)
    recollect(db)
    tags_once, decisions_once = all_tags(db), removals(db)

    recollect(db)

    assert all_tags(db) == tags_once
    assert removals(db) == decisions_once


def test_tags_of_another_chain_are_neither_read_nor_removed(db):
    seed_old_db(db)
    insert(db, "t_tags", blockChain="bsc", address=STALE_B, tag="spam-unsolicited")
    insert(db, "t_tags", blockChain="bsc", address=STALE_A, tag="spam-phishing")

    recollect(db)

    with closing(sqlite3.connect(db)) as conn:
        rows = sorted(conn.execute("SELECT blockChain, address, tag FROM t_tags WHERE address IN (?, ?)",
                                   (STALE_A, STALE_B)).fetchall())
    assert rows == [("bsc", STALE_A, "spam-phishing"), ("bsc", STALE_B, "spam-unsolicited"),
                    ("eth", STALE_A, "token")]


def test_no_tag_is_removed_when_its_decision_cannot_be_recorded(db, monkeypatch):
    seed_old_db(db)
    before = all_tags(db)

    def broken_persist(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(eth.decisions, "persist_new", broken_persist)
    with closing(sqlite3.connect(db)) as conn:
        try:
            eth.remove_stale_unsolicited_tags(conn, CENTRAL, [transfer(STALE_A, FRIEND, CENTRAL, "A", "A", 1)])
        except sqlite3.OperationalError:
            pass

    assert all_tags(db) == before


def test_db_failure_during_cleanup_is_recorded_not_raised(db):
    seed_old_db(db)
    before = all_tags(db)
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("DROP TABLE t_nodes_classification")
        conn.commit()

    recollect(db)

    assert all_tags(db) == before
    with closing(sqlite3.connect(db)) as conn:
        rows = conn.execute("SELECT code, scope, rule FROM t_decisions WHERE code IN ('db_error', 'tag_removed')").fetchall()
    assert rows == [("db_error", CENTRAL, "stale tag cleanup skipped on database error")]


def test_unsolicited_kept_when_homoglyph_is_the_strong_signal(db):
    seed_old_db(db)
    insert(db, "t_tags", blockChain="eth", address=STALE_B, tag="spam-homoglyph")

    recollect(db)

    tags = all_tags(db)
    assert (STALE_B, "spam-unsolicited") in tags
    assert (STALE_A, "spam-unsolicited") not in tags
