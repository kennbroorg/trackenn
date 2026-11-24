"""odd/tasks/decision-evidence-retrofit.md, T1 S1/S2/S5/S6 and T2 S3/S6:

T1: every token contract the classifier tags gets one `token_flagged` decision (rule = the
tag that fired, details.trigger = the value that fired it), without changing which
contracts/tags t_tags ends up with (S5) and without duplicating decisions on re-tagging.

T2: an account list download that returns complete=False persists one `history_incomplete`
decision for that address/list, same dedup rule.
"""
import sqlite3
from contextlib import closing

import pytest

from core import api_client, eth, token_classifier
from test_eth_logic import EXPLORER as BASE_EXPLORER, FRIEND
from test_sql_injection import run_stream, stream_params
from test_token_classifier import CENTRAL as CLASSIFIER_CENTRAL
from test_token_classifier import USDC, insert_transfer, seed_cached_case, stored_tags, transfer

NATIVE_FAKE = "0x739352337c902c3874b95f14e81ebbcf1b7b262e"
# INFO: HACKER_INU from USDC triggers only spoofed-sender (no URL/lure words, not the
# native coin), so it gets exactly one token_flagged decision with that rule.
SPOOFED_TOKEN = "0xa59af77c2288e44021867a741e67d7155d6fec66"


def read_decisions(db):
    with closing(sqlite3.connect(db)) as conn:
        try:
            return conn.execute(
                "SELECT endpoint, query_address, code, scope, rule, details FROM t_decisions"
            ).fetchall()
        except sqlite3.OperationalError:
            return []


# ---- T1 S1: fake "Ether"/"ETH" ERC-20 -> token_flagged with the name as trigger ----

def test_native_impersonation_token_is_flagged_with_name_as_trigger(db):
    t = transfer(NATIVE_FAKE, FRIEND, CLASSIFIER_CENTRAL, "Ether", "ETH", 1)
    insert_transfer(db, t)

    with closing(sqlite3.connect(db)) as conn:
        eth.db_store_tagging_opt(conn, CLASSIFIER_CENTRAL, [], [t], [], [], [])
        conn.commit()

    rows = read_decisions(db)
    assert len(rows) == 1
    endpoint, query_address, code, scope, rule, details = rows[0]
    assert endpoint == "/stream-data-info"
    assert query_address == CLASSIFIER_CENTRAL
    assert code == "token_flagged"
    assert scope == NATIVE_FAKE
    assert rule == token_classifier.TAG_NATIVE_IMPERSONATION
    assert '"trigger": "Ether"' in details


# ---- T1 S1: spoofed sender -> trigger is the spoofed `from` address ----

def test_spoofed_sender_token_is_flagged_with_spoofed_from_as_trigger(db):
    t = transfer(SPOOFED_TOKEN, USDC, CLASSIFIER_CENTRAL, "Euler Hacker Inu", "EULERHACKERINU", 1)
    insert_transfer(db, t)

    with closing(sqlite3.connect(db)) as conn:
        eth.db_store_tagging_opt(conn, CLASSIFIER_CENTRAL, [], [t], [], [], [])
        conn.commit()

    rows = read_decisions(db)
    flagged = [r for r in rows if r[2] == "token_flagged" and r[4] == token_classifier.TAG_SPOOFED_SENDER]
    assert len(flagged) == 1
    _, _, _, scope, _, details = flagged[0]
    assert scope == SPOOFED_TOKEN
    assert f'"trigger": "{USDC}"' in details


# ---- T1 S2/S5: tagged set identical with and without the change ----

def test_tagged_set_is_identical_to_before_the_decision_change(db):
    seed_cached_case(db)

    with closing(sqlite3.connect(db)) as conn:
        eth.classify_cached_tokens(conn, ("eth", CLASSIFIER_CENTRAL, "central"))
        conn.commit()

    from test_token_classifier import DAI, HACKER_INU, USDREWARD

    assert stored_tags(db, USDREWARD) == sorted([
        (token_classifier.TAG_PHISHING,),
        (token_classifier.TAG_SPOOFED_SENDER,),
        (token_classifier.TAG_UNSOLICITED,),
    ])
    assert stored_tags(db, HACKER_INU) == []
    assert stored_tags(db, DAI) == []


# ---- T1: re-running tagging does not duplicate decisions ----

def test_retagging_the_same_address_does_not_duplicate_decisions(db):
    t = transfer(NATIVE_FAKE, FRIEND, CLASSIFIER_CENTRAL, "Ether", "ETH", 1)
    insert_transfer(db, t)

    with closing(sqlite3.connect(db)) as conn:
        eth.db_store_tagging_opt(conn, CLASSIFIER_CENTRAL, [], [t], [], [], [])
        conn.commit()
        eth.apply_classifier_tags(conn, CLASSIFIER_CENTRAL, [t], [])
        conn.commit()

    rows = read_decisions(db)
    assert len(rows) == 1


# ---- T2 S3: list returning complete=False -> history_incomplete with the right action ----

@pytest.fixture
def explorer_with_incomplete_action(monkeypatch):
    """Like test_eth_logic.explorer, but tokentx reports complete=False via fetch_all_pages."""
    real_fetch_all_pages = api_client.fetch_all_pages

    def fake_fetch_all_pages(chainid, action, address, key, **kwargs):
        rows = BASE_EXPLORER.get(action, {}).get("result") or []
        if action == "tokentx":
            return rows, False
        return rows, True

    monkeypatch.setattr(api_client, "fetch_all_pages", fake_fetch_all_pages)

    def get(url):
        action = url.split("action=")[1].split("&")[0]
        response = api_client.requests.Response()
        response.status_code = 200
        import json as json_module
        response._content = json_module.dumps(BASE_EXPLORER.get(action, {"status": "0", "message": "No data found", "result": []})).encode()
        return response

    monkeypatch.setattr(api_client, "api_get", get)
    return real_fetch_all_pages


def test_incomplete_account_list_persists_history_incomplete(db, explorer_with_incomplete_action):
    import test_eth_logic

    central = test_eth_logic.CENTRAL
    run_stream(eth.event_stream_ether, stream_params(db, central, "eth"))

    rows = read_decisions(db)
    flagged = [r for r in rows if r[2] == "history_incomplete"]
    assert len(flagged) == 1
    endpoint, query_address, code, scope, rule, details = flagged[0]
    assert endpoint == "/stream-data-info"
    assert query_address == central
    assert scope == central
    assert f"MAX_ROWS_PER_LIST={api_client.MAX_ROWS_PER_LIST}" == rule
    assert '"action": "tokentx"' in details


def test_all_complete_account_lists_persist_no_history_incomplete(db, monkeypatch):
    import json as json_module

    import test_eth_logic

    def get(url):
        action = url.split("action=")[1].split("&")[0]
        response = api_client.requests.Response()
        response.status_code = 200
        response._content = json_module.dumps(BASE_EXPLORER[action]).encode()
        return response

    monkeypatch.setattr(api_client, "api_get", get)

    central = test_eth_logic.CENTRAL
    run_stream(eth.event_stream_ether, stream_params(db, central, "eth"))

    rows = read_decisions(db)
    assert [r for r in rows if r[2] == "history_incomplete"] == []
