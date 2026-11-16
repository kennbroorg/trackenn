"""Pagination helper (core/api_client.fetch_all_pages) and its use in core/eth.py /
core/bsc.py for the wallet account lists: S1-S8 of odd/tasks/collector-pagination.md.
"""
import json
import sqlite3
from contextlib import closing

import pandas as pd
import pytest

from core import api_client, bsc, eth
from test_eth_logic import CENTRAL, EXPLORER, FRIEND, FUNDER, tx
from test_sql_injection import insert, run_stream, stream_params

CHAIN_ID = api_client.ETH_CHAIN_ID


def page_response(rows):
    response = api_client.requests.Response()
    response.status_code = 200
    response._content = json.dumps({"status": "1", "message": "OK", "result": rows}).encode()
    return response


def row(block, n, frm="0xfrom", to="0xto", value=1):
    return {"blockNumber": str(block), "timeStamp": str(1_700_000_000 + n), "hash": f"0xh{n}",
            "from": frm, "to": to, "value": str(value)}


# ---------------------------------------------------------------------------
# fetch_all_pages unit tests
# ---------------------------------------------------------------------------


@pytest.fixture
def pages(monkeypatch):
    """Serve EXPLORER_PAGES[action], a list of pages, keyed by the startblock of the request."""
    calls = []
    served = {}

    def get(url):
        calls.append(url)
        action = url.split("action=")[1].split("&")[0]
        startblock = int(url.split("startblock=")[1].split("&")[0])
        queue = served[action]
        index = queue["starts"].index(startblock) if startblock in queue["starts"] else len(queue["pages"]) - 1
        page = queue["pages"][min(index, len(queue["pages"]) - 1)] if queue["starts"] else queue["pages"][queue["calls"]]
        queue["calls"] += 1
        return page_response(page)

    def register(action, pages_by_start):
        # pages_by_start: list of (startblock, rows) pairs served in order of calls
        served[action] = {"starts": [s for s, _ in pages_by_start], "pages": [r for _, r in pages_by_start], "calls": 0}

    monkeypatch.setattr(api_client, "api_get", get)
    return calls, register


def test_helper_collects_rows_across_pages(pages):
    calls, register = pages
    page1 = [row(100 + i, i) for i in range(1000)]
    page2 = [row(2000 + i, 1000 + i) for i in range(34)]
    register("txlist", [(0, page1), (1099, page2)])

    rows, complete = api_client.fetch_all_pages(CHAIN_ID, "txlist", "0xabc", "K", page_size=1000, max_rows=50000)

    assert len(rows) == 1034
    assert complete is True


def test_helper_keeps_every_row_once_when_a_block_is_split_across_pages():
    monkeypatch_calls = []

    def get(url):
        startblock = int(url.split("startblock=")[1].split("&")[0])
        if startblock == 0:
            page = [row(100, i) for i in range(998)] + [row(200, 998 + i) for i in range(2)]
        elif startblock == 200:
            page = [row(200, i) for i in range(1000, 1000 + 3)] + [row(201, 1003)]
        else:
            page = []
        monkeypatch_calls.append(startblock)
        return page_response(page)

    import unittest.mock
    with unittest.mock.patch.object(api_client, "api_get", get):
        rows, complete = api_client.fetch_all_pages(CHAIN_ID, "txlist", "0xabc", "K", page_size=1000, max_rows=50000)

    assert len(rows) == len({r["hash"] for r in rows})
    assert len(rows) == 1004
    assert complete is True


def test_helper_never_loops_forever_when_one_block_has_more_rows_than_a_page():
    def get(url):
        # INFO: every page is the exact same 1000 rows of block 100: startblock never advances
        return page_response([row(100, i) for i in range(1000)])

    import unittest.mock
    with unittest.mock.patch.object(api_client, "api_get", get):
        rows, complete = api_client.fetch_all_pages(CHAIN_ID, "txlist", "0xabc", "K", page_size=1000, max_rows=50000)

    assert len(rows) == 1000
    assert complete is True


def test_helper_cap_reached_marks_incomplete_and_caps_rows():
    counter = {"start": 0}

    def get(url):
        startblock = int(url.split("startblock=")[1].split("&")[0])
        block = max(startblock, counter["start"])
        page = [row(block + i, block + i) for i in range(1000)]
        counter["start"] = block + 1000
        return page_response(page)

    import unittest.mock
    with unittest.mock.patch.object(api_client, "api_get", get):
        rows, complete = api_client.fetch_all_pages(CHAIN_ID, "txlist", "0xabc", "K", page_size=1000, max_rows=2500)

    assert len(rows) == 2500
    assert complete is False


def test_helper_empty_list_returns_complete():
    def get(url):
        return page_response([])

    import unittest.mock
    with unittest.mock.patch.object(api_client, "api_get", get):
        rows, complete = api_client.fetch_all_pages(CHAIN_ID, "txlist", "0xabc", "K", page_size=1000, max_rows=50000)

    assert rows == []
    assert complete is True


def test_helper_notok_on_second_page_raises():
    calls = {"n": 0}

    def get(url):
        calls["n"] += 1
        if calls["n"] == 1:
            return page_response([row(100 + i, i) for i in range(1000)])
        response = api_client.requests.Response()
        response.status_code = 200
        response._content = json.dumps({"status": "0", "message": "NOTOK", "result": "Max rate limit reached"}).encode()
        return response

    import unittest.mock
    with unittest.mock.patch.object(api_client, "api_get", get):
        with pytest.raises(Exception, match="Max rate limit reached"):
            api_client.fetch_all_pages(CHAIN_ID, "txlist", "0xabc", "K", page_size=1000, max_rows=50000)


# ---------------------------------------------------------------------------
# eth/bsc streams use the helper end-to-end (S1, S2, S4, S6)
# ---------------------------------------------------------------------------


def txlist_pages(total, first_block=100):
    rows = [tx(i, FUNDER if i == 1 else CENTRAL, CENTRAL if i == 1 else FRIEND, 10**18) for i in range(1, total + 1)]
    return rows


def paged_explorer(monkeypatch, overrides):
    """Like test_eth_logic.explorer, but overrides[action] is a callable(startblock) -> rows."""
    calls = []

    def get(url):
        action = url.split("action=")[1].split("&")[0]
        calls.append(action)
        if action in overrides:
            startblock = int(url.split("startblock=")[1].split("&")[0]) if "startblock=" in url else 0
            rows = overrides[action](startblock)
            payload = {"status": "1" if rows else "0", "message": "OK" if rows else "No transactions found", "result": rows}
        else:
            payload = EXPLORER[action]
        response = api_client.requests.Response()
        response.status_code = 200
        response._content = json.dumps(payload).encode()
        return response

    monkeypatch.setattr(api_client, "api_get", get)
    return calls


def make_txlist_pager(total_rows):
    """1000-row pages of synthetic txlist rows, ascending blocks, honoring startblock."""
    all_rows = [tx(i, FUNDER if i == 1 else CENTRAL, FRIEND, 10**18) for i in range(1, total_rows + 1)]

    def pager(startblock):
        page = [r for r in all_rows if int(r["blockNumber"]) >= startblock][:1000]
        return page

    return pager


@pytest.mark.parametrize("stream, chain", [pytest.param(eth.event_stream_ether, "eth", id="eth"),
                                            pytest.param(bsc.event_stream_bsc, "bsc", id="bsc")])
def test_new_wallet_with_1034_txlist_rows_stores_all_and_all_data_true(db, monkeypatch, stream, chain):
    overrides = {"txlist": make_txlist_pager(1034)}
    paged_explorer(monkeypatch, overrides)

    params = stream_params(db, CENTRAL, chain)
    run_stream(stream, params)

    with closing(sqlite3.connect(db)) as conn:
        count = conn.execute("SELECT count(*) FROM t_transactions").fetchone()[0]
        all_data = conn.execute("SELECT all_data FROM t_blocks WHERE address = ?", (CENTRAL,)).fetchone()[0]

    assert count == 1034
    assert all_data == 1


@pytest.mark.parametrize("stream, chain", [pytest.param(eth.event_stream_ether, "eth", id="eth"),
                                            pytest.param(bsc.event_stream_bsc, "bsc", id="bsc")])
def test_new_wallet_cap_hit_stores_cap_and_emits_incomplete_message(db, monkeypatch, stream, chain):
    # INFO: a small cap keeps the test fast; the real 50000 default is pinned below
    monkeypatch.setattr(api_client, "MAX_ROWS_PER_LIST", 2500)
    overrides = {"txlist": make_txlist_pager(3100)}
    paged_explorer(monkeypatch, overrides)

    params = stream_params(db, CENTRAL, chain)
    messages = run_stream(stream, params)

    with closing(sqlite3.connect(db)) as conn:
        count = conn.execute("SELECT count(*) FROM t_transactions").fetchone()[0]
        all_data = conn.execute("SELECT all_data FROM t_blocks WHERE address = ?", (CENTRAL,)).fetchone()[0]

    assert count == 2500
    assert all_data == 0
    assert any("incomplete" in m["msg"].lower() for m in messages)


# ---------------------------------------------------------------------------
# S7 cached completion
# ---------------------------------------------------------------------------


def seed_truncated_wallet(db, chain, address, stored_rows, all_data):
    # INFO: same defaults as test_sql_injection.insert (missing columns = 0) but one write
    #       for the whole seed; a commit per row made these tests slow
    with closing(sqlite3.connect(db)) as conn:
        columns = [r[0] for r in conn.execute("SELECT name FROM pragma_table_info('t_transactions')")]
        rows = []
        for i in range(stored_rows):
            values = {"blockChain": chain, "hash": f"0xc{i}", "blockNumber": 100 + i, "timeStamp": 1_700_000_000 + i,
                      "from": FUNDER if i == 0 else address, "to": address if i == 0 else FRIEND, "value": 10**18}
            rows.append({column: values.get(column, 0) for column in columns})
        pd.DataFrame(rows).to_sql("t_transactions", conn, if_exists="append", index=False)
        conn.commit()
    insert(db, "t_address_detail", blockChain=chain, address=address, type="wallet",
           first_block=100, last_block=100 + stored_rows - 1)
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("INSERT INTO t_blocks VALUES (?, ?, ?, ?, ?, ?, ?)",
                     (chain, address, 100, 100 + stored_rows - 1, 1_700_000_000, 1_700_000_000 + stored_rows - 1, all_data))
        conn.commit()


@pytest.mark.parametrize("stream, chain", [pytest.param(eth.event_stream_ether, "eth", id="eth"),
                                            pytest.param(bsc.event_stream_bsc, "bsc", id="bsc")])
def test_cached_wallet_with_exactly_1000_rows_completes_on_search(db, monkeypatch, stream, chain):
    seed_truncated_wallet(db, chain, CENTRAL, 1000, all_data=1)

    def txlist_pager(startblock):
        all_rows = [tx(1000 + i, CENTRAL, FRIEND, 10**18) for i in range(34)]
        for idx, r in enumerate(all_rows):
            r["blockNumber"] = str(1100 + idx)
        return [r for r in all_rows if int(r["blockNumber"]) >= startblock]

    paged_explorer(monkeypatch, {"txlist": txlist_pager})

    messages = run_stream(stream, stream_params(db, CENTRAL, chain))

    with closing(sqlite3.connect(db)) as conn:
        count = conn.execute("SELECT count(*) FROM t_transactions").fetchone()[0]
        block_to = conn.execute("SELECT block_to FROM t_blocks WHERE address = ?", (CENTRAL,)).fetchone()[0]

    assert count == 1034
    assert block_to == 1133
    assert messages[-1] == {"msg": "End collection", "end": True, "error": False, "content": {}}


@pytest.mark.parametrize("stream, chain", [pytest.param(eth.event_stream_ether, "eth", id="eth"),
                                            pytest.param(bsc.event_stream_bsc, "bsc", id="bsc")])
def test_cached_wallet_with_fewer_than_1000_rows_and_all_data_true_makes_no_api_call(db, monkeypatch, stream, chain):
    seed_truncated_wallet(db, chain, CENTRAL, 10, all_data=1)
    calls = paged_explorer(monkeypatch, {})

    run_stream(stream, stream_params(db, CENTRAL, chain))

    assert calls == []


def test_default_cap_is_50000_rows_per_list():
    assert api_client.MAX_ROWS_PER_LIST == 50000
