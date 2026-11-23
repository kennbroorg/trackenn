"""R15 cache, partial results and decision evidence for GET /created-contracts
(odd/tasks/created-contracts-cache.md, T1, S1, S2, S5). Offline: fetch_all_pages is faked."""
import sqlite3
from contextlib import closing

import pytest
from fastapi.testclient import TestClient

import app as app_module
from core import api_client, created_contracts, decisions

CENTRAL = "0xa0000000000000000000000000000000000000aa"
CHAIN_ID = api_client.ETH_CHAIN_ID
KEY = "K"


def create_row(n, frm, contract_address):
    return {"blockNumber": str(100 + n), "timeStamp": str(1_700_000_000 + n), "hash": f"0xi{n}",
            "from": frm, "to": "", "contractAddress": contract_address, "type": "create"}


def wide_tree(width):
    """Level 1: width contracts; each creates one level-2 child; children create nothing."""
    level1 = [{"hash": f"0xc{i}", "timeStamp": str(i), "contractAddress": f"0xp{i:03d}", "type": "create"}
              for i in range(width)]
    table = {f"0xp{i:03d}": [create_row(i, f"0xp{i:03d}", f"0xk{i:03d}")] for i in range(width)}
    return level1, table


class FakeFetch:
    def __init__(self, table, fail_at=None):
        self.table, self.fail_at, self.calls = table, fail_at, []

    def __call__(self, chainid, action, address, key, **kwargs):
        self.calls.append((action, address.lower(), kwargs.get("startblock", 0)))
        if self.fail_at is not None and len(self.calls) == self.fail_at:
            raise Exception("Max rate limit reached")
        return self.table.get(address.lower(), []), True


def walk(db, level1, fetch, refresh=False):
    source = created_contracts.Source(str(db), "eth", CHAIN_ID, KEY, fetch, refresh=refresh)
    return created_contracts.find_created_contracts(CENTRAL, level1, CHAIN_ID, KEY, fetch, source=source)


def codes(result):
    return [d["code"] for d in result["decisions"]]


# ---- S2: failure at call 40 keeps what was found and says so ----

def test_failure_mid_walk_returns_partial_result_marked_incomplete(db):
    level1, table = wide_tree(24)  # 24 level-1 + 24 level-2 = 48 contracts, ~48 expansions
    fetch = FakeFetch(table, fail_at=40)

    result = walk(db, level1, fetch)

    assert result["incomplete"] is True
    found = {c["address"] for c in result["contracts"]}
    assert {f"0xp{i:03d}" for i in range(24)} <= found  # level 1 never lost
    assert len(found) > 24  # children expanded before the failure kept
    assert result["pending"], "addresses left unexpanded must be listed"
    error = next(d for d in result["decisions"] if d["code"] == "upstream_error")
    assert error["reason"] == "Max rate limit reached"
    assert set(error["details"]["pending"]) == set(result["pending"])
    assert len(fetch.calls) == 40  # stops calling after the first failure


def test_second_identical_query_repeats_no_successful_call(db):
    level1, table = wide_tree(24)
    first = FakeFetch(table, fail_at=40)
    walk(db, level1, first)
    successful = {address for action, address, _ in first.calls[:39]}

    second = FakeFetch(table)
    result = walk(db, level1, second)

    assert not successful & {address for _, address, _ in second.calls}
    assert result["incomplete"] is False
    assert len(result["contracts"]) == 48


# ---- S1: cache ----

def test_cached_query_makes_no_call_and_returns_identical_contracts(db):
    level1, table = wide_tree(5)
    first = walk(db, level1, FakeFetch(table))
    fetch = FakeFetch(table)

    second = walk(db, level1, fetch)

    assert fetch.calls == []
    assert second["contracts"] == first["contracts"]
    served = next(d for d in second["decisions"] if d["code"] == "served_from_cache")
    assert len(served["details"]["addresses"]) == 10
    assert "served_from_cache" not in codes(first)


def test_refresh_refetches_from_last_cached_block_and_merges(db):
    level1, table = wide_tree(1)
    walk(db, level1, FakeFetch(table))
    table["0xp000"] = table["0xp000"] + [create_row(50, "0xp000", "0xnew")]
    fetch = FakeFetch(table)

    result = walk(db, level1, fetch, refresh=True)

    assert ("txlistinternal", "0xp000", 100) in fetch.calls  # from the last cached block, not 0
    p000 = [c for c in result["contracts"] if c["creator"] == "0xp000"]
    assert sorted(c["address"] for c in p000) == ["0xk000", "0xnew"]


def test_failed_expansion_is_not_cached_as_empty(db):
    level1, table = wide_tree(1)
    walk(db, level1, FakeFetch(table, fail_at=1))
    fetch = FakeFetch(table)

    result = walk(db, level1, fetch)

    assert ("txlistinternal", "0xp000", 0) in fetch.calls
    assert "0xk000" in {c["address"] for c in result["contracts"]}


def test_without_cache_db_walk_still_works_and_says_cache_is_off(tmp_path):
    level1, table = wide_tree(1)
    fetch = FakeFetch(table)

    result = walk(tmp_path / "missing.db", level1, fetch)

    assert {c["address"] for c in result["contracts"]} == {"0xp000", "0xk000"}
    assert "cache_unavailable" in codes(result)
    assert not (tmp_path / "missing.db").exists()  # never creates a DB file


# ---- S5: decision shape and persistence ----

def test_every_decision_has_code_scope_rule_reason_details(db):
    level1, table = wide_tree(24)
    result = walk(db, level1, FakeFetch(table, fail_at=40))

    assert result["decisions"]
    for decision in result["decisions"]:
        assert set(decision) == {"code", "scope", "rule", "reason", "details"}
        assert decision["code"] in decisions.CODES


def test_clean_uncached_walk_has_no_decisions(db):
    level1, table = wide_tree(2)

    result = walk(db, level1, FakeFetch(table))

    assert result["decisions"] == []
    assert result["incomplete"] is False


CONFIG = {"ethscan": "K1", "bscscan": "K2", "level": "INFO"}


@pytest.fixture
def client(db):
    app_module.app.state.config = {**CONFIG, "dbname": str(db)}
    with TestClient(app_module.app) as test_client:
        yield test_client


def test_endpoint_returns_and_persists_decisions(client, db, monkeypatch):
    def failing(*args, **kwargs):
        raise Exception("Max rate limit reached")
    monkeypatch.setattr(api_client, "fetch_all_pages", failing)

    body = client.get("/created-contracts", params={"address": CENTRAL}).json()

    assert body["incomplete"] is True
    assert body["pending"] == [CENTRAL]
    assert [d["code"] for d in body["decisions"]] == ["upstream_error"]
    with closing(sqlite3.connect(db)) as conn:
        stored = conn.execute("SELECT endpoint, query_address, code, scope FROM t_decisions").fetchall()
    assert stored == [("/created-contracts", CENTRAL, "upstream_error", CENTRAL)]
