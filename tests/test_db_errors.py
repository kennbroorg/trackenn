"""R17 visible DB errors (odd/tasks/created-contracts-cache.md, T3, S4): a locked or unreadable
investigation DB is logged and reported, never answered as "no data". A DB not created yet
(missing file or table) is still no data."""
import logging
import sqlite3
from contextlib import closing

import pytest
from fastapi.testclient import TestClient

import app as app_module
from core import api_client, created_contracts, onchain_messages

CENTRAL = "0xa0000000000000000000000000000000000000aa"
CONFIG = {"ethscan": "K1", "bscscan": "K2", "level": "INFO"}


@pytest.fixture
def client(db):
    app_module.app.state.config = {**CONFIG, "dbname": str(db)}
    with TestClient(app_module.app) as test_client:
        yield test_client


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
    # INFO: sqlite waits 5 s on a lock by default; tests must not
    real_connect = sqlite3.connect
    monkeypatch.setattr(sqlite3, "connect", lambda *a, **k: real_connect(*a, **{**k, "timeout": 0}))


# ---- /messages ----

def test_messages_locked_db_is_503_and_logged(client, locked, no_wait, caplog):
    with caplog.at_level(logging.ERROR):
        response = client.get("/messages", params={"address": CENTRAL})

    assert response.status_code == 503
    assert response.json()["detail"] == "Investigation database could not be read"
    assert any("database is locked" in record.getMessage() for record in caplog.records)


def test_messages_healthy_db_unchanged(client):
    response = client.get("/messages", params={"address": CENTRAL})

    assert response.status_code == 200
    assert response.json() == []


def test_messages_missing_db_is_still_no_data(tmp_path):
    assert onchain_messages.get_messages(str(tmp_path / "missing.db"), CENTRAL) == []


# ---- /created-contracts ----

def test_created_contracts_locked_db_reports_db_error_and_logs(client, locked, no_wait, monkeypatch, caplog):
    calls = []
    monkeypatch.setattr(api_client, "fetch_all_pages", lambda *a, **k: calls.append(a) or ([], True))

    with caplog.at_level(logging.ERROR):
        body = client.get("/created-contracts", params={"address": CENTRAL}).json()

    assert body["incomplete"] is True
    assert body["contracts"] == []
    assert body["pending"] == [CENTRAL]
    error = next(d for d in body["decisions"] if d["code"] == "db_error")
    assert error["reason"] == "database is locked"
    assert error["scope"] == CENTRAL
    assert calls == []  # never mistaken for "not collected" and re-downloaded
    assert any("database is locked" in record.getMessage() for record in caplog.records)


def test_created_contracts_missing_db_is_still_not_collected(tmp_path):
    fetched = []

    def fetch(chainid, action, address, key, **kwargs):
        fetched.append(action)
        return [], True

    result = created_contracts.get_created_contracts(
        str(tmp_path / "missing.db"), CENTRAL, "eth", api_client.ETH_CHAIN_ID, "K", fetch_all_pages=fetch)

    assert "db_error" not in [d["code"] for d in result["decisions"]]
    assert fetched == ["txlist", "txlistinternal"]


def test_created_contracts_healthy_db_unchanged(client, db, monkeypatch):
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("INSERT INTO t_blocks VALUES ('eth', ?, 0, 100, 0, 0, 1)", (CENTRAL,))
        conn.commit()
    monkeypatch.setattr(api_client, "fetch_all_pages", lambda *a, **k: ([], True))

    body = client.get("/created-contracts", params={"address": CENTRAL}).json()

    assert body["incomplete"] is False
    assert body["decisions"] == []
