"""PR #14 review follow-ups: R1-001 sanitized 503 detail, R1-002 no API key in decision
reasons, R4-1 an existing DB that cannot be opened is db_error, not "not created yet"."""
import logging
import os
import sqlite3

import pytest
from fastapi.testclient import TestClient

import app as app_module
from core import api_client, created_contracts, decisions, onchain_messages

CENTRAL = "0xa0000000000000000000000000000000000000aa"
CONFIG = {"ethscan": "K1", "bscscan": "K2", "level": "INFO"}


@pytest.fixture
def client(db):
    app_module.app.state.config = {**CONFIG, "dbname": str(db)}
    with TestClient(app_module.app) as test_client:
        yield test_client


@pytest.fixture
def locked(db, monkeypatch):
    real_connect = sqlite3.connect
    monkeypatch.setattr(sqlite3, "connect", lambda *a, **k: real_connect(*a, **{**k, "timeout": 0}))
    connection = real_connect(db, timeout=0)
    connection.execute("BEGIN EXCLUSIVE")
    yield db
    connection.rollback()
    connection.close()


# ---- R1-001 ----

def test_messages_503_detail_is_fixed_and_raw_error_only_logged(client, locked, caplog):
    with caplog.at_level(logging.ERROR):
        response = client.get("/messages", params={"address": CENTRAL})

    assert response.status_code == 503
    assert response.json() == {"detail": "Investigation database could not be read"}
    assert any("database is locked" in r.getMessage() for r in caplog.records)


# ---- R1-002 ----

def test_decision_reason_never_carries_the_api_key():
    leaked = "HTTPSConnectionPool: url=https://api.etherscan.io/v2/api?module=account&apikey=SECRET123&x=1"

    decision = decisions.make("upstream_error", CENTRAL, "rule", leaked)

    assert "SECRET123" not in decision["reason"]
    assert "apikey=***" in decision["reason"]


def test_upstream_error_reason_in_response_is_redacted(db):
    def fetch(chainid, action, address, key, **kwargs):
        raise Exception(f"timeout for https://api.etherscan.io/v2/api?apikey={key}&module=account")

    result = created_contracts.get_created_contracts(
        str(db), CENTRAL, "eth", api_client.ETH_CHAIN_ID, "SECRET123", fetch_all_pages=fetch)

    assert result["decisions"]
    assert all("SECRET123" not in d["reason"] for d in result["decisions"])


# ---- R4-1 ----

@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file permissions")
def test_existing_unopenable_db_is_db_error_not_no_data(db):
    os.chmod(db, 0)
    try:
        with pytest.raises(decisions.DatabaseUnreadable):
            onchain_messages.get_messages(str(db), CENTRAL)
        result = created_contracts.get_created_contracts(
            str(db), CENTRAL, "eth", api_client.ETH_CHAIN_ID, "K",
            fetch_all_pages=lambda *a, **k: pytest.fail("must not download"))
    finally:
        os.chmod(db, 0o644)

    assert [d["code"] for d in result["decisions"]] == ["db_error"]


def test_missing_db_file_is_still_no_data(tmp_path):
    assert onchain_messages.get_messages(str(tmp_path / "missing.db"), CENTRAL) == []
