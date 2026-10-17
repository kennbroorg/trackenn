import sqlite3
from contextlib import closing

import pytest
from fastapi.testclient import TestClient

import app as app_module
from core import misc

FRONT_ORIGIN = "http://127.0.0.1:4200"


@pytest.fixture
def dbname(tmp_path, monkeypatch):
    # INFO: Never point at the real default.db, cwd moved to the tmp dir as well
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "project.db"
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE t_tags (blockChain text, address text, tag text)")
        conn.execute("INSERT INTO t_tags VALUES ('eth', '0xa', 'central')")
        conn.commit()
    return path


@pytest.fixture
def client(dbname):
    app_module.app.state.config = {"ethscan": "K1", "bscscan": "K2", "level": "INFO", "dbname": str(dbname)}
    with TestClient(app_module.app) as test_client:
        yield test_client


def tags(path):
    with closing(sqlite3.connect(path)) as conn:
        return conn.execute("SELECT * FROM t_tags").fetchall()


def test_post_reset_db_deletes_the_project_db(client, dbname):
    response = client.post("/reset_db", headers={"Origin": FRONT_ORIGIN})

    assert response.status_code == 200
    assert response.json() == {"msg": "Database deleted", "deleted": True, "error": False}
    assert response.headers["access-control-allow-origin"] == FRONT_ORIGIN
    assert not dbname.exists()


def test_post_reset_db_without_origin_deletes_the_project_db(client, dbname):
    # INFO: Non-browser clients (curl) send no Origin
    response = client.post("/reset_db")

    assert response.status_code == 200
    assert response.json()["deleted"] is True
    assert not dbname.exists()


def test_post_reset_db_with_missing_db_is_not_an_error(client, dbname):
    dbname.unlink()

    response = client.post("/reset_db", headers={"Origin": FRONT_ORIGIN})

    assert response.status_code == 200
    assert response.json() == {"msg": "Database not found, nothing to delete", "deleted": False, "error": False}
    assert not dbname.exists()


def test_post_reset_db_reports_os_errors_and_keeps_the_db(client, dbname, monkeypatch):
    def locked(path):
        raise PermissionError(13, "Permission denied")
    monkeypatch.setattr(misc.os, "remove", locked)

    response = client.post("/reset_db", headers={"Origin": FRONT_ORIGIN})

    assert response.status_code == 500
    assert response.json() == {"msg": "Database could not be deleted: Permission denied", "deleted": False, "error": True}
    assert tags(dbname) == [("eth", "0xa", "central")]


def test_post_reset_db_from_another_origin_is_rejected(client, dbname):
    # INFO: A cross-site <form method=post> is a simple request, CORS alone does not stop its side effects
    response = client.post("/reset_db", headers={"Origin": "http://evil.example"})

    assert response.status_code == 403
    assert response.json() == {"msg": "Origin not allowed", "deleted": False, "error": True}
    assert "access-control-allow-origin" not in response.headers
    assert tags(dbname) == [("eth", "0xa", "central")]


def test_get_reset_db_is_not_allowed(client, dbname):
    response = client.get("/reset_db")

    assert response.status_code == 405
    assert tags(dbname) == [("eth", "0xa", "central")]


def test_stream_checking_reset_action_is_rejected_and_keeps_the_db(client, dbname, monkeypatch):
    calls = []
    monkeypatch.setattr(misc, "event_stream_checking", lambda config: calls.append(config) or iter(()))

    response = client.get("/stream-data-checking", params={"action": "reset"}, headers={"Origin": FRONT_ORIGIN})

    assert response.status_code == 400
    assert response.json() == {"msg": "Unsupported action 'reset', use POST /reset_db to delete the database",
                               "end": True, "error": True, "content": {}}
    assert calls == []
    assert tags(dbname) == [("eth", "0xa", "central")]


def test_event_stream_checking_never_deletes_the_db(dbname):
    config = {"ethscan": "XXX", "bscscan": "XXX", "level": "INFO", "dbname": str(dbname), "action": "reset",
              "graph": "Star", "filters": {"filter": False}}

    list(misc.event_stream_checking(config))

    assert tags(dbname) == [("eth", "0xa", "central")]
