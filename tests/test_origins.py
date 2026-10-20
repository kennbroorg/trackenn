import importlib
import sqlite3
from contextlib import closing

import pytest
from fastapi.testclient import TestClient

import app as app_module

DEFAULT_ORIGIN = "http://127.0.0.1:4200"
EXTRA_ORIGIN = "http://localhost:4200"


@pytest.fixture
def load_app(monkeypatch, tmp_path):
    # INFO: Origins are read when the module is imported, reload it with the env under test
    monkeypatch.chdir(tmp_path)

    def load(env_value=None):
        if env_value is None:
            monkeypatch.delenv("TRACKENN_FRONT_ORIGINS", raising=False)
        else:
            monkeypatch.setenv("TRACKENN_FRONT_ORIGINS", env_value)
        module = importlib.reload(app_module)
        dbname = tmp_path / "project.db"
        with closing(sqlite3.connect(dbname)) as conn:
            conn.execute("CREATE TABLE t_tags (tag text)")
            conn.commit()
        module.app.state.config = {"ethscan": "K1", "bscscan": "K2", "level": "INFO", "dbname": str(dbname)}
        return module, dbname

    yield load
    monkeypatch.delenv("TRACKENN_FRONT_ORIGINS", raising=False)
    importlib.reload(app_module)


def test_front_origins_default_when_env_is_unset(monkeypatch):
    monkeypatch.delenv("TRACKENN_FRONT_ORIGINS", raising=False)

    assert app_module.front_origins() == [DEFAULT_ORIGIN]


@pytest.mark.parametrize("value", ["", "  ", " , "])
def test_front_origins_default_when_env_is_empty(monkeypatch, value):
    monkeypatch.setenv("TRACKENN_FRONT_ORIGINS", value)

    assert app_module.front_origins() == [DEFAULT_ORIGIN]


def test_front_origins_reads_comma_separated_env(monkeypatch):
    monkeypatch.setenv("TRACKENN_FRONT_ORIGINS", f" {DEFAULT_ORIGIN} ,{EXTRA_ORIGIN},")

    assert app_module.front_origins() == [DEFAULT_ORIGIN, EXTRA_ORIGIN]


def test_front_origins_drops_wildcard(monkeypatch):
    # INFO: Starlette opens CORS to every site when "*" is anywhere in allow_origins
    monkeypatch.setenv("TRACKENN_FRONT_ORIGINS", f"{EXTRA_ORIGIN},*")

    assert app_module.front_origins() == [EXTRA_ORIGIN]


def test_front_origins_only_wildcard_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("TRACKENN_FRONT_ORIGINS", "*")

    assert app_module.front_origins() == [DEFAULT_ORIGIN]


def test_front_origins_strips_trailing_slash(monkeypatch):
    # INFO: Browsers never send a trailing slash in Origin, it would never match
    monkeypatch.setenv("TRACKENN_FRONT_ORIGINS", f"{EXTRA_ORIGIN}/")

    assert app_module.front_origins() == [EXTRA_ORIGIN]


def test_wildcard_in_env_does_not_open_cors(load_app, monkeypatch):
    module, dbname = load_app(f"{DEFAULT_ORIGIN},*")
    monkeypatch.setattr(module.misc, "event_stream_checking", lambda params: iter(["data:{}\n\n"]))

    with TestClient(module.app) as client:
        stream = client.get("/stream-data-checking", params={"action": "init"},
                            headers={"Origin": "http://evil.example"})
        reset = client.post("/reset_db", headers={"Origin": "http://evil.example"})

    assert "access-control-allow-origin" not in stream.headers
    assert reset.status_code == 403
    assert dbname.exists()


def test_configured_origin_gets_cors_and_can_reset(load_app):
    module, dbname = load_app(f"{DEFAULT_ORIGIN},{EXTRA_ORIGIN}")

    with TestClient(module.app) as client:
        response = client.post("/reset_db", headers={"Origin": EXTRA_ORIGIN})

    assert response.status_code == 200
    assert response.json() == {"msg": "Database deleted", "deleted": True, "error": False}
    assert response.headers["access-control-allow-origin"] == EXTRA_ORIGIN
    assert not dbname.exists()


def test_origin_left_out_of_the_env_is_rejected(load_app):
    module, dbname = load_app(EXTRA_ORIGIN)

    with TestClient(module.app) as client:
        response = client.post("/reset_db", headers={"Origin": DEFAULT_ORIGIN})

    assert response.status_code == 403
    assert response.json() == {"msg": "Origin not allowed", "deleted": False, "error": True}
    assert "access-control-allow-origin" not in response.headers
    assert dbname.exists()


def test_unset_env_keeps_previous_origins(load_app):
    module, dbname = load_app()

    with TestClient(module.app) as client:
        allowed = client.post("/reset_db", headers={"Origin": DEFAULT_ORIGIN})
        denied = client.post("/reset_db", headers={"Origin": EXTRA_ORIGIN})

    assert allowed.status_code == 200
    assert allowed.headers["access-control-allow-origin"] == DEFAULT_ORIGIN
    assert denied.status_code == 403
    assert "access-control-allow-origin" not in denied.headers
