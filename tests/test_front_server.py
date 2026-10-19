import logging
import os

import app as app_module
from core import misc


class FakeTCPServer:
    # INFO: Mirrors tests/test_security.py's FakeTCPServer, local to keep this file self-contained
    address = None
    handler = None

    def __init__(self, address, handler):
        FakeTCPServer.address = address
        FakeTCPServer.handler = handler

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def serve_forever(self):
        pass


def test_handler_serves_the_front_from_front_dir_not_the_cwd(monkeypatch, tmp_path):
    # INFO: front_dir() is cwd-independent (code dir in source runs, executable dir when frozen)
    monkeypatch.chdir(tmp_path)
    captured = {}

    def fake_init(self, *args, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(app_module.http.server.SimpleHTTPRequestHandler, "__init__", fake_init)

    app_module.Handler(None, None, None)

    assert captured["directory"] == misc.front_dir()
    assert captured["directory"] != os.path.join(os.getcwd(), "front")


def test_http_server_logs_a_clear_error_and_returns_when_front_is_missing(monkeypatch, tmp_path, caplog):
    missing_front = tmp_path / "front"
    monkeypatch.setattr(misc, "front_dir", lambda: str(missing_front))
    monkeypatch.setattr(app_module.socketserver, "TCPServer", FakeTCPServer)
    FakeTCPServer.address = None
    caplog.set_level(logging.ERROR)

    app_module.httpServer()  # must not raise; the API process keeps running independently

    assert FakeTCPServer.address is None  # the port was never bound
    assert "just front" in caplog.text
    assert str(missing_front) in caplog.text


def test_http_server_serves_when_front_has_index_html(monkeypatch, tmp_path):
    front = tmp_path / "front"
    front.mkdir()
    (front / "index.html").write_text("<html></html>")
    monkeypatch.setattr(misc, "front_dir", lambda: str(front))
    monkeypatch.setattr(app_module.socketserver, "TCPServer", FakeTCPServer)

    app_module.httpServer()

    assert FakeTCPServer.address == ("127.0.0.1", 4200)
