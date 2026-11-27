"""R12 (odd/tasks/backend-backlog-c.md, T12, S12): the signature table script pages through
4byte.directory, normalizes the result, pulls extra selectors from local DBs, and never
touches the existing data/functionSignatures.json on failure."""
import importlib.util
import json
import os
import sqlite3

import pytest

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "install", "signatures", "update_signatures.py")
SIGNATURES_URL_PREFIX = "https://www.4byte.directory/api/v1/signatures/?hex_signature="


@pytest.fixture
def script():
    spec = importlib.util.spec_from_file_location("update_signatures", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    def __init__(self, body=None, status=200):
        self._body = body
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise script_requests_error(self.status_code)

    def json(self):
        return self._body


def script_requests_error(status):
    import requests
    return requests.HTTPError(f"{status}")


def fake_4byte(pages):
    """pages: {selector: [page_body, page_body, ...]} consumed in order; a selector not in
    the dict means 4byte has nothing for it (one empty page). A page's own `next` value is
    used verbatim as the next request's URL, so it is also a valid key into `pages`."""
    calls = []
    queues = {selector: list(queue) for selector, queue in pages.items()}
    aliases = {}  # next-page URL -> selector, filled in as pages are served

    def get(url, timeout):
        calls.append(url)
        if url.startswith(SIGNATURES_URL_PREFIX):
            key = url[len(SIGNATURES_URL_PREFIX):]
        else:
            key = aliases.get(url, url)
        queue = queues.get(key)
        if not queue:
            return FakeResponse({"results": [], "next": None})
        page = queue.pop(0)
        if page.get("next"):
            aliases[page["next"]] = key
        return FakeResponse(page)

    return get, calls


def make_db(path, inputs):
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE t_transactions (input text NOT NULL)")
    connection.executemany(
        "INSERT INTO t_transactions (input) VALUES (?)", [(i,) for i in inputs])
    connection.commit()
    connection.close()


def test_fetch_signatures_follows_pagination(script, monkeypatch):
    pages = {
        "0xa9059cbb": [
            {"results": [{"text_signature": "transfer(address,uint256)"}], "next": "page2"},
            {"results": [{"text_signature": "another(uint256)"}], "next": None},
        ]
    }
    get, calls = fake_4byte(pages)
    monkeypatch.setattr(script.requests, "get", get)
    monkeypatch.setattr(script.time, "sleep", lambda *_: None)

    signatures = script.fetch_signatures("0xa9059cbb")

    assert signatures == ["transfer(address,uint256)", "another(uint256)"]
    assert calls[1] == "page2"


def test_build_signatures_lowercases_sorts_and_dedupes(script, monkeypatch):
    pages = {
        "0xabcdef01": [{"results": [
            {"text_signature": "b()"}, {"text_signature": "a()"}, {"text_signature": "a()"},
        ], "next": None}],
    }
    get, _ = fake_4byte(pages)
    monkeypatch.setattr(script.requests, "get", get)
    monkeypatch.setattr(script.time, "sleep", lambda *_: None)

    table = script.build_signatures({"0xABCDEF01"})

    assert table == {"0xabcdef01": ["a()", "b()"]}


def test_build_signatures_omits_unresolved_selectors(script, monkeypatch):
    get, _ = fake_4byte({})
    monkeypatch.setattr(script.requests, "get", get)
    monkeypatch.setattr(script.time, "sleep", lambda *_: None)

    table = script.build_signatures({"0xdeadbeef"})

    assert table == {}


def test_selectors_from_db_skips_short_inputs(script, tmp_path):
    db_path = tmp_path / "test.db"
    make_db(str(db_path), ["0xa9059cbb000000", "0x1234", "", "0x095ea7b3abc"])

    selectors = script.selectors_from_db(str(db_path))

    assert selectors == {"0xa9059cbb", "0x095ea7b3"}


def test_network_failure_leaves_existing_file_untouched(script, monkeypatch, tmp_path):
    output_path = tmp_path / "functionSignatures.json"
    output_path.write_text('{"source": "old", "generated_at": "2020-01-01", "signatures": {}}')
    monkeypatch.setattr(script, "OUTPUT_PATH", str(output_path))

    def failing_get(url, timeout):
        import requests
        raise requests.ConnectionError("boom")

    monkeypatch.setattr(script.requests, "get", failing_get)
    monkeypatch.setattr(script.time, "sleep", lambda *_: None)
    monkeypatch.setattr(script, "COMMON_SELECTORS", {"0xa9059cbb"})

    with pytest.raises(SystemExit):
        script.main([])

    assert json.loads(output_path.read_text()) == {
        "source": "old", "generated_at": "2020-01-01", "signatures": {}}


def test_main_writes_the_documented_shape(script, monkeypatch, tmp_path):
    output_path = tmp_path / "functionSignatures.json"
    db_path = tmp_path / "local.db"
    make_db(str(db_path), ["0xdeadbeef1234"])

    pages = {
        "0xa9059cbb": [{"results": [{"text_signature": "transfer(address,uint256)"}], "next": None}],
    }
    get, _ = fake_4byte(pages)
    monkeypatch.setattr(script.requests, "get", get)
    monkeypatch.setattr(script.time, "sleep", lambda *_: None)
    monkeypatch.setattr(script, "OUTPUT_PATH", str(output_path))
    monkeypatch.setattr(script, "COMMON_SELECTORS", {"0xa9059cbb"})

    script.main(["--db", str(db_path)])

    data = json.loads(output_path.read_text())
    assert data["source"] == script.SOURCE
    assert data["signatures"] == {"0xa9059cbb": ["transfer(address,uint256)"]}
    assert "0xdeadbeef" not in data["signatures"]  # unresolved selector omitted
