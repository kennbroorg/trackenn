"""R20 (odd/tasks/backend-backlog-c.md, T1, S1): the OFAC labels script downloads the pinned
dataset revision, refuses any other one with a clear message, and keeps only ETH rows."""
import importlib.util
import json
import os

import pandas as pd
import pytest

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "install", "labels", "update_ofac_labels.py")


@pytest.fixture
def script():
    spec = importlib.util.spec_from_file_location("update_ofac_labels", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    def __init__(self, commit=None, content=b"", location=None):
        self.headers = {}
        if commit:
            self.headers["X-Repo-Commit"] = commit
        if location:
            self.headers["Location"] = location
        self.is_redirect = location is not None
        self.content = content

    def raise_for_status(self):
        pass


def hugging_face(commit, content=b"parquet"):
    """Fake requests.get as Hugging Face answers: a 302 carrying X-Repo-Commit, then the file
    from storage without that header."""
    calls = []

    def get(url, timeout, allow_redirects=True):
        calls.append((url, allow_redirects))
        if url == "https://storage.example/file":
            return FakeResponse(content=content)
        return FakeResponse(commit=commit, location="https://storage.example/file")

    return get, calls


def test_url_is_pinned_to_the_documented_revision(script):
    assert script.REVISION == "72105dc742ce7891b11ee138a5defd9e498c1d99"
    assert f"/resolve/{script.REVISION}/" in script.DATASET_PARQUET_URL
    assert "/resolve/main/" not in script.DATASET_PARQUET_URL


def test_different_revision_fails_with_a_clear_message(script, monkeypatch):
    get, calls = hugging_face("deadbeef")
    monkeypatch.setattr(script.requests, "get", get)

    with pytest.raises(SystemExit) as error:
        script.download_parquet()

    assert str(error.value) == (
        "Dataset revision mismatch: expected 72105dc742ce7891b11ee138a5defd9e498c1d99, got deadbeef")
    assert len(calls) == 1  # the file is never downloaded


def test_missing_revision_header_fails_too(script, monkeypatch):
    get, _ = hugging_face(None)
    monkeypatch.setattr(script.requests, "get", get)

    with pytest.raises(SystemExit) as error:
        script.download_parquet()

    assert "expected 72105dc742ce7891b11ee138a5defd9e498c1d99, got none" in str(error.value)


def test_pinned_revision_returns_the_bytes(script, monkeypatch):
    get, calls = hugging_face(script.REVISION, b"abc")
    monkeypatch.setattr(script.requests, "get", get)

    assert script.download_parquet() == b"abc"
    assert calls[0] == (script.DATASET_PARQUET_URL, False)  # header read before redirecting
    assert calls[1][0] == "https://storage.example/file"


def test_rows_to_labels_keeps_eth_lowercases_and_keeps_the_shape(script):
    df = pd.DataFrame([
        {"currency": "ETH", "address": "0xABCdef0000000000000000000000000000000001", "label": "LAZARUS GROUP"},
        {"currency": "BTC", "address": "1BoatSLRHtKNngkdXEeobR76b53LETtpyT", "label": "SOMEONE"},
        {"currency": "ETH", "address": "0x00000000000000000000000000000000000000Ff", "label": "TORNADO CASH"},
    ])

    labels = script.rows_to_labels(df)

    assert labels == {
        "0xabcdef0000000000000000000000000000000001": {
            "name": "LAZARUS GROUP", "labels": ["ofac-sanctions-lists", "ofac-sdn"]},
        "0x00000000000000000000000000000000000000ff": {
            "name": "TORNADO CASH", "labels": ["ofac-sanctions-lists", "ofac-sdn"]},
    }
    json.dumps(labels)  # serializable as the data/ file


def test_committed_data_file_has_the_same_shape(script):
    with open(script.OUTPUT_PATH) as f:
        data = json.load(f)

    assert data
    for address, entry in data.items():
        assert address == address.lower()
        assert set(entry) == {"name", "labels"}
        assert entry["labels"] == ["ofac-sanctions-lists", "ofac-sdn"]
