"""R18 (odd/tasks/backend-backlog-c.md, T3, S3): public, updatable classifier lists
snapshotted in data/ instead of hardcoded (user decision L7: keep the current 17 TLDs
and lure words, add a domain blacklist and a token contract list as pinned snapshots).
Offline: the data files are faked via monkeypatch, no network access."""
import importlib.util
import json
import sqlite3
from contextlib import closing

import pytest

from core import eth, token_classifier
from test_token_classifier import CENTRAL, insert_transfer, stored_tags, transfer

BLACKLISTED_DOMAIN = "spoof-synthetic-domain.cfd"  # a TLD not in token_classifier._URL_RE
WIZARD = "0xc1000000000000000000000000000000000c1d"
LEGIT = "0xc2000000000000000000000000000000000c2e"
SUBDOMAIN_CONTRACT = "0xc3000000000000000000000000000000000c3f"
TOKEN_LIST_CONTRACT = "0xb1000000000000000000000000000000000b1d"
VICTIM = "0xd1000000000000000000000000000000000d1e"
FRIEND = "0xf0000000000000000000000000000000000f0e"


@pytest.fixture(autouse=True)
def clear_cache():
    """Each test controls the data file state via monkeypatch, so the module-level lazy
    cache (R18-4: load once, no network/disk access at runtime beyond that) must not leak
    results from one test into the next."""
    token_classifier._cache.clear()
    token_classifier._load_problems.clear()
    yield
    token_classifier._cache.clear()
    token_classifier._load_problems.clear()


def fake_phishing_domains(monkeypatch, tmp_path, domains):
    path = tmp_path / "phishingDomains.json"
    path.write_text(json.dumps({"source": "test", "revision": "test", "domains": sorted(domains)}))
    monkeypatch.setattr(token_classifier, "PHISHING_DOMAINS_PATH", str(path))
    return path


def fake_token_lists(monkeypatch, tmp_path, tokens):
    path = tmp_path / "tokenLists.json"
    path.write_text(json.dumps({"source": "test", "revision": "test", "tokens": tokens}))
    monkeypatch.setattr(token_classifier, "TOKEN_LISTS_PATH", str(path))
    return path


# ---- Blacklist-driven phishing detection (items 2, 5) ----

def test_token_with_blacklisted_domain_not_in_the_17_tlds_is_tagged_phishing(monkeypatch, tmp_path):
    fake_phishing_domains(monkeypatch, tmp_path, {BLACKLISTED_DOMAIN})

    assert token_classifier._URL_RE.search(BLACKLISTED_DOMAIN) is None  # not one of the 17 TLDs
    assert token_classifier.is_phishing_text(f"WizardCoin at {BLACKLISTED_DOMAIN}", "WIZ") is True

    transfers = [transfer(WIZARD, FRIEND, CENTRAL, f"WizardCoin at {BLACKLISTED_DOMAIN}", "WIZ", 1)]
    triggers = token_classifier.find_phishing_triggers(transfers)

    assert triggers[WIZARD] == BLACKLISTED_DOMAIN


def test_subdomain_of_a_blacklisted_domain_matches(monkeypatch, tmp_path):
    fake_phishing_domains(monkeypatch, tmp_path, {BLACKLISTED_DOMAIN})

    subdomain_text = f"SubCoin at app.{BLACKLISTED_DOMAIN}"
    assert token_classifier.is_phishing_text(subdomain_text, "SUB") is True

    transfers = [transfer(SUBDOMAIN_CONTRACT, FRIEND, CENTRAL, subdomain_text, "SUB", 1)]
    triggers = token_classifier.find_phishing_triggers(transfers)

    assert triggers[SUBDOMAIN_CONTRACT] == f"app.{BLACKLISTED_DOMAIN}"


def test_legit_name_with_no_url_is_not_tagged(monkeypatch, tmp_path):
    fake_phishing_domains(monkeypatch, tmp_path, {BLACKLISTED_DOMAIN})

    assert token_classifier.is_phishing_text("Legit Token", "LEGIT") is False

    transfers = [transfer(LEGIT, FRIEND, CENTRAL, "Legit Token", "LEGIT", 1)]
    assert token_classifier.find_phishing_contracts(transfers) == set()


def test_blacklist_wired_into_db_store_tagging_opt(monkeypatch, tmp_path, db):
    fake_phishing_domains(monkeypatch, tmp_path, {BLACKLISTED_DOMAIN})
    t = transfer(WIZARD, FRIEND, CENTRAL, f"Visit {BLACKLISTED_DOMAIN}", "WIZ", 1)
    insert_transfer(db, t)

    with closing(sqlite3.connect(db)) as conn:
        eth.db_store_tagging_opt(conn, CENTRAL, [], [t], [], [], [])
        conn.commit()

    assert ("spam-phishing",) in stored_tags(db, WIZARD)


# ---- Token-list-driven spoofed-sender (items 3, 5) ----

def test_token_list_contract_as_sender_makes_spoofed_sender_fire(monkeypatch, tmp_path):
    fake_token_lists(monkeypatch, tmp_path, [
        {"address": TOKEN_LIST_CONTRACT, "symbol": "TLT", "name": "Token List Token"},
    ])

    transfers = [transfer(VICTIM, TOKEN_LIST_CONTRACT, CENTRAL, "Victim Token", "VIC", 1)]
    triggers = token_classifier.find_spoofed_sender_triggers(transfers)

    assert triggers[VICTIM] == TOKEN_LIST_CONTRACT


def test_token_list_wired_into_db_store_tagging_opt(monkeypatch, tmp_path, db):
    fake_token_lists(monkeypatch, tmp_path, [
        {"address": TOKEN_LIST_CONTRACT, "symbol": "TLT", "name": "Token List Token"},
    ])
    t = transfer(VICTIM, TOKEN_LIST_CONTRACT, CENTRAL, "Victim Token", "VIC", 1)
    insert_transfer(db, t)

    with closing(sqlite3.connect(db)) as conn:
        eth.db_store_tagging_opt(conn, CENTRAL, [], [t], [], [], [])
        conn.commit()

    assert ("spam-spoofed-sender",) in stored_tags(db, VICTIM)


# ---- R18-5: missing/broken data file -> baseline behaviour + evidence ----

def test_missing_phishing_domains_file_falls_back_to_baseline_and_reports_the_problem(monkeypatch, tmp_path):
    missing_path = tmp_path / "does-not-exist.json"
    monkeypatch.setattr(token_classifier, "PHISHING_DOMAINS_PATH", str(missing_path))

    # Baseline: unchanged URL/lure check still fires, no blacklist available
    assert token_classifier.is_phishing_text("Visit https://evil.com now", "EVIL") is True
    assert token_classifier.is_phishing_text(f"WizardCoin at {BLACKLISTED_DOMAIN}", "WIZ") is False

    problems = token_classifier.list_load_problems()
    assert {"file": "does-not-exist.json", "reason": problems[0]["reason"]} in problems


def test_broken_token_lists_file_falls_back_to_baseline_and_reports_the_problem(monkeypatch, tmp_path):
    broken_path = tmp_path / "tokenLists.json"
    broken_path.write_text("{not valid json")
    monkeypatch.setattr(token_classifier, "TOKEN_LISTS_PATH", str(broken_path))

    known = token_classifier._known_token_contracts()
    assert known == token_classifier._FALLBACK_BASELINE_TOKEN_CONTRACTS

    problems = token_classifier.list_load_problems()
    assert [p["file"] for p in problems] == ["tokenLists.json"]


def test_missing_data_file_persists_one_cache_unavailable_decision(monkeypatch, tmp_path, db):
    missing_path = tmp_path / "does-not-exist.json"
    monkeypatch.setattr(token_classifier, "PHISHING_DOMAINS_PATH", str(missing_path))
    t = transfer(LEGIT, FRIEND, CENTRAL, "Legit Token", "LEGIT", 1)
    insert_transfer(db, t)

    with closing(sqlite3.connect(db)) as conn:
        eth.db_store_tagging_opt(conn, CENTRAL, [], [t], [], [], [])
        conn.commit()
        rows = conn.execute(
            "SELECT code, scope, rule FROM t_decisions WHERE code = 'cache_unavailable'").fetchall()

    assert rows == [("cache_unavailable", CENTRAL, "does-not-exist.json")]


def test_missing_data_file_persists_the_decision_only_once(monkeypatch, tmp_path, db):
    missing_path = tmp_path / "does-not-exist.json"
    monkeypatch.setattr(token_classifier, "PHISHING_DOMAINS_PATH", str(missing_path))
    t = transfer(LEGIT, FRIEND, CENTRAL, "Legit Token", "LEGIT", 1)
    insert_transfer(db, t)

    with closing(sqlite3.connect(db)) as conn:
        eth.db_store_tagging_opt(conn, CENTRAL, [], [t], [], [], [])
        conn.commit()
        eth.apply_classifier_tags(conn, CENTRAL, [t], [])
        conn.commit()
        rows = conn.execute(
            "SELECT code FROM t_decisions WHERE code = 'cache_unavailable'").fetchall()

    assert rows == [("cache_unavailable",)]


# ---- The update script refuses an unpinned revision (S1-style guard) ----

SCRIPT_PATH = "install/labels/update_classifier_lists.py"


@pytest.fixture
def script():
    spec = importlib.util.spec_from_file_location("update_classifier_lists", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    def __init__(self, status_code=200, json_body=None):
        self.status_code = status_code
        self._json_body = json_body or {}

    def json(self):
        return self._json_body

    def raise_for_status(self):
        if self.status_code != 200:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_phishing_download_refuses_a_non_200_status(script, monkeypatch):
    monkeypatch.setattr(script.requests, "get", lambda *a, **k: FakeResponse(status_code=404))

    with pytest.raises(SystemExit) as error:
        script.download_phishing_blacklist()

    assert str(script.PHISHING_REVISION) in str(error.value)


def test_token_list_download_refuses_a_different_revision(script, monkeypatch):
    other_version = {"major": 0, "minor": 0, "patch": 1}
    monkeypatch.setattr(
        script.requests, "get",
        lambda *a, **k: FakeResponse(json_body={"version": other_version, "tokens": []}))

    with pytest.raises(SystemExit) as error:
        script.download_token_list()

    assert str(error.value) == (
        f"Token list revision mismatch: expected {script.TOKEN_LIST_REVISION}, got 0.0.1")


def test_pinned_token_list_revision_is_accepted(script, monkeypatch):
    pinned_version = {"major": 22, "minor": 34, "patch": 0}
    tokens = [{"chainId": 1, "address": "0xAbC", "symbol": "X", "name": "X token"}]
    monkeypatch.setattr(
        script.requests, "get",
        lambda *a, **k: FakeResponse(json_body={"version": pinned_version, "tokens": tokens}))

    assert script.download_token_list() == tokens


def test_build_token_lists_keeps_only_chain_id_1_and_lowercases_addresses(script, monkeypatch):
    monkeypatch.setattr(script, "download_token_list", lambda: [
        {"chainId": 1, "address": "0xABC", "symbol": "A", "name": "Mainnet token"},
        {"chainId": 137, "address": "0xDEF", "symbol": "B", "name": "Polygon token"},
    ])

    result = script.build_token_lists()

    assert result["tokens"] == [{"address": "0xabc", "symbol": "A", "name": "Mainnet token"}]
    assert result["revision"] == script.TOKEN_LIST_REVISION


def test_build_phishing_domains_lowercases_and_sorts(script, monkeypatch):
    monkeypatch.setattr(script, "download_phishing_blacklist", lambda: ["Zeta.com", "alpha.io", "alpha.io"])

    result = script.build_phishing_domains()

    assert result["domains"] == ["alpha.io", "zeta.com"]
    assert result["revision"] == script.PHISHING_REVISION


# ---- Committed snapshot files have the documented shape ----

def test_committed_phishing_domains_file_has_the_documented_shape():
    with open("data/phishingDomains.json") as f:
        data = json.load(f)

    assert set(data) == {"source", "revision", "domains"}
    assert data["domains"] == sorted(data["domains"])
    assert all(domain == domain.lower() for domain in data["domains"])


def test_committed_token_lists_file_has_the_documented_shape():
    with open("data/tokenLists.json") as f:
        data = json.load(f)

    assert set(data) == {"source", "revision", "tokens"}
    for token in data["tokens"]:
        assert set(token) == {"address", "symbol", "name"}
        assert token["address"] == token["address"].lower()


# ---- Known-label exclusion for blacklist hits (user decision after the live check) ----

def fake_known_labels(monkeypatch, tmp_path, labels):
    path = tmp_path / "etherscanCombinedAllLabels.json"
    path.write_text(json.dumps(labels))
    monkeypatch.setattr(token_classifier, "KNOWN_LABELS_PATH", str(path))
    return path


def test_blacklist_hit_on_a_known_labelled_contract_is_not_tagged(monkeypatch, tmp_path):
    fake_phishing_domains(monkeypatch, tmp_path, {BLACKLISTED_DOMAIN})
    fake_known_labels(monkeypatch, tmp_path, {WIZARD: {"name": "Wizard: Voucher Token", "labels": ["wizard"]}})
    transfers = [transfer(WIZARD, FRIEND, CENTRAL, f"Wizard.{BLACKLISTED_DOMAIN.split('.')[-1]} Voucher", "VWIZ", 1),
                 transfer(SUBDOMAIN_CONTRACT, FRIEND, CENTRAL, f"WizardCoin at {BLACKLISTED_DOMAIN}", "WIZ", 2)]
    transfers[0]["tokenName"] = f"WizardCoin at {BLACKLISTED_DOMAIN}"

    phishing = token_classifier.find_phishing_contracts(transfers)

    assert WIZARD not in phishing            # known project token: blacklist does not apply
    assert SUBDOMAIN_CONTRACT in phishing    # same text, unknown contract: still tagged
    assert WIZARD not in token_classifier.find_phishing_triggers(transfers)


def test_known_label_never_shields_a_url_or_lure_word_match(monkeypatch, tmp_path):
    fake_phishing_domains(monkeypatch, tmp_path, set())
    fake_known_labels(monkeypatch, tmp_path, {WIZARD: {"name": "Wizard", "labels": ["wizard"]}})
    transfers = [transfer(WIZARD, FRIEND, CENTRAL, "Claim at https://evil.com", "EVIL", 1)]

    assert WIZARD in token_classifier.find_phishing_contracts(transfers)


@pytest.mark.parametrize("risky", [["blocked"], ["spam-token"], ["phish-hack"]])
def test_risky_labels_do_not_count_as_known(monkeypatch, tmp_path, risky):
    fake_phishing_domains(monkeypatch, tmp_path, {BLACKLISTED_DOMAIN})
    fake_known_labels(monkeypatch, tmp_path, {WIZARD: {"name": "Fake_Phishing123", "labels": risky}})
    transfers = [transfer(WIZARD, FRIEND, CENTRAL, f"WizardCoin at {BLACKLISTED_DOMAIN}", "WIZ", 1)]

    assert WIZARD in token_classifier.find_phishing_contracts(transfers)


def test_missing_labels_file_keeps_blacklist_and_reports_problem(monkeypatch, tmp_path):
    fake_phishing_domains(monkeypatch, tmp_path, {BLACKLISTED_DOMAIN})
    monkeypatch.setattr(token_classifier, "KNOWN_LABELS_PATH", str(tmp_path / "missing.json"))
    transfers = [transfer(WIZARD, FRIEND, CENTRAL, f"WizardCoin at {BLACKLISTED_DOMAIN}", "WIZ", 1)]

    assert WIZARD in token_classifier.find_phishing_contracts(transfers)
    assert [p["file"] for p in token_classifier.list_load_problems()] == ["missing.json"]
