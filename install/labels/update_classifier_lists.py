#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""Regenerate data/phishingDomains.json and data/tokenLists.json, the two public,
updatable lists the internal classifier (core/token_classifier.py) reads (R18,
odd/tasks/backend-backlog-c.md T3, S3; user decision L7: "Blacklist publica + TLDs
actuales" — the current 17 TLDs in core/token_classifier.py._URL_RE are kept as-is,
this only adds a domain blacklist and a token contract list).

Both downloads are pinned to a fixed revision and refuse any other one, same pattern
as install/labels/update_ofac_labels.py:

- data/phishingDomains.json: the `blacklist` array of
  https://github.com/MetaMask/eth-phishing-detect, src/config.json, fetched from
  raw.githubusercontent.com at PHISHING_REVISION (a commit SHA). raw.githubusercontent.com
  does not report which commit a branch/tag resolved to, so the file is instead fetched
  from the immutable /<revision>/ URL directly: there is nothing to serve but the pinned
  commit's content, or a 404.
- data/tokenLists.json: chainId 1 tokens from the Uniswap Labs default list
  (https://tokens.uniswap.org), which carries its own {major, minor, patch} version in the
  body. The GitHub source repo (Uniswap/default-token-list) is gone, so the list itself is
  the only pinnable artifact; the downloaded version is checked against TOKEN_LIST_REVISION
  and the script refuses anything else.

Usage: python install/labels/update_classifier_lists.py
"""

import json
import os

import requests

# INFO: pinned so a regeneration reproduces the reviewed data; bump it on purpose, with the
#       new revision checked, never by downloading whatever the branch holds today
PHISHING_REVISION = "261c8ca25d2eb3e3e85d4b2f0ab36bf6c7923be3"
PHISHING_SOURCE_URL = (
    "https://raw.githubusercontent.com/MetaMask/eth-phishing-detect/"
    f"{PHISHING_REVISION}/src/config.json"
)

TOKEN_LIST_REVISION = "22.34.0"
TOKEN_LIST_SOURCE_URL = "https://tokens.uniswap.org"
TOKEN_LIST_CHAIN_ID = 1

_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
PHISHING_OUTPUT_PATH = os.path.join(_DATA_DIR, "phishingDomains.json")
TOKEN_LIST_OUTPUT_PATH = os.path.join(_DATA_DIR, "tokenLists.json")


def _version_string(version):
    return f"{version['major']}.{version['minor']}.{version['patch']}"


def download_phishing_blacklist():
    """The `blacklist` array at the pinned revision. The URL itself names the revision
    (raw.githubusercontent.com serves the file for that exact commit or a 404), so there is
    no separate header to check; anything other than 200 stops the script."""
    response = requests.get(PHISHING_SOURCE_URL, timeout=30)
    if response.status_code != 200:
        raise SystemExit(
            f"Phishing blacklist download failed: expected revision {PHISHING_REVISION}, "
            f"got HTTP {response.status_code}")
    return response.json()["blacklist"]


def download_token_list():
    """The Uniswap Labs default token list, refusing any version other than
    TOKEN_LIST_REVISION."""
    response = requests.get(TOKEN_LIST_SOURCE_URL, timeout=30)
    response.raise_for_status()
    body = response.json()
    served = _version_string(body["version"])
    if served != TOKEN_LIST_REVISION:
        raise SystemExit(f"Token list revision mismatch: expected {TOKEN_LIST_REVISION}, got {served}")
    return body["tokens"]


def build_phishing_domains():
    domains = sorted({domain.lower() for domain in download_phishing_blacklist()})
    return {"source": PHISHING_SOURCE_URL, "revision": PHISHING_REVISION, "domains": domains}


def build_token_lists():
    tokens = [
        {"address": token["address"].lower(), "symbol": token["symbol"], "name": token["name"]}
        for token in download_token_list()
        if token.get("chainId") == TOKEN_LIST_CHAIN_ID
    ]
    tokens.sort(key=lambda token: token["address"])
    return {"source": TOKEN_LIST_SOURCE_URL, "revision": TOKEN_LIST_REVISION, "tokens": tokens}


def _write(path, data):
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2, sort_keys=True)
            f.write("\n")
    except OSError as e:
        raise SystemExit(f"Could not write {path}: {e}") from e


def main():
    phishing = build_phishing_domains()
    _write(PHISHING_OUTPUT_PATH, phishing)
    print(f"Wrote {len(phishing['domains'])} domains to {PHISHING_OUTPUT_PATH}")

    token_lists = build_token_lists()
    _write(TOKEN_LIST_OUTPUT_PATH, token_lists)
    print(f"Wrote {len(token_lists['tokens'])} tokens to {TOKEN_LIST_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
