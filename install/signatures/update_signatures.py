#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""Regenerate data/functionSignatures.json, the pinned function-signature table GET /calls
(R12, odd/tasks/backend-backlog-c.md T12, S12) uses to name a selector when neither
Etherscan's stored functionName nor a fetched ABI resolves it.

The source is 4byte.directory (https://www.4byte.directory/api/v1/signatures/), a public,
crowd-submitted database: there is no single reviewable revision to pin (unlike
install/labels/update_ofac_labels.py's Hugging Face dataset or
install/labels/update_classifier_lists.py's GitHub commit / Uniswap list version), so the
source URL and the generation date are recorded instead, same spirit as the other two
scripts' {source, revision} header.

Two selector sets are looked up:
- COMMON_SELECTORS below: the handful of signatures any EVM chain decoder needs (ERC-20,
  WETH, Uniswap V2/V3 routers, multicall, ERC-721, permit), hardcoded with their known text
  signature in a comment instead of computed, since keccak is not otherwise needed by this
  codebase and pulling in a hashing dependency for it is not worth it.
- every distinct 4-byte selector found in the `input` column of t_transactions in the
  SQLite databases passed with --db (repeatable), opened read-only so the script never
  writes to them.

Selectors 4byte has no signature for are omitted from the output; a missing key means
unknown, there is no empty-list placeholder. Pagination (`next`) is followed for every
selector in case 4byte knows several signatures for it (GET /calls lists all of them as
candidates when it cannot pick one, S12 decision selector_ambiguous).

A network failure raises SystemExit with the selector that failed; the existing
data/functionSignatures.json is left untouched (the write only happens after every lookup
succeeds).

Usage: python install/signatures/update_signatures.py [--db PATH ...]

No network access happens at app runtime: core code only reads the generated JSON file.
"""

import argparse
import datetime
import json
import os
import sqlite3
import time

import requests

SOURCE = "https://www.4byte.directory/api/v1/signatures/"
SIGNATURES_URL = SOURCE + "?hex_signature={selector}"
REQUEST_DELAY_SECONDS = 0.3

OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "functionSignatures.json",
)

# INFO: hardcoded instead of computed, each with the signature it is keccak256(...)[:4] of;
#       no keccak implementation is otherwise needed by this codebase
COMMON_SELECTORS = {
    "0xa9059cbb",  # transfer(address,uint256)
    "0x23b872dd",  # transferFrom(address,address,uint256)
    "0x095ea7b3",  # approve(address,uint256)
    "0xd0e30db0",  # deposit() -- WETH
    "0x2e1a7d4d",  # withdraw(uint256) -- WETH
    "0x38ed1739",  # swapExactTokensForTokens(uint256,uint256,address[],address,uint256) -- Uniswap V2
    "0x8803dbee",  # swapTokensForExactTokens(uint256,uint256,address[],address,uint256) -- Uniswap V2
    "0x7ff36ab5",  # swapExactETHForTokens(uint256,address[],address,uint256) -- Uniswap V2
    "0x18cbafe5",  # swapExactTokensForETH(uint256,uint256,address[],address,uint256) -- Uniswap V2
    "0xfb3bdb41",  # swapETHForExactTokens(uint256,address[],address,uint256) -- Uniswap V2
    "0x414bf389",  # exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160)) -- Uniswap V3
    "0xc04b8d59",  # exactInput((bytes,address,uint256,uint256,uint256)) -- Uniswap V3
    "0xdb3e2198",  # exactOutputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160)) -- Uniswap V3
    "0xf28c0498",  # exactOutput((bytes,address,uint256,uint256,uint256)) -- Uniswap V3
    "0xac9650d8",  # multicall(bytes[])
    "0x5ae401dc",  # multicall(uint256,bytes[])
    "0x42842e0e",  # safeTransferFrom(address,address,uint256) -- ERC-721
    "0xb88d4fde",  # safeTransferFrom(address,address,uint256,bytes) -- ERC-721
    "0xa22cb465",  # setApprovalForAll(address,bool)
    "0xd505accf",  # permit(address,address,uint256,uint256,uint8,bytes32,bytes32)
}


def selectors_from_db(path):
    """Every distinct lowercase 4-byte selector in t_transactions.input, opened read-only."""
    uri = f"file:{path}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        cursor = connection.execute("SELECT input FROM t_transactions")
        return {
            row[0][:10].lower()
            for row in cursor
            if row[0] and len(row[0]) >= 10
        }
    finally:
        connection.close()


def fetch_signatures(selector):
    """Every distinct text_signature 4byte.directory has for `selector`, following
    pagination. Raises SystemExit with the failing selector on any network error."""
    signatures = []
    url = SIGNATURES_URL.format(selector=selector)
    while url:
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            body = response.json()
        except requests.RequestException as e:
            raise SystemExit(f"4byte.directory lookup failed for {selector}: {e}") from e
        signatures.extend(result["text_signature"] for result in body["results"])
        url = body.get("next")
        if url:
            time.sleep(REQUEST_DELAY_SECONDS)
    return signatures


def build_signatures(selectors):
    table = {}
    for selector in sorted(selector.lower() for selector in selectors):
        found = fetch_signatures(selector)
        if found:
            table[selector] = sorted(set(found))
        time.sleep(REQUEST_DELAY_SECONDS)
    return table


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db", action="append", default=[], dest="dbs",
        help="SQLite DB to scan for extra selectors (repeatable)")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    selectors = set(COMMON_SELECTORS)
    for db_path in args.dbs:
        selectors |= selectors_from_db(db_path)

    signatures = build_signatures(selectors)

    output = {
        "source": SOURCE,
        "generated_at": datetime.date.today().isoformat(),
        "signatures": signatures,
    }
    try:
        with open(OUTPUT_PATH, "w") as f:
            json.dump(output, f, indent=2, sort_keys=True)
            f.write("\n")
    except OSError as e:
        raise SystemExit(f"Could not write {OUTPUT_PATH}: {e}") from e

    unknown = sorted(selectors - set(signatures))
    print(f"Resolved {len(signatures)} of {len(selectors)} selectors to {OUTPUT_PATH}")
    if unknown:
        print(f"Unknown: {', '.join(unknown)}")


if __name__ == "__main__":
    main()
