#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""List the contracts an address created, directly and recursively through the
contracts it created in turn (R11, S1-S3). Pure walk logic: callers inject how each
level's internal creations are fetched, so tests never hit the network.

Level 1 is read from data already stored by the normal collection flow (t_transactions,
t_internals): the address's own creations. Level N+1 is the internal create/create2 rows
of each level-N contract, one `txlistinternal` per contract (cost grows with the width of
each level, not with the whole history)."""

import sqlite3
from contextlib import closing

from core import api_client

# INFO: 3 hops reaches a liquidator from the attacker in the Euler case (address ->
# orchestrator -> violator -> liquidator, see documents/EULER-RESEARCH.md). Generalizing
# further would need one API call per contract found at each extra level, so this is a
# deliberate ceiling, not a number derived from any single case.
MAX_DEPTH = 3

# INFO: A factory contract (e.g. Uniswap V2 Factory) creates one contract per call
# forever; without a hard total cap a single popular address would expand without bound
# (documents/VALIDACION.md, S3 control for R11 cost).
MAX_CONTRACTS = 50

CREATE_TYPES = ("create", "create2")


def level1_from_db(dbname, address, blockchain="eth"):
    """Stored direct creations of address (S1): normal transactions with empty `to` and
    a `contractAddress`, plus internal create/create2 rows whose `from` is address."""
    address = address.lower()
    rows = []
    try:
        with closing(sqlite3.connect(f"file:{dbname}?mode=ro", uri=True)) as connection:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT hash, timeStamp, contractAddress FROM t_transactions "
                "WHERE blockChain = ? AND lower(`from`) = ? AND `to` = '' AND contractAddress != ''",
                (blockchain, address),
            )
            for trx_hash, timestamp, contract_address in cursor.fetchall():
                rows.append({"hash": trx_hash, "timeStamp": timestamp, "contractAddress": contract_address, "type": "create"})

            cursor.execute(
                "SELECT hash, timeStamp, contractAddress, type FROM t_internals "
                "WHERE blockChain = ? AND lower(`from`) = ? AND type IN ('create', 'create2') AND contractAddress != ''",
                (blockchain, address),
            )
            for trx_hash, timestamp, contract_address, create_type in cursor.fetchall():
                rows.append({"hash": trx_hash, "timeStamp": timestamp, "contractAddress": contract_address, "type": create_type})
    except sqlite3.OperationalError:
        return []
    return rows


def _expand(contract_address, chainid, key, fetch_all_pages):
    """Internal create/create2 rows made BY contract_address, the next level's creations
    (S1). txlistinternal(contract_address) returns every internal row touching it,
    including the one that created it (`to` = contract_address); only `from` =
    contract_address rows are its own creations."""
    rows, _ = fetch_all_pages(chainid, "txlistinternal", contract_address, key)
    return [
        row for row in rows
        if row.get("type") in CREATE_TYPES and row.get("contractAddress")
        and row.get("from", "").lower() == contract_address.lower()
    ]


def find_created_contracts(address, level1_rows, chainid, key, fetch_all_pages=None):
    """Walk the creation hierarchy starting from address's level-1 rows (S1), deduplicating
    by contract address while keeping every creation transaction (S3), down to MAX_DEPTH and
    up to MAX_CONTRACTS contracts total (S2). Returns (contracts, truncated, limit).

    The contracts limit is enforced the moment a new contract would exceed it: no API call
    is made for it, and the ones already queued ahead of it in the same level are still
    expanded (the cap is on contracts kept, not on work done within an already-started
    level). The depth limit is enforced by never expanding a level-MAX_DEPTH contract."""
    fetch_all_pages = fetch_all_pages or api_client.fetch_all_pages

    contracts = {}
    truncated = False
    limit = None

    for row in level1_rows:
        key_lower = row["contractAddress"].lower()
        if key_lower not in contracts and len(contracts) >= MAX_CONTRACTS:
            truncated, limit = True, "contracts"
            continue
        entry = contracts.setdefault(key_lower, {
            "address": row["contractAddress"], "creator": address, "level": 1, "creations": [],
        })
        entry["creations"].append({"hash": row["hash"], "timeStamp": row["timeStamp"], "type": row["type"]})

    frontier = [key for key in contracts if contracts[key]["level"] == 1]
    level = 1

    while frontier and not truncated:
        if level >= MAX_DEPTH:
            # INFO: one more expansion per edge contract, discarded, only to tell a chain
            # that legitimately ends here from one the depth cap actually cut short
            for contract_key in frontier:
                if _expand(contracts[contract_key]["address"], chainid, key, fetch_all_pages):
                    truncated, limit = True, "depth"
                    break
            break

        next_frontier = []
        for contract_key in frontier:
            if len(contracts) >= MAX_CONTRACTS:
                truncated, limit = True, "contracts"
                break
            creator_address = contracts[contract_key]["address"]
            for row in _expand(creator_address, chainid, key, fetch_all_pages):
                child_key = row["contractAddress"].lower()
                if child_key not in contracts:
                    if len(contracts) >= MAX_CONTRACTS:
                        truncated, limit = True, "contracts"
                        continue
                    contracts[child_key] = {
                        "address": row["contractAddress"], "creator": creator_address, "level": level + 1, "creations": [],
                    }
                    next_frontier.append(child_key)
                contracts[child_key]["creations"].append({"hash": row["hash"], "timeStamp": row["timeStamp"], "type": row["type"]})
        level += 1
        frontier = next_frontier

    return list(contracts.values()), truncated, limit


def _is_collected(dbname, address, blockchain):
    try:
        with closing(sqlite3.connect(f"file:{dbname}?mode=ro", uri=True)) as connection:
            row = connection.execute(
                "SELECT 1 FROM t_blocks WHERE blockChain = ? AND lower(address) = ?",
                (blockchain, address.lower()),
            ).fetchone()
    except sqlite3.OperationalError:
        return False
    return row is not None


def level1_from_api(address, chainid, key, fetch_all_pages):
    """Direct creations of an address never collected (S1): its own txlist rows with empty
    `to` and a contractAddress, plus its own internal create/create2 rows."""
    rows = []
    trxs, _ = fetch_all_pages(chainid, "txlist", address, key)
    for trx in trxs:
        if not trx.get("to") and trx.get("contractAddress"):
            rows.append({"hash": trx["hash"], "timeStamp": trx["timeStamp"],
                         "contractAddress": trx["contractAddress"], "type": "create"})
    rows.extend(_expand(address, chainid, key, fetch_all_pages))
    return rows


def get_created_contracts(dbname, address, blockchain, chainid, key, fetch_all_pages=None):
    """Entry point (S4): level 1 from stored data when the address was collected (no API
    call), from the API otherwise; deeper levels via the API."""
    fetch_all_pages = fetch_all_pages or api_client.fetch_all_pages
    if _is_collected(dbname, address, blockchain):
        level1_rows = level1_from_db(dbname, address, blockchain)
    else:
        level1_rows = level1_from_api(address, chainid, key, fetch_all_pages)
    return find_created_contracts(address, level1_rows, chainid, key, fetch_all_pages)
