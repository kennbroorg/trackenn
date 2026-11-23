#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""Evidence of automatic decisions (odd/tasks/created-contracts-cache.md, S5).

Whenever the backend decides something on the analyst's behalf (cuts a walk, serves cached
data, skips an address after an error) it records a decision so the UI can show what was
hidden, cut or assumed. A decision never deletes data: it says what was left out and why.

Shape: {code, scope, rule, reason, details}. `code` is a closed catalogue (CODES) the front
maps to a notice; `scope` is the address the decision affects; `rule` names the parameter or
rule that took it, so it can be audited and changed."""

import json
import logging
import sqlite3
import time
from contextlib import closing

logger = logging.getLogger(__name__)

CODES = {
    "served_from_cache": "Data came from the local cache, not from a fresh API call",
    "cache_unavailable": "The cache could not be read or written; every call went to the API",
    "upstream_error": "The explorer API failed; some addresses were not expanded",
    "history_incomplete": "An account list hit its row cap; creations past it are missing",
    "call_cap_reached": "The per-query API call budget ran out; some addresses were not expanded",
    "depth_cap_reached": "The depth limit was reached and deeper creations exist; they were not listed",
    "depth_cut_unverified": "The depth limit was reached; deeper creations were not checked",
    "size_cap_reached": "The total contracts cap was reached; further contracts were dropped",
    "db_error": "The investigation database could not be read",
}


def make(code, scope, rule, reason, details=None):
    if code not in CODES:
        raise ValueError(f"Unknown decision code: {code}")
    return {"code": code, "scope": scope, "rule": rule, "reason": reason, "details": details or {}}


def persist(dbname, endpoint, query_address, blockchain, decisions):
    """Append decisions to t_decisions so a later report can explain them. Never creates the
    DB file; a failure is logged and returned as False (the response still carries them)."""
    if not decisions:
        return True
    try:
        with closing(sqlite3.connect(f"file:{dbname}?mode=rw", uri=True)) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS t_decisions (created_at integer, endpoint text, "
                "query_address text, blockChain text, code text, scope text, rule text, "
                "reason text, details text)")
            now = int(time.time())
            connection.executemany(
                "INSERT INTO t_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [(now, endpoint, query_address, blockchain, d["code"], d["scope"], d["rule"],
                  d["reason"], json.dumps(d["details"])) for d in decisions])
            connection.commit()
    except sqlite3.Error as error:
        logger.error("Could not persist %d decisions for %s: %s", len(decisions), query_address, error)
        return False
    return True
