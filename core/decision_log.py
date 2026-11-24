#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""Read-only query over t_decisions (odd/tasks/decision-evidence-retrofit.md, T3 S4/S6).

Lets the UI list the decisions recorded for a query address, so an automatic decision
taken in favour of the investigation is not just in a log file nobody reads."""

import json
import logging
import sqlite3
from contextlib import closing

from core.decisions import DatabaseUnreadable, not_created_yet

logger = logging.getLogger(__name__)

_SELECT = ("SELECT created_at, endpoint, code, scope, rule, reason, details FROM t_decisions "
           "WHERE blockChain = ? AND lower(query_address) = ?")
_QUERY_ALL = _SELECT + " ORDER BY created_at DESC"
_QUERY_BY_ENDPOINT = _SELECT + " AND endpoint = ? ORDER BY created_at DESC"


def get_decisions(dbname, address, blockchain="eth", endpoint=None):
    """Decisions stored for `address` (case-insensitive), newest first, as a list of dicts
    (created_at, endpoint, code, scope, rule, reason, details). Optionally filtered by
    endpoint. A DB or table not created yet has no decisions; one that exists but cannot be
    read raises DatabaseUnreadable."""
    address = address.lower()
    # INFO: two fixed statements instead of string building; every value is a bound parameter
    if endpoint is None:
        query, params = _QUERY_ALL, (blockchain, address)
    else:
        query, params = _QUERY_BY_ENDPOINT, (blockchain, address, endpoint)

    try:
        with closing(sqlite3.connect(f"file:{dbname}?mode=ro", uri=True)) as connection:
            cursor = connection.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
    except sqlite3.Error as error:
        if not_created_yet(error, dbname):
            return []
        logger.error("Could not read decisions of %s from %s: %s", address, dbname, error)
        raise DatabaseUnreadable(str(error)) from None

    return [
        {
            "created_at": created_at,
            "endpoint": endpoint_name,
            "code": code,
            "scope": scope,
            "rule": rule,
            "reason": reason,
            "details": _parse_details(details),
        }
        for created_at, endpoint_name, code, scope, rule, reason, details in rows
    ]


def _parse_details(details):
    """details is JSON written by decisions.persist; a row written by hand or truncated must
    not break the whole list, so it comes back as raw text under "unparsed"."""
    try:
        return json.loads(details) if details else {}
    except (TypeError, ValueError):
        return {"unparsed": details}
