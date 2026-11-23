#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""List the contracts an address created, directly and recursively through the
contracts it created in turn (R11, S1-S3). Pure walk logic: callers inject how each
level's internal creations are fetched, so tests never hit the network.

Level 1 is read from data already stored by the normal collection flow (t_transactions,
t_internals): the address's own creations. Level N+1 is the internal create/create2 rows
of each level-N contract, one `txlistinternal` per contract (cost grows with the width of
each level, not with the whole history)."""

import json
import sqlite3
import time
from collections import deque
from contextlib import closing

from core import api_client, decisions

# INFO: 3 hops reaches a liquidator from the attacker in the Euler case (address ->
# orchestrator -> violator -> liquidator, see documents/EULER-RESEARCH.md). Generalizing
# further would need one API call per contract found at each extra level, so this is a
# deliberate ceiling, not a number derived from any single case.
MAX_DEPTH = 3

# INFO: A factory contract (e.g. Uniswap V2 Factory) creates one contract per call
# forever; without a hard total cap a single popular address would expand without bound
# (documents/VALIDACION.md, S3 control for R11 cost).
MAX_CONTRACTS = 50

# INFO: per-query API call budget (R16 S3): a query with a popular address can no longer
# spend an unbounded number of explorer calls; the caller picks a lower number for a
# cheaper query, or a higher one (up to the ceiling) for a more thorough one.
DEFAULT_MAX_CALLS = 50
MAX_CALLS_CEILING = 200

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


class UpstreamError(Exception):
    """An explorer call failed; the walk stops calling and reports what is pending (S2)."""


class BudgetExhausted(Exception):
    """The per-query call budget ran out (R16 S3); the walk stops expanding, keeping
    everything found so far, instead of treating it as an upstream failure."""


class CallBudget:
    """Counts explorer downloads made in one query, clamped to [1, 200] (R16 S3). Cache
    hits never call spend(): only Source._download and level1_from_api's own txlist call
    count against it."""

    def __init__(self, max_calls):
        self.max_calls = max(1, min(200, max_calls))
        self.calls = 0

    def available(self):
        return self.calls < self.max_calls

    def spend(self):
        self.calls += 1


class Source:
    """Where each contract's own creations come from (S1): the per-address cache in
    t_created_contracts_cache when it has them, the explorer API otherwise. Successful
    downloads are cached; failed ones never are, so a retry asks again. The cache lives in
    the investigation DB and never creates it: without a usable DB the walk still runs,
    uncached, and says so (cache_unavailable)."""

    def __init__(self, dbname, blockchain, chainid, key, fetch_all_pages, refresh=False, budget=None):
        self.dbname, self.blockchain, self.chainid, self.key = dbname, blockchain, chainid, key
        self.fetch_all_pages, self.refresh = fetch_all_pages, refresh
        self.budget = budget or CallBudget(DEFAULT_MAX_CALLS)
        self.cache_error = None
        self.served = []
        self.incomplete_histories = []

    def _connect(self):
        connection = sqlite3.connect(f"file:{self.dbname}?mode=rw", uri=True)
        connection.execute(
            "CREATE TABLE IF NOT EXISTS t_created_contracts_cache (blockChain text, address text, "
            "last_block integer, complete integer, fetched_at integer, rows text, "
            "UNIQUE(blockChain, address))")
        return connection

    def _read(self, address):
        if self.dbname is None or self.cache_error:
            return None
        try:
            with closing(self._connect()) as connection:
                return connection.execute(
                    "SELECT last_block, complete, fetched_at, rows FROM t_created_contracts_cache "
                    "WHERE blockChain = ? AND address = ?", (self.blockchain, address)).fetchone()
        except sqlite3.Error as error:
            self.cache_error = str(error)
            return None

    def _write(self, address, last_block, complete, rows):
        if self.dbname is None or self.cache_error:
            return
        try:
            with closing(self._connect()) as connection:
                connection.execute(
                    "INSERT OR REPLACE INTO t_created_contracts_cache VALUES (?, ?, ?, ?, ?, ?)",
                    (self.blockchain, address, last_block, int(complete), int(time.time()), json.dumps(rows)))
                connection.commit()
        except sqlite3.Error as error:
            self.cache_error = str(error)

    def _download(self, address, startblock):
        kwargs = {"startblock": startblock} if startblock else {}
        try:
            return self.fetch_all_pages(self.chainid, "txlistinternal", address, self.key, **kwargs)
        except Exception as error:
            raise UpstreamError(str(error)) from None

    def creations(self, contract_address):
        """Internal create/create2 rows made BY contract_address, the next level's creations.
        txlistinternal(contract_address) returns every internal row touching it, including
        the one that created it (`to` = contract_address); only `from` = contract_address
        rows are its own creations. Raises UpstreamError when the API fails, BudgetExhausted
        when the per-query call budget ran out (R16 S3); a cache hit costs nothing."""
        address = contract_address.lower()
        cached = self._read(address)
        if cached and not self.refresh:
            last_block, complete, fetched_at, rows = cached
            self.served.append({"address": address, "fetched_at": fetched_at})
            if not complete:
                self.incomplete_histories.append(address)
            try:
                return json.loads(rows)
            except ValueError as error:
                # INFO: an unreadable entry is a cache miss, refetched and overwritten below
                self.cache_error = self.cache_error or f"corrupt cache entry for {address}: {error}"
                cached = None

        if not self.budget.available():
            raise BudgetExhausted(contract_address)

        merged = []
        if cached and self.refresh:
            try:
                merged = json.loads(cached[3])
            except ValueError:
                cached = None
        startblock = cached[0] if cached else 0
        raw, complete = self._download(contract_address, startblock)
        self.budget.spend()
        own = [
            {"hash": row["hash"], "timeStamp": row["timeStamp"], "contractAddress": row["contractAddress"],
             "type": row["type"], "blockNumber": row.get("blockNumber")}
            for row in raw
            if row.get("type") in CREATE_TYPES and row.get("contractAddress")
            and row.get("from", "").lower() == address
        ]
        if merged:
            # INFO: a refresh starts at the last cached block (not +1, a block can hold more
            # rows), so rows already cached come back and are dropped here
            seen = {(r["hash"], r["contractAddress"].lower()) for r in merged}
            own = merged + [r for r in own if (r["hash"], r["contractAddress"].lower()) not in seen]
        if not complete:
            self.incomplete_histories.append(address)
        blocks = [int(row["blockNumber"]) for row in raw if row.get("blockNumber")]
        self._write(address, max(blocks + [startblock]), complete, own)
        return own

    def cached_only(self, contract_address):
        """The depth cap (R16 S3) never calls the API: this reads the cache if it already
        holds contract_address's creations, without spending the call budget, and returns
        None when nothing is cached (the caller then knows the cut is unverified)."""
        address = contract_address.lower()
        cached = self._read(address)
        if not cached:
            return None
        last_block, complete, fetched_at, rows = cached
        self.served.append({"address": address, "fetched_at": fetched_at})
        if not complete:
            self.incomplete_histories.append(address)
        try:
            return json.loads(rows)
        except ValueError:
            return None

    def decisions(self):
        found = []
        if self.cache_error:
            found.append(decisions.make(
                "cache_unavailable", None, "t_created_contracts_cache", self.cache_error))
        if self.served:
            found.append(decisions.make(
                "served_from_cache", None, "refresh=false",
                f"{len(self.served)} addresses answered from the cache; use refresh=true to fetch new creations",
                {"addresses": self.served}))
        for address in dict.fromkeys(self.incomplete_histories):
            found.append(decisions.make(
                "history_incomplete", address, f"MAX_ROWS_PER_LIST={api_client.MAX_ROWS_PER_LIST}",
                "txlistinternal hit its row cap; later creations of this address are missing"))
        return found


def _new_result():
    return {"contracts": [], "truncated": False, "limit": None, "incomplete": False,
            "pending": [], "decisions": [], "calls": 0}


def find_created_contracts(address, level1_rows, chainid, key, fetch_all_pages=None, source=None):
    """Walk the creation hierarchy starting from address's level-1 rows (S1), deduplicating
    by contract address while keeping every creation transaction (S3), down to MAX_DEPTH and
    up to MAX_CONTRACTS contracts total (S2).

    Returns {contracts, truncated, limit, incomplete, pending, decisions}. An explorer failure
    stops the walk: everything found so far is kept, the addresses not expanded are listed in
    `pending`, and an upstream_error decision says why (R15 S2). Every limit hit is also a
    decision (S5).

    The contracts limit is enforced the moment a new contract would exceed it: no API call
    is made for it, and the ones already queued ahead of it in the same level are still
    expanded (the cap is on contracts kept, not on work done within an already-started
    level). The depth limit is enforced by never expanding a level-MAX_DEPTH contract: its
    creations are read from the cache only (R16 S3), never downloaded; an edge contract not
    already cached is reported unverified instead of spending a call to check it. Running
    out of the per-query call budget (source.budget) stops expansion the same way an
    upstream error does, keeping everything found and reporting call_cap_reached."""
    fetch_all_pages = fetch_all_pages or api_client.fetch_all_pages
    source = source or Source(None, None, chainid, key, fetch_all_pages)
    result = _new_result()

    contracts = {}
    dropped = []

    def keep(row, creator, level):
        child_key = row["contractAddress"].lower()
        is_new = child_key not in contracts
        if is_new:
            if len(contracts) >= MAX_CONTRACTS:
                dropped.append(row["contractAddress"])
                return False
            contracts[child_key] = {"address": row["contractAddress"], "creator": creator, "level": level,
                                    "creations": []}
        contracts[child_key]["creations"].append({"hash": row["hash"], "timeStamp": row["timeStamp"],
                                                  "type": row["type"]})
        return is_new

    for row in level1_rows:
        keep(row, address, 1)

    # INFO: breadth-first, so levels are expanded in order and a failure leaves the rest of
    # the queue as exactly what the walk could not see
    queue = deque(key for key in contracts if contracts[key]["level"] == 1)
    depth_cut_by = None
    unverified = []

    while queue and not dropped:
        contract_key = queue[0]
        entry = contracts[contract_key]
        try:
            if entry["level"] >= MAX_DEPTH:
                # INFO: the depth cap never spends a call: a cached edge tells a chain that
                # legitimately ends here from one the cap actually cut short; an uncached
                # edge is reported unverified instead of being downloaded just to check it
                cached_rows = source.cached_only(entry["address"])
                if cached_rows is None:
                    unverified.append(entry["address"])
                elif cached_rows and depth_cut_by is None:
                    depth_cut_by = entry["address"]
            elif len(contracts) >= MAX_CONTRACTS:
                dropped.append(None)
                break
            else:
                for row in source.creations(entry["address"]):
                    if keep(row, entry["address"], entry["level"] + 1):
                        queue.append(row["contractAddress"].lower())
        except UpstreamError as error:
            # INFO: edge contracts left unchecked only affect the depth verdict, not the list
            pending = [contracts[k]["address"] for k in queue if contracts[k]["level"] < MAX_DEPTH]
            pending = pending or [entry["address"]]
            result["incomplete"] = True
            result["pending"] = pending
            result["decisions"].append(decisions.make(
                "upstream_error", pending[0], "stop on first explorer error", str(error), {"pending": pending}))
            break
        except BudgetExhausted:
            pending = [contracts[k]["address"] for k in queue if contracts[k]["level"] < MAX_DEPTH]
            pending = pending or [entry["address"]]
            result["incomplete"] = True
            result["pending"] = pending
            result["decisions"].append(decisions.make(
                "call_cap_reached", address, f"max_calls={source.budget.max_calls}",
                "The per-query API call budget ran out; some addresses were not expanded",
                {"pending": pending, "calls": source.budget.calls}))
            break
        queue.popleft()

    if dropped:
        result["truncated"], result["limit"] = True, "contracts"
        result["decisions"].append(decisions.make(
            "size_cap_reached", address, f"MAX_CONTRACTS={MAX_CONTRACTS}",
            "Total contracts cap reached; further creations were not added",
            {"dropped": [d for d in dropped if d]}))
    elif depth_cut_by:
        result["truncated"], result["limit"] = True, "depth"
        result["decisions"].append(decisions.make(
            "depth_cap_reached", depth_cut_by, f"MAX_DEPTH={MAX_DEPTH}",
            "Depth limit reached; this contract created more contracts that were not listed"))

    if unverified:
        result["decisions"].append(decisions.make(
            "depth_cut_unverified", address, f"MAX_DEPTH={MAX_DEPTH}",
            "Depth limit reached; deeper creations were not checked",
            {"addresses": unverified}))

    result["contracts"] = list(contracts.values())
    result["calls"] = source.budget.calls
    result["decisions"] = source.decisions() + result["decisions"]
    return result


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


def level1_from_api(address, chainid, key, fetch_all_pages, budget=None):
    """Direct creations of an address never collected (S1): its own txlist rows with empty
    `to` and a contractAddress, plus its own internal create/create2 rows. Both downloads
    count against budget (R16 S3); once it runs out, whichever of the two was not made yet
    is skipped and whatever was already found is kept."""
    budget = budget or CallBudget(DEFAULT_MAX_CALLS)
    rows = []
    if budget.available():
        trxs, _ = fetch_all_pages(chainid, "txlist", address, key)
        budget.spend()
        for trx in trxs:
            if not trx.get("to") and trx.get("contractAddress"):
                rows.append({"hash": trx["hash"], "timeStamp": trx["timeStamp"],
                             "contractAddress": trx["contractAddress"], "type": "create"})
    if budget.available():
        rows.extend(Source(None, None, chainid, key, fetch_all_pages, budget=budget).creations(address))
    return rows


def get_created_contracts(dbname, address, blockchain, chainid, key, fetch_all_pages=None, refresh=False,
                          max_calls=DEFAULT_MAX_CALLS):
    """Entry point (S4): level 1 from stored data when the address was collected (no API
    call), from the API otherwise; deeper levels from the cache or the API. max_calls caps
    every download made answering this query (R16 S3), clamped to [1, MAX_CALLS_CEILING]."""
    fetch_all_pages = fetch_all_pages or api_client.fetch_all_pages
    budget = CallBudget(max_calls)
    if _is_collected(dbname, address, blockchain):
        level1_rows = level1_from_db(dbname, address, blockchain)
    else:
        try:
            level1_rows = level1_from_api(address, chainid, key, fetch_all_pages, budget=budget)
        except Exception as error:
            result = _new_result()
            result.update(incomplete=True, pending=[address], calls=budget.calls, decisions=[decisions.make(
                "upstream_error", address, "stop on first explorer error", str(error), {"pending": [address]})])
            return result
    source = Source(dbname, blockchain, chainid, key, fetch_all_pages, refresh=refresh, budget=budget)
    return find_created_contracts(address, level1_rows, chainid, key, fetch_all_pages, source=source)
