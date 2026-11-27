"""R5: direct counterparties of an address that are mixers or sanctioned, from stored data.

Reads t_transactions, t_internals and t_transfers already collected (no explorer call) and the
labels in t_labels (Etherscan/BscScan labels plus the pinned OFAC SDN list). Every flow to or
from a risky counterparty is grouped by counterparty and direction with its amounts and every
hash as evidence, and leaves a decision; an inflow from a mixer also says the trail is lost.
"""
import json
import logging
import re
import sqlite3
import typing
from collections.abc import Iterator
from contextlib import closing

from core import decisions
from core.balances import to_int, to_units, track_unreadable

logger = logging.getLogger(__name__)

ENDPOINT = "/risk-flows"
LABEL_CHAIN = {"eth": "ethereum", "bsc": "binance"}
NATIVE_SYMBOL = {"eth": "ETH", "bsc": "BNB"}
NATIVE_DECIMALS = 18
SANCTION_LABELS = {"ofac-sdn", "ofac-sanctions-lists"}
MIXER_LABEL = "ethereum-mixer"
TORNADO_LABEL = "tornado-cash"
# INFO: Tornado contracts that take deposits: pools ("100 ETH", "500,000 cDAI 2") and entry
#       points. Governance, TORN/vTORN tokens, verifiers and vesting are not mixers (they
#       would flag holders of the governance token, e.g. Vitalik's vTORN).
TORNADO_MIXER_NAME = re.compile(
    r"^Tornado\.Cash: (?:[\d.,]+ [A-Za-z]+(?: \d+)?|Router|Proxy|Old Proxy|Mixer \d+)$")
EXAMPLE_HASHES = 5


def kinds_for(name, labels):
    """The risk kinds a labelled address carries: "mixer" and/or "sanctioned"."""
    found = []
    if MIXER_LABEL in labels or (TORNADO_LABEL in labels and TORNADO_MIXER_NAME.match(str(name or ""))):
        found.append("mixer")
    if SANCTION_LABELS & set(labels):
        found.append("sanctioned")
    return found


def _rows(cursor):
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _parse_labels(raw):
    try:
        labels = json.loads(str(raw or "[]"))
    except ValueError:
        return []
    return [str(label) for label in labels] if isinstance(labels, list) else []


def _read(dbname, address, blockchain):
    try:
        with closing(sqlite3.connect(dbname)) as conn:
            # INFO: fixed single-line SQL literals; caller values are bound parameters only
            data = {
                "t_transactions": _rows(conn.execute("SELECT hash, `from`, `to`, value, timeStamp, isError FROM t_transactions WHERE blockChain = ? AND (lower(`from`) = ? OR lower(`to`) = ?)", (blockchain, address, address))),  # noqa: E501
                "t_internals": _rows(conn.execute("SELECT hash, `from`, `to`, value, timeStamp, isError FROM t_internals WHERE blockChain = ? AND (lower(`from`) = ? OR lower(`to`) = ?)", (blockchain, address, address))),  # noqa: E501
                "t_transfers": _rows(conn.execute("SELECT hash, `from`, `to`, value, timeStamp, contractAddress, tokenSymbol, tokenDecimal FROM t_transfers WHERE blockChain = ? AND (lower(`from`) = ? OR lower(`to`) = ?)", (blockchain, address, address))),  # noqa: E501
            }
            data["labels"] = conn.execute(
                "SELECT lower(address), name, labels FROM t_labels WHERE blockChain = ?",
                (LABEL_CHAIN[blockchain],)).fetchall()
            return data
    except sqlite3.Error as error:
        logger.error("Risk flows: investigation database unreadable: %s", error)
        raise decisions.DatabaseUnreadable(str(error)) from None


def _risky_index(label_rows):
    """Labels of every row of one address are merged (an etherscan row and an ofac_sdn row
    can coexist); the name is the first non-empty one in table order, so an Etherscan name
    wins over the generic name of a later OFAC row. Mixer is judged per row by its own name."""
    merged = {}
    for address, name, raw in label_rows:
        labels = _parse_labels(raw)
        entry = merged.setdefault(address, {"name": None, "labels": set(), "kinds": set()})
        if not entry["name"] and name:
            entry["name"] = name
        entry["labels"].update(labels)
        entry["kinds"].update(kinds_for(name, labels))
    order = ("mixer", "sanctioned")
    return {address: {"name": entry["name"], "labels": sorted(entry["labels"]),
                      "kinds": [kind for kind in order if kind in entry["kinds"]]}
            for address, entry in merged.items() if entry["kinds"]}


class Movement(typing.NamedTuple):
    row: dict
    counterparty: str
    direction: str
    contract: str
    symbol: object
    decimals: int
    scope: str
    fields: tuple[str, ...]


def _movements(data, address, blockchain) -> Iterator[Movement]:
    """Unreadable numbers are scoped like /balances: the queried address for the native coin,
    the token contract for ERC-20."""
    native = NATIVE_SYMBOL[blockchain]
    for table in ("t_transactions", "t_internals"):
        for row in data[table]:
            if str(row["isError"]) == "1":
                continue
            for counterparty, direction in _sides(row, address):
                yield Movement(row, counterparty, direction, "eth", native, NATIVE_DECIMALS, address, ("value",))
    for row in data["t_transfers"]:
        contract = str(row["contractAddress"]).lower()
        for counterparty, direction in _sides(row, address):
            yield Movement(row, counterparty, direction, contract, row["tokenSymbol"], to_int(row["tokenDecimal"]),
                           contract, ("value", "tokenDecimal"))


def _sides(row, address) -> Iterator[tuple[str, str]]:
    sender, receiver = str(row["from"]).lower(), str(row["to"]).lower()
    if sender == address and receiver != address:
        yield (receiver, "out")
    if receiver == address and sender != address:
        yield (sender, "in")


def _group(data, address, blockchain, risky, unreadable):
    groups = {}
    for row, counterparty, direction, contract, symbol, decimals, scope, fields in _movements(
            data, address, blockchain):
        if counterparty not in risky:
            continue
        track_unreadable(unreadable, scope, row, fields)
        value, tx_hash, ts = to_int(row["value"]), str(row["hash"]), to_int(row["timeStamp"])
        info = risky[counterparty]
        entry = groups.setdefault((counterparty, direction), {
            "counterparty": counterparty, "name": info["name"], "kinds": info["kinds"],
            "labels": info["labels"], "direction": direction, "tokens": {},
            "first_timeStamp": ts, "last_timeStamp": ts, "hashes": []})
        token = entry["tokens"].setdefault(contract, {"symbol": symbol, "decimals": decimals, "raw": 0, "count": 0})
        token["raw"] += value
        token["count"] += 1
        entry["first_timeStamp"] = min(entry["first_timeStamp"], ts)
        entry["last_timeStamp"] = max(entry["last_timeStamp"], ts)
        if tx_hash not in entry["hashes"]:
            entry["hashes"].append(tx_hash)
    return groups


def _public(entry):
    per_token = [{"contract": contract, "symbol": token["symbol"],
                  "amount": to_units(token["raw"], token["decimals"]), "count": token["count"]}
                 for contract, token in sorted(entry["tokens"].items())]
    public = {key: value for key, value in entry.items() if key != "tokens"}
    public["per_token"] = per_token
    return public


def _decisions(entry):
    count = sum(token["count"] for token in entry["tokens"].values())
    hashes = entry["hashes"][:EXAMPLE_HASHES]  # INFO: the single place the example limit applies
    found = [decisions.make(
        "flow_risky", entry["counterparty"], "counterparty labelled mixer or sanctioned",
        "Funds moved directly between the address and a mixer or a sanctioned address",
        {"direction": entry["direction"], "kinds": entry["kinds"], "name": entry["name"],
         "count": count, "hashes": hashes})]
    if entry["direction"] == "in" and "mixer" in entry["kinds"]:
        found.append(decisions.make(
            "mixer_trail_lost", entry["counterparty"], "inflow from a mixer",
            "Funds came out of a mixer: the trail cannot be followed back from here",
            {"count": count, "hashes": hashes}))
    return found


def _persist(dbname, address, blockchain, found):
    """Store decisions once per (code, scope, rule, details); a failure is only logged."""
    if not found:
        return
    try:
        with closing(sqlite3.connect(f"file:{dbname}?mode=rw", uri=True)) as connection:
            decisions.persist_new(connection, ENDPOINT, address, blockchain, found)
    except sqlite3.Error as error:
        logger.error("Risk flows: could not persist %d decisions for %s: %s", len(found), address, error)


def get_risk_flows(dbname, address, blockchain="eth"):
    address = address.lower()
    data = _read(dbname, address, blockchain)
    unreadable = {}
    groups = _group(data, address, blockchain, _risky_index(data["labels"]), unreadable)
    ordered = sorted(groups.values(), key=lambda e: (e["first_timeStamp"], e["counterparty"], e["direction"]))
    found = [decision for entry in ordered for decision in _decisions(entry)]
    for scope in sorted(unreadable):
        entry = unreadable[scope]
        details = {"count": entry["count"], "fields": sorted(entry["fields"]), "hashes": entry["hashes"]}
        logger.warning("Risk flows: unreadable stored value for %s: %s", scope, details)
        found.append(decisions.make(
            "value_unreadable", scope, "non-numeric stored value counted as 0",
            "A stored numeric field was not a number and was counted as 0", details))
    _persist(dbname, address, blockchain, found)
    return {"address": address, "counterparties": [_public(entry) for entry in ordered], "decisions": found}
