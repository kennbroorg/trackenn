"""R7: case balance computed from the transfers already collected.

Per real token: inflow, outflow and net for the queried address (ETH from t_transactions
plus t_internals, ERC-20 from t_transfers). Contracts the classifier flagged as spam go to a
separate section with their tags, NFTs (ERC-721) and ERC-1155 are counted outside the
monetary total. Every automatic decision is returned as evidence. Optionally the computed
values are compared with the live chain through Etherscan, within a call budget.
"""
import json
import logging
import sqlite3
from contextlib import closing
from decimal import Decimal, InvalidOperation

from core import api_client, decisions

logger = logging.getLogger(__name__)

DEFAULT_MAX_CALLS = 20
ETH_DECIMALS = 18
SPAM_PREFIX = "spam-"


def _int(value):
    try:
        return int(Decimal(str(value or 0)))
    except (InvalidOperation, ValueError):
        return 0


def _units(raw, decimals):
    value = Decimal(raw).scaleb(-decimals)
    text = format(value.normalize(), "f")
    return "0" if text in ("-0", "") else text


TABLES = ("t_transactions", "t_internals", "t_transfers", "t_nfts", "t_multitoken")


def _rows(conn, query, args):
    cursor = conn.execute(query, args)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _read(dbname, address, blockchain):
    try:
        with closing(sqlite3.connect(dbname)) as conn:
            where = "blockChain = ? AND (lower(`from`) = ? OR lower(`to`) = ?)"
            args = (blockchain, address, address)
            data = {
                table: _rows(conn, f"SELECT * FROM {table} WHERE {where}", args)
                for table in TABLES  # INFO: fixed allowlist, never user input
            }
            data["tags"] = conn.execute(
                "SELECT lower(address), tag FROM t_tags WHERE blockChain = ? AND tag LIKE 'spam-%'",
                (blockchain,)).fetchall()
            has_decisions = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='t_decisions'").fetchone()
            data["incomplete"] = [] if not has_decisions else [row[0] for row in conn.execute(
                "SELECT details FROM t_decisions WHERE code = 'history_incomplete' AND lower(query_address) = ?",
                (address,))]
            return data
    except sqlite3.Error as error:
        logger.error("Balances: investigation database unreadable: %s", error)
        raise decisions.DatabaseUnreadable(str(error)) from None


def _action(details):
    """The account list a history_incomplete decision names, "" when unreadable."""
    try:
        return str(json.loads(str(details or "{}")).get("action") or "")
    except (ValueError, AttributeError):
        return ""


def _eth(data, address):
    inflow = outflow = fees = 0
    for row in data["t_transactions"]:
        sender = str(row["from"]).lower() == address
        if sender:
            fees += _int(row["gasUsed"]) * _int(row["gasPrice"])
        if str(row["isError"]) == "1":
            continue
        if sender:
            outflow += _int(row["value"])
        if str(row["to"]).lower() == address:
            inflow += _int(row["value"])
    for row in data["t_internals"]:
        if str(row["isError"]) == "1":
            continue
        if str(row["from"]).lower() == address:
            outflow += _int(row["value"])
        if str(row["to"]).lower() == address:
            inflow += _int(row["value"])
    net = inflow - outflow - fees
    return {"inflow": inflow, "outflow": outflow, "fees": fees, "net": net}


def _tokens(data, address):
    totals = {}
    for row in data["t_transfers"]:
        contract = str(row["contractAddress"]).lower()
        entry = totals.setdefault(contract, {
            "contract": contract, "symbol": row["tokenSymbol"], "decimals": _int(row["tokenDecimal"]),
            "inflow": 0, "outflow": 0, "transfers_in": 0, "transfers_out": 0})
        if str(row["to"]).lower() == address:
            entry["inflow"] += _int(row["value"])
            entry["transfers_in"] += 1
        if str(row["from"]).lower() == address:
            entry["outflow"] += _int(row["value"])
            entry["transfers_out"] += 1
    for entry in totals.values():
        entry["net"] = entry["inflow"] - entry["outflow"]
    return totals


def _nfts(data, address):
    counts = {}
    sources = (("t_nfts", "erc721", lambda row: 1), ("t_multitoken", "erc1155", lambda row: _int(row["tokenValue"])))
    for table, standard, units in sources:
        for row in data[table]:
            key = (str(row["contractAddress"]).lower(), str(row["tokenID"]))
            entry = counts.setdefault(key, {"contract": key[0], "token_id": key[1], "symbol": row["tokenSymbol"],
                                            "standard": standard, "units_in": 0, "units_out": 0})
            if str(row["to"]).lower() == address:
                entry["units_in"] += units(row)
            if str(row["from"]).lower() == address:
                entry["units_out"] += units(row)
    for entry in counts.values():
        entry["held"] = entry["units_in"] - entry["units_out"]
    return [counts[key] for key in sorted(counts)]


def _public_token(entry, tags):
    decimals = entry["decimals"]
    return {"contract": entry["contract"], "symbol": entry["symbol"], "decimals": decimals,
            "inflow": _units(entry["inflow"], decimals), "outflow": _units(entry["outflow"], decimals),
            "net": _units(entry["net"], decimals), "transfers_in": entry["transfers_in"],
            "transfers_out": entry["transfers_out"], "flagged_tags": tags}


def _chain_value(chainid, key, address, contract):
    params = {"module": "account", "address": address, "tag": "latest", "apikey": key}
    if contract:
        params = {"module": "account", "action": "tokenbalance", "contractaddress": contract, **params}
    else:
        params = {"module": "account", "action": "balance", **params}
    response = api_client.api_get(api_client.explorer_url(chainid, **params))
    return _int(api_client.extract_result(response))


def _compare(address, computed_eth, real_tokens, chainid, key, max_calls):
    """Compare ETH first, then each real ERC-20 in contract order, within max_calls."""
    onchain = {"eth": None, "tokens": {}}
    found = []
    targets = [(None, computed_eth, ETH_DECIMALS)] + [
        (entry["contract"], entry["net"], entry["decimals"]) for entry in real_tokens]
    for index, (contract, computed, decimals) in enumerate(targets):
        if index >= max_calls:
            found.append(decisions.make(
                "call_cap_reached", address, f"max_calls={max_calls}",
                "The on-chain comparison ran out of calls; some tokens were not compared",
                {"uncompared": sorted(c or "eth" for c, _, _ in targets[index:]), "max_calls": max_calls}))
            break
        try:
            chain = _chain_value(chainid, key, address, contract)
        except Exception as error:  # INFO: any explorer failure keeps the computed balance
            message = api_client.redact(error)
            logger.warning("Balances: on-chain comparison failed for %s: %s", address, message)
            found.append(decisions.make(
                "upstream_error", contract or address, "on-chain balance comparison", message,
                {"uncompared": sorted(c or "eth" for c, _, _ in targets[index:])}))
            break
        comparison = {"computed": _units(computed, decimals), "chain": _units(chain, decimals),
                      "difference": _units(chain - computed, decimals)}
        if contract:
            onchain["tokens"][contract] = comparison
        else:
            onchain["eth"] = comparison
        if chain != computed:
            found.append(decisions.make(
                "balance_mismatch", contract or address, "computed net != on-chain balance",
                "The balance computed from collected transfers differs from the live chain balance",
                {"token": contract or "eth", **comparison}))
    return onchain, found


def get_balances(dbname, address, blockchain="eth", onchain=False, chainid=None, key=None,
                 max_calls=DEFAULT_MAX_CALLS):
    address = address.lower()
    data = _read(dbname, address, blockchain)
    spam_tags = {}
    for contract, tag in data["tags"]:
        spam_tags.setdefault(contract, set()).add(tag)

    eth = _eth(data, address)
    tokens = _tokens(data, address)
    real = [tokens[c] for c in sorted(tokens) if c not in spam_tags]
    spam = [tokens[c] for c in sorted(tokens) if c in spam_tags]

    found = []
    if data["incomplete"]:
        actions = sorted({_action(details) for details in data["incomplete"]} - {""})
        found.append(decisions.make(
            "balance_partial", address, "history_incomplete recorded",
            "Some collected history was cut at its row cap; balances are bounds, not exact values",
            {"actions": actions}))
    negatives = ([(address, eth["net"], ETH_DECIMALS)] if eth["net"] < 0 else []) + [
        (entry["contract"], entry["net"], entry["decimals"]) for entry in real if entry["net"] < 0]
    for scope, net, decimals in negatives:
        found.append(decisions.make(
            "balance_negative", scope, "outflow > inflow",
            "More left than entered: history is missing or the token behaves unusually",
            {"net": _units(net, decimals)}))

    result = {
        "address": address,
        "eth": {name: _units(value, ETH_DECIMALS) for name, value in eth.items()},
        "tokens": [_public_token(entry, []) for entry in real],
        "spam": [_public_token(entry, sorted(spam_tags[entry["contract"]])) for entry in spam],
        "nfts": _nfts(data, address),
    }
    if onchain:
        result["onchain"], compared = _compare(address, eth["net"], real, chainid, key, max_calls)
        found.extend(compared)
    result["decisions"] = found
    return result
