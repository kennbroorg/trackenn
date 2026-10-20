import ast
import json
import re
import sqlite3
from contextlib import closing
from pathlib import Path

import pandas as pd
import pytest

from core import api_client, bsc, eth, misc

ROOT = Path(__file__).resolve().parent.parent
CORE_MODULES = ["core/eth.py", "core/bsc.py", "core/misc.py"]
# INFO: Names built by the code itself (insert_with_ignore), never user input
SAFE_SQL_NAMES = {"table.name", "columns", "placeholders"}

OR_PAYLOAD = "0xabc' or '1'='1"
UPDATE_ALL_PAYLOAD = "0xabc' where 1=1 --"


SEED_TABLES = {"t_tags", "t_address_detail", "t_transactions", "t_transfers", "t_funders_creators", "t_labels"}


def insert(path, table, **values):
    # INFO: Test seeding only, table allowlisted and columns read from the real schema
    assert table in SEED_TABLES
    with closing(sqlite3.connect(path)) as conn:
        columns = [row[0] for row in conn.execute("SELECT name FROM pragma_table_info(?)", (table,))]
        assert set(values) <= set(columns)
        row = {column: values.get(column, 0) for column in columns}
        pd.DataFrame([row]).to_sql(table, conn, if_exists="append", index=False)
        conn.commit()


def stored_tags(path):
    with closing(sqlite3.connect(path)) as conn:
        return sorted(conn.execute("SELECT blockChain, address, tag FROM t_tags").fetchall())


def run_stream(stream, params):
    messages = []
    generator = stream(params)
    try:
        for chunk in generator:
            message = json.loads(chunk[len("data:"):])
            messages.append(message)
            if message["end"]:
                break
    except Exception as error:
        messages.append({"exception": repr(error)})
    finally:
        generator.close()
    return messages


@pytest.fixture
def api_calls(monkeypatch):
    calls = []

    def failing_get(url):
        calls.append(url)
        raise api_client.requests.ConnectionError("explorer unavailable")

    monkeypatch.setattr(api_client, "api_get", failing_get)
    return calls


def stream_params(db, address, network):
    config = {"ethscan": "K", "bscscan": "K", "level": "INFO", "dbname": str(db)}
    return {"address": address, "network": network, "graph": "Star", "filters": {"filter": False}, "config": config}


STREAMS = [pytest.param(eth.event_stream_ether, "eth", id="eth"), pytest.param(bsc.event_stream_bsc, "bsc", id="bsc")]


@pytest.mark.parametrize("stream, network", STREAMS)
@pytest.mark.parametrize("address", ["0xabc", OR_PAYLOAD, UPDATE_ALL_PAYLOAD])
def test_stream_stores_central_and_path_address_literally(db, api_calls, stream, network, address):
    insert(db, "t_tags", blockChain=network, address="0xold", tag="central")
    insert(db, "t_tags", blockChain=network, address="0xother", tag="wallet")

    run_stream(stream, stream_params(db, address, network))

    assert stored_tags(db) == sorted([
        (network, address, "central"),
        (network, "0xother", "wallet"),
        (network, address, "path"),
    ])


@pytest.mark.parametrize("stream, network", STREAMS)
def test_stream_network_cannot_read_other_address_detail(db, api_calls, stream, network):
    insert(db, "t_address_detail", blockChain=network, address="0xother", type="wallet")

    messages = run_stream(stream, stream_params(db, "0xnew", f"{network}' or '1'='1"))

    assert len(api_calls) == 1  # INFO: 0xnew is not cached, so the explorer is queried
    assert "0xother" not in json.dumps(messages)
    assert stored_tags(db) == [
        (f"{network}' or '1'='1", "0xnew", "central"),
        (f"{network}' or '1'='1", "0xnew", "path"),
    ]


def test_checking_stored_central_cannot_read_other_address_detail(db):
    insert(db, "t_tags", blockChain="eth", address=OR_PAYLOAD, tag="central")
    insert(db, "t_address_detail", blockChain="eth", address="0xother", type="wallet")
    config = {"ethscan": "K", "bscscan": "K", "level": "INFO", "dbname": str(db),
              "action": "init", "graph": "Star", "filters": {"filter": False}}

    messages = run_stream(misc.event_stream_checking, config)

    assert "0xother" not in json.dumps(messages)
    assert messages[-1]["end"] is True


def seed_activity(db, chain, address):
    insert(db, "t_transactions", blockChain=chain, hash="0x1", **{"from": address, "to": "0xb"},
           value=10**18, gasPrice=10**9, gasUsed=21000, isError=0)
    insert(db, "t_transactions", blockChain=chain, hash="0x2", **{"from": "0xb", "to": address},
           value=3 * 10**18, gasPrice=10**9, gasUsed=21000, isError=0)
    insert(db, "t_transactions", blockChain=chain, hash="0x3", **{"from": "0xc", "to": "0xd"},
           value=5 * 10**18, gasPrice=10**9, gasUsed=21000, isError=0)
    insert(db, "t_transfers", blockChain=chain, hash="0x4", **{"from": "0xc", "to": address},
           value=2_500_000, tokenSymbol="USDT", tokenName="Tether", tokenDecimal=6)
    insert(db, "t_transfers", blockChain=chain, hash="0x5", **{"from": "0xc", "to": "0xd"},
           value=9_000_000, tokenSymbol="USDT", tokenName="Tether", tokenDecimal=6)


BALANCES = [pytest.param(eth, "eth", "ETH", "Ether", id="eth"), pytest.param(bsc, "bsc", "BNB", "BNB", id="bsc")]


@pytest.mark.parametrize("module, chain, token, token_name", BALANCES)
def test_balance_and_gas_of_wallet(db, module, chain, token, token_name):
    seed_activity(db, chain, "0xa")

    with closing(sqlite3.connect(db)) as conn:
        result = module.get_balance_and_gas(conn, (chain, "0xa", "central"), "wallet", "K")

    assert [(row["token"], row["tokenName"], row["balance"]) for row in result["balance"]] == \
        [(token, token_name, pytest.approx(2 - 21000 * 10**9 * 1e-18))]
    assert [(row["token"], row["tokenName"], row["balance"]) for row in result["tokens"]] == \
        [("USDT", "Tether", pytest.approx(2.5))]
    assert result["gas"] == pytest.approx(21000 * 10**9 * 1e-18)


@pytest.mark.parametrize("module, chain, token, token_name", BALANCES)
def test_balance_and_gas_only_counts_literal_address(db, module, chain, token, token_name):
    seed_activity(db, chain, OR_PAYLOAD)

    with closing(sqlite3.connect(db)) as conn:
        result = module.get_balance_and_gas(conn, (chain, OR_PAYLOAD, "central"), "wallet", "K")

    gas = 21000 * 10**9 * 1e-18
    assert result == {
        "balance": [{"blockChain": chain, "address": OR_PAYLOAD, "token": token, "tokenName": token_name,
                     "balance": pytest.approx(2 - gas)}],
        "tokens": [{"blockChain": chain, "address": OR_PAYLOAD, "token": "USDT", "tokenName": "Tether",
                    "balance": pytest.approx(2.5)}],
        "gas": pytest.approx(gas),
    }


MODULES = [pytest.param(eth, "eth", "ethereum", id="eth"), pytest.param(bsc, "bsc", "binance", id="bsc")]


@pytest.mark.parametrize("module, chain, label_chain", MODULES)
@pytest.mark.parametrize("address", ["0xabc", OR_PAYLOAD])
def test_funders_tags_and_labels_only_for_literal_address(db, module, chain, label_chain, address):
    for target in (address, "0xother"):
        insert(db, "t_funders_creators", blockChain=chain, hash=f"0x{target}", type="funder",
               **{"from": "0xfunder", "to": target})
        insert(db, "t_tags", blockChain=chain, address=target, tag="wallet")
        insert(db, "t_labels", blockChain=label_chain, source="etherscan_label", address=target,
               name=f"name {target}", labels="[]")

    with closing(sqlite3.connect(db)) as conn:
        funders = json.loads(module.get_funders_creators(conn, (chain, address, "central"))["funders"])
        tags = module.get_tags_labels(conn, (chain, address, "central"))

    assert [funder["to"] for funder in funders] == [address]
    assert tags["tags"] == [{"blockChain": chain, "address": address, "tag": "wallet"}]
    assert tags["labels"] == [{"blockChain": label_chain, "source": "etherscan_label", "address": address,
                               "name": f"name {address}", "labels": "[]"}]


SQL_KEYWORDS = re.compile(r"\b(SELECT|INSERT|UPDATE|DELETE)\b")


def sql_text(node):
    """Literal text of a string node (plain or f-string), else None"""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(part.value for part in node.values if isinstance(part, ast.Constant))
    return None


def concat_parts(node):
    """Flatten a chain of + into its operands"""
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return concat_parts(node.left) + concat_parts(node.right)
    return [node]


def interpolated_sql_in(source):
    """SQL statements built with values: f-string, + concatenation, % formatting or .format()"""
    found = []
    for node in ast.walk(ast.parse(source)):
        names = set()
        if isinstance(node, ast.JoinedStr):
            text = sql_text(node)
            names = {ast.unparse(part.value) for part in node.values if isinstance(part, ast.FormattedValue)}
        elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            parts = concat_parts(node)
            text = "".join(sql_text(part) or "" for part in parts)
            names = {ast.unparse(part) for part in parts if sql_text(part) is None}
        elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
            text = sql_text(node.left)
            names = {ast.unparse(node.right)}
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format":
            text = sql_text(node.func.value)
            names = {ast.unparse(arg) for arg in [*node.args, *[kw.value for kw in node.keywords]]}
        else:
            continue
        if text and SQL_KEYWORDS.search(text) and names - SAFE_SQL_NAMES:
            found.append((node.lineno, sorted(names)))
    return found


@pytest.mark.parametrize("source", [
    'q = f"SELECT * FROM t_tags WHERE address = \'{address}\'"',
    'q = "SELECT * FROM t_tags WHERE address = \'" + address + "\'"',
    'q = "SELECT * FROM t_tags WHERE address = \'%s\'" % address',
    'q = "SELECT * FROM t_tags WHERE address = \'{}\'".format(address)',
    'q = "UPDATE t_tags SET tag = \'{tag}\'".format(tag=tag)',
])
def test_guard_detects_interpolated_sql(source):
    assert interpolated_sql_in(source) != []


@pytest.mark.parametrize("source", [
    'cursor.execute("SELECT * FROM t_tags WHERE address = ?", (address,))',
    'q = "SELECT * FROM t_tags " + "WHERE tag = \'central\'"',
    'stmt = f"INSERT INTO {table.name} ({columns}) VALUES ({placeholders})"',
    'logger.debug("INSERT Path: %s %s", insert, (blockchain, address))',
    'message = "Selected %s" % address',
])
def test_guard_allows_bound_parameters(source):
    assert interpolated_sql_in(source) == []


@pytest.mark.parametrize("path", CORE_MODULES)
def test_core_sql_has_no_interpolated_values(path):
    assert interpolated_sql_in((ROOT / path).read_text()) == []
