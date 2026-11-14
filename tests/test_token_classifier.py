"""Internal spam/phishing token classifier (core/token_classifier.py, wired into
core/eth.py db_store_tagging_opt). Fixtures from the Euler case (documents/EULER.md).
"""
import sqlite3
from contextlib import closing

import pytest

from core import eth, token_classifier

CENTRAL = "0xb2698c2d99ad2c302a95a8db26b08d17a77cedd4"
USDREWARD = "0x470662ab26fdcf1b9e10de4c5287fd4948b2a370"
AETH_TRADE = "0xd763b37289264e6389c837a0a759f1e89dd75d0b"
HACKER_INU = "0xa59af77c2288e44021867a741e67d7155d6fec66"
FUCK_YEARN = "0x1fd6dfe10d14bc2dc77f68eaad06e96bd82edc1a"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
DAI = "0x6b175474e89094c44da98b954eedeac495271d0f"
FRIEND = "0xb0000000000000000000000000000000000000bb"
ETH_FOUNDATION = "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae"


def transfer(contract, frm, to, token_name, token_symbol, n=1):
    return {"blockChain": "eth", "blockNumber": str(100 + n), "timeStamp": str(1_700_000_000 + n * 60),
            "hash": f"0xt{n}", "from": frm, "to": to, "value": "1000000", "input": "deprecated",
            "contractAddress": contract, "tokenDecimal": "18", "tokenSymbol": token_symbol, "tokenName": token_name}


def trx(frm, to, n=1, input="0x"):
    return {"blockChain": "eth", "blockNumber": str(100 + n), "timeStamp": str(1_700_000_000 + n * 60),
            "hash": f"0xh{n}", "from": frm, "to": to, "value": "0", "input": input, "contractAddress": ""}


# ---- Pure function unit tests (S2, S3) ----

def test_phishing_detected_by_url_in_name_and_symbol():
    transfers = [
        transfer(USDREWARD, FRIEND, CENTRAL, "# usdreward.org", "Visit https://usdreward.org to claim rewards", 1),
        transfer(AETH_TRADE, FRIEND, CENTRAL, "# aeth.trade", "Visit https://aeth.trade to claim rewards", 2),
    ]

    assert token_classifier.find_phishing_contracts(transfers) == {USDREWARD, AETH_TRADE}


@pytest.mark.parametrize("name, symbol", [
    ("Wrapped Ether", "WETH"),
    ("USD Coin", "USDC"),
    ("Dai Stablecoin", "DAI"),
    ("Tether", "USDT"),
])
def test_legit_token_names_are_not_phishing(name, symbol):
    assert token_classifier.is_phishing_text(name, symbol) is False


@pytest.mark.parametrize("name, symbol", [
    ("# usdreward.org", "Visit https://usdreward.org to claim rewards"),
    ("# aeth.trade", "Visit https://aeth.trade to claim rewards"),
    ("www.freemoney.com", "CLAIM"),
    ("Reward Token", "REWARD"),
])
def test_phishing_text_variants_are_detected(name, symbol):
    assert token_classifier.is_phishing_text(name, symbol) is True


def test_unsolicited_detected_when_central_only_received():
    transfers = [
        transfer(HACKER_INU, FRIEND, CENTRAL, "Euler Hacker Inu", "EULERHACKERINU", 1),
        transfer(FUCK_YEARN, FRIEND, CENTRAL, "FUCK YEARN", "FYFI", 2),
    ]
    trxs = [trx(FRIEND, CENTRAL, 1)]

    result = token_classifier.find_unsolicited_contracts(transfers, trxs, CENTRAL)

    assert result == {HACKER_INU, FUCK_YEARN}


def test_not_unsolicited_when_central_also_sent_the_token():
    transfers = [
        transfer(DAI, FRIEND, CENTRAL, "Dai Stablecoin", "DAI", 1),
        transfer(DAI, CENTRAL, FRIEND, "Dai Stablecoin", "DAI", 2),
    ]

    assert token_classifier.find_unsolicited_contracts(transfers, [], CENTRAL) == set()


def test_not_unsolicited_when_central_called_the_contract():
    transfers = [transfer(DAI, FRIEND, CENTRAL, "Dai Stablecoin", "DAI", 1)]
    trxs = [trx(CENTRAL, DAI, 1, input="0xa9059cbb00")]

    assert token_classifier.find_unsolicited_contracts(transfers, trxs, CENTRAL) == set()


def test_classify_tokens_combines_both_criteria_without_mixing_tags():
    transfers = [
        transfer(USDREWARD, FRIEND, CENTRAL, "# usdreward.org", "Visit https://usdreward.org to claim rewards", 1),
        transfer(HACKER_INU, FRIEND, CENTRAL, "Euler Hacker Inu", "EULERHACKERINU", 2),
        transfer(DAI, FRIEND, CENTRAL, "Dai Stablecoin", "DAI", 3),
        transfer(DAI, CENTRAL, FRIEND, "Dai Stablecoin", "DAI", 4),
    ]

    result = token_classifier.classify_tokens(transfers, [], CENTRAL)

    assert result == {
        USDREWARD: {token_classifier.TAG_PHISHING, token_classifier.TAG_UNSOLICITED},
        HACKER_INU: {token_classifier.TAG_UNSOLICITED},
    }


# ---- Spoofed sender (S6) ----

def test_spoofed_sender_when_known_token_contract_emits_a_foreign_token():
    # INFO: Euler: "Visit https://usdreward.org..." claims to come from the real USDC contract
    transfers = [transfer(USDREWARD, USDC, CENTRAL, "# usdreward.org", "Visit https://usdreward.org to claim rewards", 1)]

    assert token_classifier.find_spoofed_sender_contracts(transfers) == {USDREWARD}


def test_spoofed_sender_is_case_insensitive():
    transfers = [transfer(HACKER_INU, USDC.upper().replace("0X", "0x"), CENTRAL, "Euler Hacker Inu", "EULERHACKERINU", 1)]

    assert token_classifier.find_spoofed_sender_contracts(transfers) == {HACKER_INU}


def test_spoofed_sender_when_known_wallet_sends_a_phishing_token():
    # INFO: Euler: "Visit https://aeth.trade..." claims to come from the Ethereum Foundation
    transfers = [transfer(AETH_TRADE, ETH_FOUNDATION, CENTRAL, "# aeth.trade", "Visit https://aeth.trade to claim rewards", 1)]

    assert token_classifier.find_spoofed_sender_contracts(transfers) == {AETH_TRADE}


@pytest.mark.parametrize("contract, frm, name, symbol", [
    (USDC, USDC, "USD Coin", "USDC"),                      # mint/burn of the token itself
    (DAI, FRIEND, "Dai Stablecoin", "DAI"),                # ordinary sender
    (DAI, ETH_FOUNDATION, "Dai Stablecoin", "DAI"),        # known wallet sending a legit token
    (USDREWARD, FRIEND, "# usdreward.org", "Visit https://usdreward.org to claim rewards"),
])
def test_not_spoofed_sender(contract, frm, name, symbol):
    transfers = [transfer(contract, frm, CENTRAL, name, symbol, 1)]

    assert token_classifier.find_spoofed_sender_contracts(transfers) == set()


def test_classify_tokens_adds_spoofed_sender_tag():
    transfers = [transfer(USDREWARD, USDC, CENTRAL, "# usdreward.org", "Visit https://usdreward.org to claim rewards", 1)]

    result = token_classifier.classify_tokens(transfers, [], CENTRAL)

    assert result == {USDREWARD: {token_classifier.TAG_PHISHING, token_classifier.TAG_UNSOLICITED,
                                  token_classifier.TAG_SPOOFED_SENDER}}


def test_spoofed_sender_token_contract_is_tagged_and_transfers_are_kept(db):
    t = transfer(USDREWARD, USDC, CENTRAL, "# usdreward.org", "Visit https://usdreward.org to claim rewards", 1)
    insert_transfer(db, t)

    with closing(sqlite3.connect(db)) as conn:
        eth.db_store_tagging_opt(conn, CENTRAL, [], [t], [], [], [])
        conn.commit()

    assert ("spam-spoofed-sender",) in stored_tags(db, USDREWARD)
    assert ("spam-spoofed-sender",) not in stored_tags(db, USDC)
    assert stored_transfers(db) == [("0xt1", USDREWARD)]


# ---- Integration with db_store_tagging_opt (S1, S4 support) ----

def stored_tags(db, address):
    with closing(sqlite3.connect(db)) as conn:
        return sorted(conn.execute("SELECT tag FROM t_tags WHERE address = ?", (address,)).fetchall())


def stored_transfers(db):
    with closing(sqlite3.connect(db)) as conn:
        return conn.execute("SELECT hash, contractAddress FROM t_transfers ORDER BY hash").fetchall()


def insert_transfer(db, t):
    row = {
        "nonce": 0, "blockHash": "0xbh", "transactionIndex": 0, "gas": 0, "gasPrice": 0, "gasUsed": 0,
        "cumulativeGasUsed": 0, "confirmations": 0, "methodId": "0x", "functionName": "",
        **t,
    }
    with closing(sqlite3.connect(db)) as conn:
        conn.execute(
            """INSERT INTO t_transfers
               (blockChain, blockNumber, timeStamp, hash, nonce, blockHash, `from`, `to`, value, input,
                contractAddress, tokenDecimal, tokenSymbol, tokenName, transactionIndex, gas, gasPrice,
                gasUsed, cumulativeGasUsed, confirmations, methodId, functionName)
               VALUES (:blockChain, :blockNumber, :timeStamp, :hash, :nonce, :blockHash, :from, :to, :value,
                :input, :contractAddress, :tokenDecimal, :tokenSymbol, :tokenName, :transactionIndex, :gas,
                :gasPrice, :gasUsed, :cumulativeGasUsed, :confirmations, :methodId, :functionName)""",
            row,
        )
        conn.commit()


def test_phishing_token_contract_is_tagged_and_transfers_are_kept(db):
    # INFO: central also sent the token back so only the phishing criterion applies here
    t_in = transfer(USDREWARD, FRIEND, CENTRAL, "# usdreward.org", "Visit https://usdreward.org to claim rewards", 1)
    t_out = transfer(USDREWARD, CENTRAL, FRIEND, "# usdreward.org", "Visit https://usdreward.org to claim rewards", 2)
    insert_transfer(db, t_in)
    insert_transfer(db, t_out)

    with closing(sqlite3.connect(db)) as conn:
        eth.db_store_tagging_opt(conn, CENTRAL, [], [t_in, t_out], [], [], [])
        conn.commit()

    assert stored_tags(db, USDREWARD) == [("contract",), ("spam-phishing",)]
    assert stored_transfers(db) == [("0xt1", USDREWARD), ("0xt2", USDREWARD)]


def test_unsolicited_token_contract_is_tagged(db):
    t = transfer(HACKER_INU, FRIEND, CENTRAL, "Euler Hacker Inu", "EULERHACKERINU", 1)
    insert_transfer(db, t)

    with closing(sqlite3.connect(db)) as conn:
        eth.db_store_tagging_opt(conn, CENTRAL, [], [t], [], [], [])
        conn.commit()

    assert stored_tags(db, HACKER_INU) == [("contract",), ("spam-unsolicited",)]


def test_nodes_links_filter_can_exclude_spam_tagged_tokens(db):
    # INFO: S4 - same generic filters["tags"] path used by central/path/wallet/contract/funder (core/eth.py get_nodes_links_bd)
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("INSERT INTO t_nodes_classification VALUES ('0xa', '0xa', ?, '[]')", ('["central"]',))
        conn.execute("INSERT INTO t_nodes_classification VALUES (?, ?, ?, '[]')", (USDREWARD, USDREWARD, '["contract", "spam-phishing"]'))
        conn.execute("INSERT INTO t_nodes_classification VALUES (?, ?, ?, '[]')", (FRIEND, FRIEND, '["wallet"]'))
        conn.commit()

        result = eth.get_nodes_links_bd(
            conn, ("eth", "0xa", "central"),
            params={"filters": {"filter": True, "tags": {"wallet": True, "central": True}}},
        )

    # INFO: filters["tags"] matches by inclusion (OR); the token node also carries "contract",
    # so leaving "contract" unselected (same as today, for any contract) is what excludes it here
    addresses = {node["address"] for node in result["transactions"]["nodes"]}
    assert addresses == {"0xa", FRIEND}
    assert USDREWARD not in addresses


def test_existing_tags_for_an_address_are_unchanged(db):
    t_dai_in = transfer(DAI, FRIEND, CENTRAL, "Dai Stablecoin", "DAI", 1)
    t_dai_out = transfer(DAI, CENTRAL, FRIEND, "Dai Stablecoin", "DAI", 2)
    insert_transfer(db, t_dai_in)
    insert_transfer(db, t_dai_out)

    with closing(sqlite3.connect(db)) as conn:
        eth.db_store_tagging_opt(conn, CENTRAL, [], [t_dai_in, t_dai_out], [], [], [])
        conn.commit()

    assert stored_tags(db, DAI) == [("contract",)]
