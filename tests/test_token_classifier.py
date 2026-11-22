"""Internal spam/phishing token classifier (core/token_classifier.py, wired into
core/eth.py db_store_tagging_opt and the cached path, S7). Fixtures from the Euler
case (documents/EULER.md).
"""
import sqlite3
from contextlib import closing

import pytest

from core import eth, misc, token_classifier
from test_sql_injection import insert, run_stream, stream_params

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
    # INFO: S2 - HACKER_INU is only-received with no strong signal, so it gets no tag
    transfers = [
        transfer(USDREWARD, FRIEND, CENTRAL, "# usdreward.org", "Visit https://usdreward.org to claim rewards", 1),
        transfer(HACKER_INU, FRIEND, CENTRAL, "Euler Hacker Inu", "EULERHACKERINU", 2),
        transfer(DAI, FRIEND, CENTRAL, "Dai Stablecoin", "DAI", 3),
        transfer(DAI, CENTRAL, FRIEND, "Dai Stablecoin", "DAI", 4),
    ]

    result = token_classifier.classify_tokens(transfers, [], CENTRAL)

    assert result == {
        USDREWARD: {token_classifier.TAG_PHISHING, token_classifier.TAG_UNSOLICITED},
    }


# ---- S2: unsolicited never tags alone, only confirms a strong signal ----

def test_only_received_legit_looking_token_gets_no_tag():
    transfers = [transfer(HACKER_INU, FRIEND, CENTRAL, "Euler Hacker Inu", "EULERHACKERINU", 1)]

    assert token_classifier.classify_tokens(transfers, [], CENTRAL) == {}


def test_only_received_phishing_token_gets_phishing_and_unsolicited():
    transfers = [transfer(USDREWARD, FRIEND, CENTRAL, "# usdreward.org",
                           "Visit https://usdreward.org to claim rewards", 1)]

    result = token_classifier.classify_tokens(transfers, [], CENTRAL)

    assert result == {USDREWARD: {token_classifier.TAG_PHISHING, token_classifier.TAG_UNSOLICITED}}


def test_only_received_spoofed_sender_token_gets_spoofed_and_unsolicited():
    transfers = [transfer(HACKER_INU, USDC, CENTRAL, "Euler Hacker Inu", "EULERHACKERINU", 1)]

    result = token_classifier.classify_tokens(transfers, [], CENTRAL)

    assert result == {HACKER_INU: {token_classifier.TAG_SPOOFED_SENDER, token_classifier.TAG_UNSOLICITED}}


# ---- S3: native coin impersonation ----

FAKE_ETH = "0x739352337c902c3874b95f14e81ebbcf1b7b262e"
STETH = "0xae7ab96520de3a18e5e111b5eaab095312d7fe84"
ENS = "0xfac7bea255a6990f749363002136af6556b31e04"


def test_fake_erc20_named_ether_eth_is_native_impersonation():
    transfers = [transfer(FAKE_ETH, FRIEND, CENTRAL, "Ether", "ETH", 1)]

    assert token_classifier.find_native_impersonation_contracts(transfers) == {FAKE_ETH}


def test_fake_erc20_named_ether_eth_classified_with_unsolicited_when_only_received():
    transfers = [transfer(FAKE_ETH, FRIEND, CENTRAL, "Ether", "ETH", 1)]

    result = token_classifier.classify_tokens(transfers, [], CENTRAL)

    assert result == {FAKE_ETH: {token_classifier.TAG_NATIVE_IMPERSONATION, token_classifier.TAG_UNSOLICITED}}


@pytest.mark.parametrize("name, symbol", [
    ("Wrapped Ether", "WETH"),
    ("Lido Staked Ether", "stETH"),
    ("Ethereum Name Service", "ENS"),
])
def test_legit_tokens_do_not_match_native_impersonation(name, symbol):
    transfers = [transfer(STETH, FRIEND, CENTRAL, name, symbol, 1)]

    assert token_classifier.find_native_impersonation_contracts(transfers) == set()


@pytest.mark.parametrize("name, symbol", [
    ("ether", "eth"),
    ("  Ether  ", "  ETH  "),
    ("ETHEREUM", "Eth"),
])
def test_native_impersonation_is_case_insensitive_and_trimmed(name, symbol):
    transfers = [transfer(FAKE_ETH, FRIEND, CENTRAL, name, symbol, 1)]

    assert token_classifier.find_native_impersonation_contracts(transfers) == {FAKE_ETH}


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


def test_unsolicited_alone_is_no_longer_tagged(db):
    # INFO: S2 - only-received with no strong signal (phishing/spoofed-sender/native-impersonation) gets no tag
    t = transfer(HACKER_INU, FRIEND, CENTRAL, "Euler Hacker Inu", "EULERHACKERINU", 1)
    insert_transfer(db, t)

    with closing(sqlite3.connect(db)) as conn:
        eth.db_store_tagging_opt(conn, CENTRAL, [], [t], [], [], [])
        conn.commit()

    assert stored_tags(db, HACKER_INU) == [("contract",)]


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


# ---- S7: classifier on the cached path + Complex node tag sync ----

def seed_cached_case(db):
    # INFO: Euler phishing token (from USDC) + an unsolicited token + a legit DAI token
    # the central both received and sent, stored exactly as the download path would.
    insert_transfer(db, transfer(USDREWARD, USDC, CENTRAL, "# usdreward.org", "Visit https://usdreward.org to claim rewards", 1))
    insert_transfer(db, transfer(HACKER_INU, FRIEND, CENTRAL, "Euler Hacker Inu", "EULERHACKERINU", 2))
    insert_transfer(db, transfer(DAI, FRIEND, CENTRAL, "Dai Stablecoin", "DAI", 3))
    insert_transfer(db, transfer(DAI, CENTRAL, FRIEND, "Dai Stablecoin", "DAI", 4))


def test_cached_path_classifies_stored_transfers_and_is_idempotent(db):
    seed_cached_case(db)

    with closing(sqlite3.connect(db)) as conn:
        eth.classify_cached_tokens(conn, ("eth", CENTRAL, "central"))
        conn.commit()

    assert stored_tags(db, USDREWARD) == sorted([
        (token_classifier.TAG_PHISHING,),
        (token_classifier.TAG_SPOOFED_SENDER,),
        (token_classifier.TAG_UNSOLICITED,),
    ])
    # INFO: S2 - HACKER_INU is only-received with no strong signal, no tag
    assert stored_tags(db, HACKER_INU) == []
    assert stored_tags(db, DAI) == []

    with closing(sqlite3.connect(db)) as conn:
        eth.classify_cached_tokens(conn, ("eth", CENTRAL, "central"))
        conn.commit()

    assert stored_tags(db, USDREWARD) == sorted([
        (token_classifier.TAG_PHISHING,),
        (token_classifier.TAG_SPOOFED_SENDER,),
        (token_classifier.TAG_UNSOLICITED,),
    ])


def test_cached_path_merges_tags_into_existing_node_rows_without_duplicates(db):
    seed_cached_case(db)
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("INSERT INTO t_nodes_classification VALUES (?, ?, ?, '[]')", (USDREWARD, USDREWARD, '["contract", "path"]'))
        conn.execute("INSERT INTO t_nodes_classification VALUES (?, ?, ?, '[]')", (HACKER_INU, HACKER_INU, '["contract"]'))
        conn.commit()

        eth.classify_cached_tokens(conn, ("eth", CENTRAL, "central"))
        conn.commit()

        def node_tags(address):
            import json
            return json.loads(conn.execute("SELECT tag FROM t_nodes_classification WHERE address = ?", (address,)).fetchone()[0])

        usdreward_tags = node_tags(USDREWARD)
        hacker_inu_tags = node_tags(HACKER_INU)

        assert set(usdreward_tags) == {"contract", "path", token_classifier.TAG_PHISHING,
                                       token_classifier.TAG_SPOOFED_SENDER, token_classifier.TAG_UNSOLICITED}
        assert "contract" in usdreward_tags and "path" in usdreward_tags  # INFO: existing tags never removed
        # INFO: S2 - HACKER_INU is only-received with no strong signal, no tag added
        assert set(hacker_inu_tags) == {"contract"}

        # INFO: run again, idempotent, no duplicate entries in the JSON list
        eth.classify_cached_tokens(conn, ("eth", CENTRAL, "central"))
        conn.commit()
        usdreward_tags_again = node_tags(USDREWARD)

    assert sorted(usdreward_tags_again) == sorted(usdreward_tags)
    assert len(usdreward_tags_again) == len(set(usdreward_tags_again))


def seed_cached_stream(db):
    insert(db, "t_tags", blockChain="eth", address=CENTRAL, tag="central")
    insert(db, "t_tags", blockChain="eth", address=CENTRAL, tag="path")
    insert(db, "t_address_detail", blockChain="eth", address=CENTRAL, type="wallet")
    insert(db, "t_transactions", blockChain="eth", hash="0xeth1", **{"from": CENTRAL, "to": FRIEND},
           value=10**18, gasPrice=10**9, gasUsed=21000, isError=0)
    seed_cached_case(db)


def test_stream_cached_path_stores_tags_and_still_ends(db, monkeypatch):
    seed_cached_stream(db)

    messages = run_stream(eth.event_stream_ether, stream_params(db, CENTRAL, "eth"))

    assert messages[-1] == {"msg": "End collection", "end": True, "error": False, "content": {}}
    assert stored_tags(db, USDREWARD) != []
    assert stored_tags(db, DAI) == []


def test_stream_cached_path_survives_classifier_exception(db, monkeypatch):
    seed_cached_stream(db)
    monkeypatch.setattr(eth, "classify_cached_tokens", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))

    messages = run_stream(eth.event_stream_ether, stream_params(db, CENTRAL, "eth"))

    assert messages[-1] == {"msg": "End collection", "end": True, "error": False, "content": {}}
    assert "exception" not in [tuple(m.keys()) for m in messages]
    assert stored_tags(db, USDREWARD) == []  # INFO: classifier never ran, but the stream still finished cleanly


def test_checking_stream_cached_path_stores_tags(db):
    # INFO: core/misc.py event_stream_checking passes the t_tags 'central' row to the classifier
    seed_cached_stream(db)
    config = {"ethscan": "K", "bscscan": "K", "level": "INFO", "dbname": str(db), "action": "checking",
              "graph": "Star", "filters": {"filter": False}}

    messages = run_stream(lambda c: misc.event_stream_checking(c), config)

    assert "exception" not in [key for m in messages for key in m]
    assert ("spam-phishing",) in stored_tags(db, USDREWARD)
    assert stored_tags(db, DAI) == []


def test_download_path_classifier_failure_keeps_funder_tags(db, monkeypatch):
    # INFO: a classifier error must not abort db_store_tagging_opt before the funders are stored
    monkeypatch.setattr(token_classifier, "classify_tokens", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    funding = trx(FRIEND, CENTRAL, 1)
    first_out = trx(CENTRAL, DAI, 2)

    with closing(sqlite3.connect(db)) as conn:
        eth.db_store_tagging_opt(conn, CENTRAL, [funding, first_out], [], [], [], [])
        conn.commit()

    assert ("funder",) in stored_tags(db, FRIEND)
