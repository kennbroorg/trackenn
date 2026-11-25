"""Homoglyph token-impersonation classifier rule (core/token_classifier.py, S6,
odd/tasks/backend-backlog-c.md T7). Cyrillic/Greek look-alikes of Latin letters used to
imitate a pinned Uniswap-list token or the chain's native coin.
"""
from core import token_classifier
from test_token_classifier import CENTRAL, FRIEND, transfer

FAKE_WBTC = "0x2e57249f1e5ac0bd3b9187d19120d54008b3d967"
WBTC = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"


def test_homoglyph_wbtc_poisoning_contract_is_flagged():
    transfers = [transfer(FAKE_WBTC, FRIEND, CENTRAL, "Wrapp\u0435d \u0412\u0422C", "W\u0412\u0422C", 1)]

    assert token_classifier.find_homoglyph_contracts(transfers) == {FAKE_WBTC}


def test_real_wbtc_is_not_flagged():
    transfers = [transfer(WBTC, FRIEND, CENTRAL, "Wrapped BTC", "WBTC", 1)]

    assert token_classifier.find_homoglyph_contracts(transfers) == set()


def test_homoglyph_classify_tokens_adds_tag():
    transfers = [
        transfer(FAKE_WBTC, FRIEND, CENTRAL, "Wrapp\u0435d \u0412\u0422C", "W\u0412\u0422C", 1),
        transfer(FAKE_WBTC, CENTRAL, FRIEND, "Wrapp\u0435d \u0412\u0422C", "W\u0412\u0422C", 2),
    ]

    result = token_classifier.classify_tokens(transfers, [], CENTRAL)

    assert result == {FAKE_WBTC: {token_classifier.TAG_HOMOGLYPH}}


def test_fully_cyrillic_name_imitating_nothing_is_not_flagged():
    # INFO: H3 - skeleton maps to Latin letters, but matches no known token/native coin
    contract = "0x1111111111111111111111111111111111111b"
    transfers = [transfer(contract, FRIEND, CENTRAL, "\u0421\u043e\u0432\u0435\u0442\u0441\u043a\u0438\u0439", "SOV", 1)]

    assert token_classifier.find_homoglyph_contracts(transfers) == set()


def test_pure_ascii_classify_tokens_result_is_unchanged():
    # INFO: H5 - pinned existing case, pure ASCII, must give exactly today's result
    transfers = [
        transfer("0xUSDREWARD", FRIEND, CENTRAL, "# usdreward.org", "Visit https://usdreward.org to claim rewards", 1),
        transfer("0xHACKERINU", FRIEND, CENTRAL, "Euler Hacker Inu", "EULERHACKERINU", 2),
    ]

    result = token_classifier.classify_tokens(transfers, [], CENTRAL)

    assert result == {"0xUSDREWARD": {token_classifier.TAG_PHISHING, token_classifier.TAG_UNSOLICITED}}


FAKE_ETH_HOMOGLYPH = "0x3333333333333333333333333333333333333c"


def test_native_coin_homoglyph_is_flagged():
    # INFO: native is never an ERC-20, so it is always flagged regardless of official address
    transfers = [transfer(FAKE_ETH_HOMOGLYPH, FRIEND, CENTRAL, "\u0415ther", "\u0415TH", 1)]

    assert token_classifier.find_homoglyph_contracts(transfers) == {FAKE_ETH_HOMOGLYPH}


def test_spam_unsolicited_confirmed_by_homoglyph_alone():
    transfers = [transfer(FAKE_WBTC, FRIEND, CENTRAL, "Wrapp\u0435d \u0412\u0422C", "W\u0412\u0422C", 1)]

    result = token_classifier.classify_tokens(transfers, [], CENTRAL)

    assert result == {FAKE_WBTC: {token_classifier.TAG_HOMOGLYPH, token_classifier.TAG_UNSOLICITED}}


def test_homoglyph_evidence_contains_text_skeleton_symbol_contract_and_replacements():
    transfers = [transfer(FAKE_WBTC, FRIEND, CENTRAL, "Wrapp\u0435d \u0412\u0422C", "W\u0412\u0422C", 1)]

    _, evidence = token_classifier.classify_tokens_with_evidence(transfers, [], CENTRAL)

    info = evidence[FAKE_WBTC]
    assert info["rule"] == token_classifier.TAG_HOMOGLYPH
    trigger = info["trigger"]
    assert trigger["text"] == "W\u0412\u0422C"
    assert trigger["skeleton"] == "WBTC"
    assert trigger["imitated_symbol"] == "WBTC"
    assert trigger["official_contract"] == WBTC
    assert "\u0412 U+0412 -> B" in trigger["replaced_chars"]
    assert "\u0422 U+0422 -> T" in trigger["replaced_chars"]
