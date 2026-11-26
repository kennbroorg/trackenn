"""Homoglyph token-impersonation classifier rule (core/token_classifier.py, S6/S8,
odd/tasks/backend-backlog-c.md T7/T8). Cyrillic/Greek look-alikes of Latin letters, plus
normalization coverage (NFKC, invisible-character stripping) used to imitate a pinned
Uniswap-list token or the chain's native coin.
"""
import pytest

from core import token_classifier
from test_classifier_lists import fake_token_lists
from test_token_classifier import CENTRAL, FRIEND, transfer

FAKE_WBTC = "0x2e57249f1e5ac0bd3b9187d19120d54008b3d967"
WBTC = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"


@pytest.fixture(autouse=True)
def clear_homoglyph_caches():
    """S8 (G5): the pinned token index is cached like the other classifier lists
    (token_classifier._cache), so each test needs a clean cache and build counter."""
    token_classifier._cache.clear()
    token_classifier._load_problems.clear()
    token_classifier._token_index_builds = 0
    yield
    token_classifier._cache.clear()
    token_classifier._load_problems.clear()
    token_classifier._token_index_builds = 0


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


# ---- S8 (T8): strip Cf -> NFKC -> confusables table -> compare ----

def test_fullwidth_wbtc_is_flagged_imitating_wbtc():
    # INFO: G1 - full-width Latin (NFKC) collapses to WBTC
    contract = "0x4444444444444444444444444444444444444d"
    transfers = [transfer(contract, FRIEND, CENTRAL, "\uff37\uff22\uff34\uff23 Coin", "\uff37\uff22\uff34\uff23", 1)]

    assert token_classifier.find_homoglyph_contracts(transfers) == {contract}

    triggers = token_classifier.find_homoglyph_triggers(transfers)
    info = triggers[contract]
    assert info["skeleton"] == "WBTC"
    assert info["official_contract"] == WBTC
    assert info["normalization"] == "NFKC"
    assert "\uff37 U+FF37 -> W" in info["replaced_chars"]
    assert "\uff22 U+FF22 -> B" in info["replaced_chars"]
    assert "\uff34 U+FF34 -> T" in info["replaced_chars"]
    assert "\uff23 U+FF23 -> C" in info["replaced_chars"]


def test_mathematical_bold_wbtc_is_flagged_imitating_wbtc():
    # INFO: G1 - mathematical alphanumeric symbols (NFKC) collapse to WBTC
    contract = "0x4444444444444444444444444444444444444e"
    transfers = [transfer(contract, FRIEND, CENTRAL, "\U0001d41c\U0001d401\U0001d413\U0001d402 Coin", "\U0001d416\U0001d401\U0001d413\U0001d402", 1)]

    assert token_classifier.find_homoglyph_contracts(transfers) == {contract}

    triggers = token_classifier.find_homoglyph_triggers(transfers)
    info = triggers[contract]
    assert info["skeleton"] == "WBTC"
    assert info["official_contract"] == WBTC
    assert info["normalization"] == "NFKC"
    assert "\U0001d416 U+1D416 -> W" in info["replaced_chars"]


def test_fullwidth_and_cyrillic_mix_is_flagged_imitating_wbtc():
    # INFO: G1 - a mix of full-width Latin (NFKC) and pinned Cyrillic confusables
    contract = "0x4444444444444444444444444444444444444f"
    transfers = [transfer(contract, FRIEND, CENTRAL, "\uff37rapped \u0412TC", "\uff37\u0412TC", 1)]

    assert token_classifier.find_homoglyph_contracts(transfers) == {contract}

    triggers = token_classifier.find_homoglyph_triggers(transfers)
    info = triggers[contract]
    assert info["skeleton"] == "WBTC"
    assert info["official_contract"] == WBTC
    assert info["normalization"] == "NFKC"
    assert "\uff37 U+FF37 -> W" in info["replaced_chars"]
    assert "\u0412 U+0412 -> B" in info["replaced_chars"]


def test_zero_width_space_in_wbtc_is_flagged_on_non_official_contract():
    # INFO: G1 - invisible characters (category Cf) are stripped before comparison
    contract = "0x5555555555555555555555555555555555555a"
    transfers = [transfer(contract, FRIEND, CENTRAL, "WB\u200bTC Coin", "WB\u200bTC", 1)]

    assert token_classifier.find_homoglyph_contracts(transfers) == {contract}

    triggers = token_classifier.find_homoglyph_triggers(transfers)
    info = triggers[contract]
    assert info["skeleton"] == "WBTC"
    assert info["official_contract"] == WBTC
    assert info["normalization"] == "NFKC"
    assert "U+200B -> (removed)" in info["replaced_chars"]


def test_official_wbtc_contract_is_never_flagged_even_with_nonascii_skeleton_self_match():
    # INFO: G2 - the official contract's own non-ASCII name/symbol, whose skeleton equals
    # its own official symbol, must never be flagged (reaches the official-contract check)
    transfers = [transfer(WBTC, FRIEND, CENTRAL, "\uff37rapped BTC", "\uff37BTC", 1)]

    assert token_classifier.find_homoglyph_contracts(transfers) == set()


def test_victim_wbtc_poisoning_still_flagged_with_same_evidence_plus_normalization():
    # INFO: G3 - existing victim case keeps its evidence, plus the new normalization field
    transfers = [transfer(FAKE_WBTC, FRIEND, CENTRAL, "Wrapp\u0435d \u0412\u0422C", "W\u0412\u0422C", 1)]

    triggers = token_classifier.find_homoglyph_triggers(transfers)
    info = triggers[FAKE_WBTC]
    assert info["text"] == "W\u0412\u0422C"
    assert info["skeleton"] == "WBTC"
    assert info["imitated_symbol"] == "WBTC"
    assert info["official_contract"] == WBTC
    assert "\u0412 U+0412 -> B" in info["replaced_chars"]
    assert "\u0422 U+0422 -> T" in info["replaced_chars"]
    assert info["normalization"] == "NFKC"


def test_pure_ascii_classify_tokens_result_is_unchanged_s8():
    # INFO: G3 - pure-ASCII input must still give exactly today's result
    transfers = [
        transfer("0xUSDREWARD", FRIEND, CENTRAL, "# usdreward.org", "Visit https://usdreward.org to claim rewards", 1),
        transfer("0xHACKERINU", FRIEND, CENTRAL, "Euler Hacker Inu", "EULERHACKERINU", 2),
    ]

    result = token_classifier.classify_tokens(transfers, [], CENTRAL)

    assert result == {"0xUSDREWARD": {token_classifier.TAG_PHISHING, token_classifier.TAG_UNSOLICITED}}


def test_token_index_is_built_once_per_process(monkeypatch, tmp_path):
    # INFO: G5 - the symbol/name -> official contract index is built once per process
    fake_token_lists(monkeypatch, tmp_path, [
        {"address": WBTC, "symbol": "WBTC", "name": "Wrapped BTC"},
    ])
    transfers = [
        transfer(FAKE_WBTC, FRIEND, CENTRAL, "Wrapp\u0435d \u0412\u0422C", "W\u0412\u0422C", 1),
        transfer("0x4444444444444444444444444444444444444d", FRIEND, CENTRAL, "\uff37\uff22\uff34\uff23 Coin", "\uff37\uff22\uff34\uff23", 2),
    ]

    token_classifier.find_homoglyph_triggers(transfers)

    assert token_classifier._token_index_builds == 1
