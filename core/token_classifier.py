"""Internal spam/phishing classifier for ERC-20 token contracts.

Pure functions: no I/O, no DB access. Inputs are plain lists of dicts in the
same shape as the explorer API responses already used across core/eth.py
(transfers: from/to/contractAddress/tokenName/tokenSymbol; trxs: from/to).
"""
import re

TAG_PHISHING = "spam-phishing"
TAG_UNSOLICITED = "spam-unsolicited"
TAG_SPOOFED_SENDER = "spam-spoofed-sender"
TAG_NATIVE_IMPERSONATION = "spam-native-impersonation"

# INFO: The native coin of a chain is never itself an ERC-20 token, so any token
#       contract whose symbol/name equals it is impersonating it (S3). Per chain,
#       defaulting to eth when a transfer carries no blockChain (same default the
#       rest of core/eth.py assumes for single-chain data).
DEFAULT_CHAIN = "eth"
NATIVE_COINS = {
    "eth": {"symbol": "eth", "names": {"ether", "ethereum"}},
}

# INFO: Official Ethereum mainnet token contracts. A token contract never sends
#       units of a different token, so a transfer of token X "from" one of these
#       is an event forged by X's contract to look legitimate.
KNOWN_TOKEN_CONTRACTS = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
    "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",  # WBTC
    "0xae7ab96520de3a18e5e111b5eaab095312d7fe84",  # stETH
}

# INFO: Well-known wallets that spoofers impersonate as sender. They may send
#       the official tokens above, any other token "from" them is treated as forged.
KNOWN_IMPERSONATED_WALLETS = {
    "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae",  # Ethereum Foundation
}

_URL_RE = re.compile(
    r"https?://\S+"
    r"|www\.\S+"
    r"|\b[a-z0-9-]+\.(?:com|org|net|io|xyz|info|click|top|trade|app|site|online|club|biz|vip|live|fun)\b",
    re.IGNORECASE,
)
_LURE_WORDS_RE = re.compile(r"\b(?:claim|reward|rewards|visit)\b", re.IGNORECASE)


def is_phishing_text(token_name, token_symbol):
    """True when a token name or symbol embeds a URL/domain or lure words."""
    for text in (token_name, token_symbol):
        if not text:
            continue
        if _URL_RE.search(text) or _LURE_WORDS_RE.search(text):
            return True
    return False


def find_phishing_contracts(transfers):
    """Contract addresses whose token name/symbol match the phishing criterion (A)."""
    contracts = set()
    for transfer in transfers:
        contract = transfer.get("contractAddress")
        if not contract:
            continue
        if is_phishing_text(transfer.get("tokenName"), transfer.get("tokenSymbol")):
            contracts.add(contract)
    return contracts


def find_unsolicited_contracts(transfers, trxs, central_address):
    """Contract addresses the central address only ever received (criterion D).

    A contract qualifies when the central address appears as `to` in at least
    one transfer of that token, never as `from` in any transfer of it, and
    never sent a normal transaction to the contract itself.
    """
    central_address = (central_address or "").lower()
    received = set()
    sent = set()
    for transfer in transfers:
        contract = transfer.get("contractAddress")
        if not contract:
            continue
        frm = (transfer.get("from") or "").lower()
        to = (transfer.get("to") or "").lower()
        if to == central_address:
            received.add(contract)
        if frm == central_address:
            sent.add(contract)

    called = {
        trx.get("to")
        for trx in trxs
        if (trx.get("from") or "").lower() == central_address and trx.get("to")
    }

    return received - sent - called


def find_spoofed_sender_contracts(transfers):
    """Contract addresses whose transfers claim a sender that cannot have sent them (S6).

    Either a known token contract appears as sender of a different token, or a
    known impersonated wallet appears as sender of a token that is not official.
    """
    contracts = set()
    for transfer in transfers:
        contract = (transfer.get("contractAddress") or "").lower()
        frm = (transfer.get("from") or "").lower()
        if not contract or not frm:
            continue
        forged_by_token = frm in KNOWN_TOKEN_CONTRACTS and frm != contract
        forged_by_wallet = frm in KNOWN_IMPERSONATED_WALLETS and contract not in KNOWN_TOKEN_CONTRACTS
        if forged_by_token or forged_by_wallet:
            contracts.add(transfer["contractAddress"])
    return contracts


def find_native_impersonation_contracts(transfers):
    """Contract addresses whose token name/symbol match the chain's native coin (S3).

    The native coin is never an ERC-20 token, so any contract advertising its
    symbol/name (case-insensitive, trimmed) is forged, regardless of sender.
    """
    contracts = set()
    for transfer in transfers:
        contract = transfer.get("contractAddress")
        if not contract:
            continue
        chain = (transfer.get("blockChain") or DEFAULT_CHAIN).lower()
        native = NATIVE_COINS.get(chain)
        if not native:
            continue
        symbol = (transfer.get("tokenSymbol") or "").strip().lower()
        name = (transfer.get("tokenName") or "").strip().lower()
        if symbol == native["symbol"] or name in native["names"]:
            contracts.add(contract)
    return contracts


def classify_tokens(transfers, trxs, central_address):
    """Return {contractAddress: set(tags)} for contracts worth flagging as spam."""
    tags_by_contract = {}
    for contract in find_phishing_contracts(transfers):
        tags_by_contract.setdefault(contract, set()).add(TAG_PHISHING)
    for contract in find_spoofed_sender_contracts(transfers):
        tags_by_contract.setdefault(contract, set()).add(TAG_SPOOFED_SENDER)
    for contract in find_native_impersonation_contracts(transfers):
        tags_by_contract.setdefault(contract, set()).add(TAG_NATIVE_IMPERSONATION)

    # INFO: S2 - "only received" alone is not a signal; it only confirms an existing
    # strong signal for the same contract (TAG_PHISHING/TAG_SPOOFED_SENDER/TAG_NATIVE_IMPERSONATION).
    unsolicited = find_unsolicited_contracts(transfers, trxs, central_address)
    for contract in unsolicited:
        if tags_by_contract.get(contract):
            tags_by_contract[contract].add(TAG_UNSOLICITED)

    return tags_by_contract


def _text_matching(name, symbol, matches):
    """Whichever of name/symbol (original value, not normalized) satisfies `matches` first."""
    if name and matches(name):
        return name
    if symbol and matches(symbol):
        return symbol
    return None


def find_phishing_triggers(transfers):
    """Contract -> the token name/symbol text that matched the phishing criterion (A)."""
    triggers = {}
    for transfer in transfers:
        contract = transfer.get("contractAddress")
        if not contract or contract in triggers:
            continue
        trigger = _text_matching(
            transfer.get("tokenName"), transfer.get("tokenSymbol"),
            lambda text: bool(_URL_RE.search(text) or _LURE_WORDS_RE.search(text)))
        if trigger:
            triggers[contract] = trigger
    return triggers


def _matches_native(text, native):
    normalized = text.strip().lower()
    return normalized in native["names"] or normalized == native["symbol"]


def find_native_impersonation_triggers(transfers):
    """Contract -> the token name/symbol text that matched the chain's native coin (S3)."""
    triggers = {}
    for transfer in transfers:
        contract = transfer.get("contractAddress")
        if not contract or contract in triggers:
            continue
        chain = (transfer.get("blockChain") or DEFAULT_CHAIN).lower()
        native = NATIVE_COINS.get(chain)
        if not native:
            continue
        name, symbol = transfer.get("tokenName"), transfer.get("tokenSymbol")
        if name and _matches_native(name, native):
            triggers[contract] = name
        elif symbol and _matches_native(symbol, native):
            triggers[contract] = symbol
    return triggers


def find_spoofed_sender_triggers(transfers):
    """Contract -> the spoofed `from` address that forged its transfer (S6)."""
    triggers = {}
    for transfer in transfers:
        contract = transfer.get("contractAddress")
        frm = transfer.get("from")
        if not contract or not frm or contract in triggers:
            continue
        contract_lower, frm_lower = contract.lower(), frm.lower()
        forged_by_token = frm_lower in KNOWN_TOKEN_CONTRACTS and frm_lower != contract_lower
        forged_by_wallet = frm_lower in KNOWN_IMPERSONATED_WALLETS and contract_lower not in KNOWN_TOKEN_CONTRACTS
        if forged_by_token or forged_by_wallet:
            triggers[contract] = frm
    return triggers


# INFO: Priority order when a contract carries more than one strong signal; matches the
# order classify_tokens applies them, so the rule reported is the first one that fired.
_TRIGGER_RULES = (
    (TAG_PHISHING, find_phishing_triggers),
    (TAG_SPOOFED_SENDER, find_spoofed_sender_triggers),
    (TAG_NATIVE_IMPERSONATION, find_native_impersonation_triggers),
)


def classify_tokens_with_evidence(transfers, trxs, central_address):
    """classify_tokens plus, per flagged contract, the rule and value that fired it
    (decision-evidence-retrofit T1): {contract: {"rule": tag, "trigger": value}}.

    Never changes which contracts/tags classify_tokens returns (S5); it only reports
    evidence for the tags it already produced.
    """
    tags_by_contract = classify_tokens(transfers, trxs, central_address)
    triggers_by_rule = {tag: find_triggers(transfers) for tag, find_triggers in _TRIGGER_RULES}

    evidence = {}
    for contract, tags in tags_by_contract.items():
        for rule, _ in _TRIGGER_RULES:
            trigger = triggers_by_rule[rule].get(contract)
            if rule in tags and trigger is not None:
                evidence[contract] = {"rule": rule, "trigger": trigger}
                break
    return tags_by_contract, evidence
