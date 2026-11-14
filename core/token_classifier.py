"""Internal spam/phishing classifier for ERC-20 token contracts.

Pure functions: no I/O, no DB access. Inputs are plain lists of dicts in the
same shape as the explorer API responses already used across core/eth.py
(transfers: from/to/contractAddress/tokenName/tokenSymbol; trxs: from/to).
"""
import re

TAG_PHISHING = "spam-phishing"
TAG_UNSOLICITED = "spam-unsolicited"

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


def classify_tokens(transfers, trxs, central_address):
    """Return {contractAddress: set(tags)} for contracts worth flagging as spam."""
    tags_by_contract = {}
    for contract in find_phishing_contracts(transfers):
        tags_by_contract.setdefault(contract, set()).add(TAG_PHISHING)
    for contract in find_unsolicited_contracts(transfers, trxs, central_address):
        tags_by_contract.setdefault(contract, set()).add(TAG_UNSOLICITED)
    return tags_by_contract
