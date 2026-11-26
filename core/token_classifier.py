"""Internal spam/phishing classifier for ERC-20 token contracts.

Pure functions: no I/O, no DB access, other than the one-time lazy load of the data/
snapshot files below (R18, odd/tasks/backend-backlog-c.md T3, S3). Inputs are plain
lists of dicts in the same shape as the explorer API responses already used across
core/eth.py (transfers: from/to/contractAddress/tokenName/tokenSymbol; trxs: from/to).
"""
import json
import logging
import os
import re
import unicodedata

logger = logging.getLogger(__name__)

TAG_PHISHING = "spam-phishing"
TAG_UNSOLICITED = "spam-unsolicited"
TAG_SPOOFED_SENDER = "spam-spoofed-sender"
TAG_NATIVE_IMPERSONATION = "spam-native-impersonation"
TAG_HOMOGLYPH = "spam-homoglyph"

# INFO: S6 (odd/tasks/backend-backlog-c.md T7) - a small pinned table of Cyrillic/Greek
# letters visually confusable with Latin ones, used to compute a token name/symbol's
# "skeleton" (no dependency, no network). Only characters that are actually used to spoof
# Latin look-alikes are listed; most Cyrillic/Greek letters do not resemble a Latin letter
# closely enough to be included here, which is what keeps a genuinely non-Latin name (that
# imitates nothing) from collapsing into a Latin skeleton.
_CONFUSABLES = {
    # Cyrillic uppercase
    "\u0410": "A", "\u0412": "B", "\u0415": "E", "\u041a": "K", "\u041c": "M",
    "\u041d": "H", "\u041e": "O", "\u0420": "P", "\u0421": "C", "\u0422": "T",
    "\u0423": "Y", "\u0425": "X",
    # Cyrillic lowercase
    "\u0430": "a", "\u0435": "e", "\u043e": "o", "\u0440": "p", "\u0441": "c",
    "\u0443": "y", "\u0445": "x", "\u0456": "i",
    # Greek uppercase
    "\u0391": "A", "\u0392": "B", "\u0395": "E", "\u0396": "Z", "\u0397": "H",
    "\u0399": "I", "\u039a": "K", "\u039c": "M", "\u039d": "N", "\u039f": "O",
    "\u03a1": "P", "\u03a4": "T", "\u03a5": "Y", "\u03a7": "X",
    # Greek lowercase
    "\u03bf": "o", "\u03b9": "i", "\u03ba": "k", "\u03bd": "v",
}

# INFO: The native coin of a chain is never itself an ERC-20 token, so any token
#       contract whose symbol/name equals it is impersonating it (S3). Per chain,
#       defaulting to eth when a transfer carries no blockChain (same default the
#       rest of core/eth.py assumes for single-chain data).
DEFAULT_CHAIN = "eth"
NATIVE_COINS = {
    "eth": {"symbol": "eth", "names": {"ether", "ethereum"}},
}

_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
CLASSIFIER_LISTS_PATH = os.path.join(_DATA_DIR, "classifierLists.json")
PHISHING_DOMAINS_PATH = os.path.join(_DATA_DIR, "phishingDomains.json")
TOKEN_LISTS_PATH = os.path.join(_DATA_DIR, "tokenLists.json")
# INFO: the same local Etherscan labels the app already ships; a contract with a label there is
#       a known project token, so a public-blacklist hit on its name is not enough to tag it
#       (live check: the real Tornado Cash vTORN token, "Tornado.Cash Voucher", in Vitalik's wallet)
KNOWN_LABELS_PATH = os.path.join(_DATA_DIR, "etherscanCombinedAllLabels.json")
# INFO: labels/names that mark a contract as bad, never as a known project
_RISKY_LABELS = frozenset({"blocked", "spam-token"})
_RISKY_LABEL_RE = re.compile(r"phish|scam|fake|hack|exploit|heist|drainer|spam", re.IGNORECASE)

# INFO: R18-5 fallback when data/classifierLists.json is missing or invalid: the same 6
#       official Ethereum mainnet token contracts and the Ethereum Foundation wallet this
#       module hardcoded before the data file existed.
_FALLBACK_BASELINE_TOKEN_CONTRACTS = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
    "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",  # WBTC
    "0xae7ab96520de3a18e5e111b5eaab095312d7fe84",  # stETH
}
_FALLBACK_KNOWN_IMPERSONATED_WALLETS = {
    "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae",  # Ethereum Foundation
}

_URL_RE = re.compile(
    r"https?://\S+"
    r"|www\.\S+"
    r"|\b[a-z0-9-]+\.(?:com|org|net|io|xyz|info|click|top|trade|app|site|online|club|biz|vip|live|fun)\b",
    re.IGNORECASE,
)

# INFO: R18-5 fallback when data/classifierLists.json is missing or invalid: the same lure
#       words this module hardcoded before the data file existed (user decision L7: no new
#       lure words added).
_FALLBACK_LURE_WORDS = ("claim", "reward", "rewards", "visit")

# INFO: Only used to find a domain to look up in the public blacklist (data/phishingDomains.json);
#       unlike _URL_RE it accepts any TLD, since the blacklist (not the TLD) is the signal (L7).
_CANDIDATE_DOMAIN_RE = re.compile(
    r"\b(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,24}\b", re.IGNORECASE)

_cache = {}
_load_problems = {}
# INFO: S8 (odd/tasks/backend-backlog-c.md T8, G5) - number of times the symbol/name ->
# official contract index below was actually (re)built, so a test can assert it is built
# once per process; not reset by _cache.clear() alone, only read for that assertion.
_token_index_builds = 0


def _load_json(path):
    """Read and parse `path` once per process (R18-4: no network/disk access at runtime
    beyond this one-time load). A missing/unreadable/invalid file is recorded in
    list_load_problems() and treated as absent (R18-5), so callers fall back to the
    hardcoded baseline instead of raising."""
    if path in _cache:
        return _cache[path]
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, ValueError) as error:
        reason = str(error)
        logger.warning("Could not load classifier data file %s: %s", path, reason)
        _load_problems[path] = reason
        data = None
    _cache[path] = data
    return data


def list_load_problems():
    """[{"file", "reason"}] for every data/ snapshot that failed to load (R18-5), so the
    caller can turn each into evidence instead of silently classifying with less data."""
    return [{"file": os.path.basename(path), "reason": reason} for path, reason in _load_problems.items()]


def _classifier_lists():
    return _load_json(CLASSIFIER_LISTS_PATH)


def _baseline_token_contracts():
    data = _classifier_lists() or {}
    addresses = data.get("baselineTokenContracts", {}).get("addresses")
    if not addresses:
        return frozenset(_FALLBACK_BASELINE_TOKEN_CONTRACTS)
    return frozenset(address.lower() for address in addresses)


def _known_impersonated_wallets():
    data = _classifier_lists() or {}
    addresses = data.get("knownImpersonatedWallets", {}).get("addresses")
    if not addresses:
        return frozenset(_FALLBACK_KNOWN_IMPERSONATED_WALLETS)
    return frozenset(address.lower() for address in addresses)


def _phishing_domains():
    """The public blacklist (data/phishingDomains.json). Missing/broken file -> empty set,
    i.e. only the unchanged _URL_RE/lure-word signal applies (R18-5 baseline behaviour)."""
    data = _load_json(PHISHING_DOMAINS_PATH) or {}
    return frozenset(data.get("domains") or ())


def _token_list_addresses():
    """Contract addresses from the pinned token list (data/tokenLists.json). Missing/broken
    file -> empty set, i.e. only the baseline contracts apply (R18-5 baseline behaviour)."""
    data = _load_json(TOKEN_LISTS_PATH) or {}
    return frozenset(token["address"].lower() for token in data.get("tokens") or () if token.get("address"))


def _known_token_contracts():
    """Official token contracts for the spoofed-sender rule (S6): the baseline contracts
    always apply, the pinned token list (data/tokenLists.json) extends them (item 3)."""
    return _baseline_token_contracts() | _token_list_addresses()


def _token_list_by_symbol_and_name():
    """{lowercased symbol/name: official contract address} from the pinned token list
    (data/tokenLists.json), used by the homoglyph rule (S6) to find which official token a
    skeleton imitates. Missing/broken file -> empty dict, so the rule simply finds no match.

    Built once per process (S8, G5): cached in the shared `_cache` dict, keyed off the data
    file path like the other classifier lists, so clearing `_cache` (as the existing tests
    already do when they swap the data file) also invalidates this index."""
    cache_key = ("token_index", TOKEN_LISTS_PATH)
    if cache_key in _cache:
        return _cache[cache_key]
    data = _load_json(TOKEN_LISTS_PATH) or {}
    by_text = {}
    for token in data.get("tokens") or ():
        address = token.get("address")
        if not address:
            continue
        for text in (token.get("symbol"), token.get("name")):
            if text:
                by_text.setdefault(text.strip().lower(), address.lower())
    _cache[cache_key] = by_text
    global _token_index_builds
    _token_index_builds += 1
    return by_text


# INFO: S8 (T8, G1) - order: strip Unicode format characters (category Cf: zero-width,
# direction marks) -> NFKC (stdlib unicodedata; collapses full-width ＷＢＴＣ and mathematical
# alphanumeric 𝐖𝐁𝐓𝐂 to plain Latin) -> the pinned Cyrillic/Greek confusables table. Applied
# one original character at a time (instead of normalizing the whole string at once) so the
# evidence below can report exactly which original character changed, in reading order.
def _skeleton_char(ch):
    """(kept_text, replaced_entry_or_None) for one original character of the pipeline above:
    "" for a stripped format character, otherwise its NFKC form with the confusables table
    applied to each resulting character."""
    if unicodedata.category(ch) == "Cf":
        return "", f"U+{ord(ch):04X} -> (removed)"
    nfkc = unicodedata.normalize("NFKC", ch)
    final = "".join(_CONFUSABLES.get(c, c) for c in nfkc)
    if final != ch:
        return final, f"{ch} U+{ord(ch):04X} -> {final}"
    return final, None


def skeleton(text):
    """`text` with Unicode format characters removed, NFKC-normalized, and every pinned
    Cyrillic/Greek confusable replaced by the Latin letter it imitates (S6, S8); other
    characters are unchanged."""
    return "".join(_skeleton_char(ch)[0] for ch in text)


def _replaced_chars(text):
    """["<char> U+<CODEPOINT> -> <latin>", ...] for every character of `text` actually
    changed by the skeleton pipeline (stripped invisible, NFKC-normalized, or a pinned
    confusable), in reading order, for token_flagged evidence (S6, S8). A removed invisible
    reports as "U+<CODEPOINT> -> (removed)", with no original character shown."""
    return [entry for ch in text for entry in [_skeleton_char(ch)[1]] if entry is not None]


def _homoglyph_match(text, contract):
    """The official contract `text` imitates, or None: `text` must be non-ASCII and differ
    from its skeleton, and the skeleton (trimmed, case-insensitive) must equal a symbol/name
    in the pinned Uniswap list or the chain's native coin, with `contract` not being that
    token's own official address (native coins are never an ERC-20, so always flagged)."""
    if not text or text.isascii():
        return None
    normalized_skeleton = skeleton(text).strip().lower()
    if normalized_skeleton == text.strip().lower():
        return None
    native = NATIVE_COINS.get(DEFAULT_CHAIN)
    if native and (normalized_skeleton == native["symbol"] or normalized_skeleton in native["names"]):
        return "native"
    official = _token_list_by_symbol_and_name().get(normalized_skeleton)
    if official and official != (contract or "").lower():
        return official
    return None


def find_homoglyph_contracts(transfers):
    """Contract addresses whose token name/symbol imitates an official token or the native
    coin via confusable Cyrillic/Greek letters (S6)."""
    contracts = set()
    for transfer in transfers:
        contract = transfer.get("contractAddress")
        if not contract or contract in contracts:
            continue
        for text in (transfer.get("tokenSymbol"), transfer.get("tokenName")):
            if _homoglyph_match(text, contract) is not None:
                contracts.add(contract)
                break
    return contracts


def _lure_words_re():
    data = _classifier_lists() or {}
    words = data.get("lureWords", {}).get("words") or _FALLBACK_LURE_WORDS
    pattern = r"\b(?:" + "|".join(re.escape(word) for word in words) + r")\b"
    return re.compile(pattern, re.IGNORECASE)


def _candidate_domains(text):
    return {match.group(0).lower() for match in _CANDIDATE_DOMAIN_RE.finditer(text)}


def _blacklisted_domain(text, blacklist):
    """The domain embedded in `text` that is blacklisted, exact match or subdomain of a
    blacklisted domain, or None."""
    if not blacklist:
        return None
    for domain in _candidate_domains(text):
        parts = domain.split(".")
        if any(".".join(parts[i:]) in blacklist for i in range(len(parts) - 1)):
            return domain
    return None


def _known_labelled(contract):
    """True when `contract` has a non-risky entry in the local Etherscan labels. Loaded only
    on a blacklist hit; a missing file means nobody is known (blacklist keeps applying) and
    is reported by list_load_problems()."""
    if not contract:
        return False
    entry = (_load_json(KNOWN_LABELS_PATH) or {}).get(contract.lower())
    if not entry:
        return False
    labels = entry.get("labels") or []
    if _RISKY_LABELS.intersection(labels):
        return False
    texts = [entry.get("name") or ""] + list(labels)
    return not any(_RISKY_LABEL_RE.search(text) for text in texts)


def _phishing_match(text, contract=None):
    """The value that makes `text` phishing text: the full text when it carries a URL or a
    lure word (unchanged since before the blacklist existed, and never shielded by a label),
    otherwise the embedded domain that matches the public blacklist (data/phishingDomains.json)
    unless `contract` is a known labelled project token, or None."""
    if _URL_RE.search(text) or _lure_words_re().search(text):
        return text
    domain = _blacklisted_domain(text, _phishing_domains())
    if domain is not None and _known_labelled(contract):
        return None
    return domain


def is_phishing_text(token_name, token_symbol, contract=None):
    """True when a token name or symbol embeds a URL/domain, lure words, or a domain listed
    in the public blacklist (data/phishingDomains.json, R18 item 2). A blacklist-only hit
    does not count for a known labelled `contract`."""
    for text in (token_name, token_symbol):
        if not text:
            continue
        if _phishing_match(text, contract) is not None:
            return True
    return False


def find_phishing_contracts(transfers):
    """Contract addresses whose token name/symbol match the phishing criterion (A)."""
    contracts = set()
    for transfer in transfers:
        contract = transfer.get("contractAddress")
        if not contract or contract in contracts:
            continue
        if is_phishing_text(transfer.get("tokenName"), transfer.get("tokenSymbol"), contract):
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
        known_contracts = _known_token_contracts()
        forged_by_token = frm in known_contracts and frm != contract
        forged_by_wallet = frm in _known_impersonated_wallets() and contract not in known_contracts
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
    for contract in find_homoglyph_contracts(transfers):
        tags_by_contract.setdefault(contract, set()).add(TAG_HOMOGLYPH)

    # INFO: S2 - "only received" alone is not a signal; it only confirms an existing
    # strong signal for the same contract (TAG_PHISHING/TAG_SPOOFED_SENDER/TAG_NATIVE_IMPERSONATION/
    # TAG_HOMOGLYPH, all strong signals; R19's STRONG_SIGNAL_TAGS cleanup is not merged on this
    # branch yet, so TAG_HOMOGLYPH is simply added here next to the other strong signals above).
    unsolicited = find_unsolicited_contracts(transfers, trxs, central_address)
    for contract in unsolicited:
        if tags_by_contract.get(contract):
            tags_by_contract[contract].add(TAG_UNSOLICITED)

    return tags_by_contract


def find_phishing_triggers(transfers):
    """Contract -> the value that matched the phishing criterion (A): the token name/symbol
    text for a URL/lure-word match, or the matched domain for a public-blacklist match
    (R18 item 2)."""
    triggers = {}
    for transfer in transfers:
        contract = transfer.get("contractAddress")
        if not contract or contract in triggers:
            continue
        for text in (transfer.get("tokenName"), transfer.get("tokenSymbol")):
            if not text:
                continue
            match = _phishing_match(text, contract)
            if match is not None:
                triggers[contract] = match
                break
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


def _official_symbol(official_contract):
    """The symbol to report as `imitated_symbol` in homoglyph evidence (S6): the native
    coin's symbol for "native", otherwise the pinned token list's symbol for that contract."""
    if official_contract == "native":
        native = NATIVE_COINS.get(DEFAULT_CHAIN) or {}
        return (native.get("symbol") or "").upper()
    data = _load_json(TOKEN_LISTS_PATH) or {}
    for token in data.get("tokens") or ():
        if (token.get("address") or "").lower() == official_contract:
            return token.get("symbol")
    return None


def find_homoglyph_triggers(transfers):
    """Contract -> homoglyph evidence dict (S6, S8): original text, its skeleton, the symbol
    of the token it imitates, the official contract it imitates ("native" for the chain's
    native coin), the characters changed to compute the skeleton (invisible removals and NFKC
    changes included), and the normalization applied ("normalization": "NFKC", always present
    since NFKC is always part of the skeleton pipeline, whether or not it changed anything)."""
    triggers = {}
    for transfer in transfers:
        contract = transfer.get("contractAddress")
        if not contract or contract in triggers:
            continue
        for text in (transfer.get("tokenSymbol"), transfer.get("tokenName")):
            official_contract = _homoglyph_match(text, contract)
            if official_contract is not None:
                triggers[contract] = {
                    "text": text,
                    "skeleton": skeleton(text).strip(),
                    "imitated_symbol": _official_symbol(official_contract),
                    "official_contract": official_contract,
                    "replaced_chars": _replaced_chars(text),
                    "normalization": "NFKC",
                }
                break
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
        known_contracts = _known_token_contracts()
        forged_by_token = frm_lower in known_contracts and frm_lower != contract_lower
        forged_by_wallet = frm_lower in _known_impersonated_wallets() and contract_lower not in known_contracts
        if forged_by_token or forged_by_wallet:
            triggers[contract] = frm
    return triggers


# INFO: Priority order when a contract carries more than one strong signal; matches the
# order classify_tokens applies them, so the rule reported is the first one that fired.
_TRIGGER_RULES = (
    (TAG_PHISHING, find_phishing_triggers),
    (TAG_SPOOFED_SENDER, find_spoofed_sender_triggers),
    (TAG_NATIVE_IMPERSONATION, find_native_impersonation_triggers),
    (TAG_HOMOGLYPH, find_homoglyph_triggers),
)


def classify_tokens_with_evidence(transfers, trxs, central_address):
    """classify_tokens plus, per flagged contract, the rule and value that fired it
    (decision-evidence-retrofit T1). Returns (tags_by_contract, evidence): the exact
    classify_tokens result and {contract: {"rule": tag, "trigger": value}}.

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
        else:
            # INFO: a tag whose trigger could not be recovered (e.g. spam-unsolicited, which
            # only rides on a strong signal) still gets evidence: rule known, trigger None
            evidence[contract] = {"rule": sorted(tags)[0], "trigger": None}
    return tags_by_contract, evidence
