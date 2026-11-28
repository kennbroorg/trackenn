"""R12: decode the `input` of stored transactions into named calls (odd/tasks/backend-backlog-c.md,
T13, S12). Read-only over already-collected data (t_transactions); the only writes this module makes
are to its own ABI cache table and to decisions (K8), never to any investigation table.

Name resolution order per call (S12): the stored `functionName` (source "etherscan") -> a fetched
contract ABI, cached per contract (source "abi") -> the pinned data/functionSignatures.json table,
filtered by calldata shape when several signatures share a selector (source "signatures", refined K7,
see odd/tasks/backend-backlog-c.md L43-L44) -> unknown. Static ABI types (address, uintN, intN, bool,
bytesN) are decoded in-repo; dynamic ones (string, bytes, arrays, tuples) are shown as their raw head
word with value_undecoded (S12).

Matching a fetched ABI's functions to a selector, and the common-signature shortlist in
data/functionSignatures.json, both need keccak256, which stdlib hashlib does not provide
(hashlib has sha3_256, not the pre-NIST Keccak Ethereum uses) and no keccak/eth-hash package is in
requirements.txt. Adding one dependency for a single hash used nowhere else in the codebase is not
worth it, so a small pure-Python Keccak-f[1600] is vendored below, checked against known test vectors
(keccak256("") and the transfer(address,uint256) -> 0xa9059cbb selector, tests/test_calls.py).
"""

import json
import logging
import os
import re
import sqlite3
import time
from contextlib import closing

from core import api_client, decisions
from core.onchain_messages import decode_message

logger = logging.getLogger(__name__)

DEFAULT_MAX_CALLS = 20
MAX_CALLS_CEILING = 200
EXAMPLE_HASHES = 5
UNVERIFIED_ABI_TTL_SECONDS = 7 * 24 * 3600  # S13(a): an unverified contract may be verified later
FAILED_ABI_TTL_SECONDS = 10 * 60  # S13(b): do not spend budget retrying a rate limit immediately

_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
SIGNATURES_PATH = os.path.join(_DATA_DIR, "functionSignatures.json")

_signatures_cache = None


def _load_signatures():
    global _signatures_cache
    if _signatures_cache is None:
        try:
            with open(SIGNATURES_PATH) as file:
                _signatures_cache = json.load(file).get("signatures", {})
        except (OSError, ValueError):
            _signatures_cache = {}
    return _signatures_cache


# ---------------------------------------------------------------------------
# Keccak-256 (Ethereum variant: Keccak-f[1600], rate 136 bytes, 0x01 domain byte,
# pad10*1), vendored. Verified against keccak256("") and keccak256("abc") test
# vectors, and the transfer(address,uint256) -> 0xa9059cbb selector (tests/test_calls.py).
# ---------------------------------------------------------------------------

_MASK64 = (1 << 64) - 1

_ROUND_CONSTANTS = [
    0x0000000000000001, 0x0000000000008082, 0x800000000000808A, 0x8000000080008000,
    0x000000000000808B, 0x0000000080000001, 0x8000000080008081, 0x8000000000008009,
    0x000000000000008A, 0x0000000000000088, 0x0000000080008009, 0x000000008000000A,
    0x000000008000808B, 0x800000000000008B, 0x8000000000008089, 0x8000000000008003,
    0x8000000000008002, 0x8000000000000080, 0x000000000000800A, 0x800000008000000A,
    0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008,
]

# Rotation offsets r[x][y] for state lane (x + 5*y)
_ROTATIONS = [
    [0, 36, 3, 41, 18],
    [1, 44, 10, 45, 2],
    [62, 6, 43, 15, 61],
    [28, 55, 25, 21, 56],
    [27, 20, 39, 8, 14],
]


def _rol(value, amount):
    amount %= 64
    if amount == 0:
        return value
    return ((value << amount) | (value >> (64 - amount))) & _MASK64


def _keccak_f(state):
    for round_index in range(24):
        column_parity = [state[x] ^ state[x + 5] ^ state[x + 10] ^ state[x + 15] ^ state[x + 20] for x in range(5)]
        theta = [column_parity[(x - 1) % 5] ^ _rol(column_parity[(x + 1) % 5], 1) for x in range(5)]
        for x in range(5):
            for y in range(5):
                state[x + 5 * y] ^= theta[x]
        rotated = [0] * 25
        for x in range(5):
            for y in range(5):
                rotated[y + 5 * ((2 * x + 3 * y) % 5)] = _rol(state[x + 5 * y], _ROTATIONS[x][y])
        new_state = [0] * 25
        for x in range(5):
            for y in range(5):
                new_state[x + 5 * y] = rotated[x + 5 * y] ^ (
                    (~rotated[(x + 1) % 5 + 5 * y]) & rotated[(x + 2) % 5 + 5 * y] & _MASK64)
        state = new_state
        state[0] ^= _ROUND_CONSTANTS[round_index]
    return state


def keccak256(message):
    """Ethereum's keccak256(message) -> 32 bytes."""
    rate = 136
    state = [0] * 25
    padded = bytearray(message)
    padded.append(0x01)
    while len(padded) % rate != 0:
        padded.append(0x00)
    padded[-1] |= 0x80
    for offset in range(0, len(padded), rate):
        block = padded[offset:offset + rate]
        for word_offset in range(0, rate, 8):
            state[word_offset // 8] ^= int.from_bytes(block[word_offset:word_offset + 8], "little")
        state = _keccak_f(state)
    digest = bytearray()
    for i in range(4):
        digest += state[i].to_bytes(8, "little")
    return bytes(digest[:32])


# ---------------------------------------------------------------------------
# Signature text parsing and the in-repo static-type decoder
# ---------------------------------------------------------------------------

def _split_top_level(text):
    parts, current, depth = [], "", 0
    for char in text:
        if char == "," and depth == 0:
            parts.append(current)
            current = ""
            continue
        if char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
        current += char
    if current or parts:
        parts.append(current)
    return parts


def _type_only(param_text):
    """Strip a parameter's name (and any leading qualifier such as `indexed`/`memory`), keeping
    only its type: Etherscan's stored functionName carries parameter names, e.g.
    "transfer(address _to, uint256 _value)" (R3-001). The type is always the first whitespace-
    separated token at depth 0 (a tuple or array type never contains a top-level space)."""
    depth = 0
    for index, char in enumerate(param_text):
        if char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
        elif char.isspace() and depth == 0:
            return param_text[:index]
    return param_text


def parse_signature(signature_text):
    """"name(type1,type2)" -> (name, [type1, type2]), nested parens/brackets kept intact, parameter
    names stripped (R3-001). Raises ValueError on text with no "(" or no closing ")", so callers can
    fall back to the next source instead of crashing on a malformed stored functionName."""
    if "(" not in signature_text or not signature_text.endswith(")"):
        raise ValueError(f"not a function signature: {signature_text!r}")
    name, rest = signature_text.split("(", 1)
    inner = rest[:-1]
    types = [_type_only(t.strip()) for t in _split_top_level(inner)] if inner else []
    return name, types


_DYNAMIC_TOP_LEVEL = re.compile(r"^.+\[\d*\]$")


def is_dynamic_type(param_type):
    """Dynamic per S12's own classification: string, bytes, every array (fixed or not, since a
    fixed array's element count still needs decoding this module does not attempt) and tuples."""
    if param_type in ("string", "bytes"):
        return True
    if param_type.startswith("("):
        return True
    if _DYNAMIC_TOP_LEVEL.match(param_type):
        return True
    return False


def decode_static(param_type, word):
    if param_type == "address":
        return "0x" + word[-20:].hex()
    if param_type == "bool":
        return int.from_bytes(word, "big") != 0
    if param_type.startswith("uint"):
        return int.from_bytes(word, "big")
    if param_type.startswith("int"):
        value = int.from_bytes(word, "big")
        return value - (1 << 256) if value >= (1 << 255) else value
    if param_type.startswith("bytes"):
        length = int(param_type[len("bytes"):]) if len(param_type) > len("bytes") else 32
        return "0x" + word[:length].hex()
    return "0x" + word.hex()


def raw_words(data):
    """data as a list of {type: "raw", value} 32-byte words (R2-raw-params): the fallback shape
    used whenever no signature resolves a selector's params (unknown or ambiguous)."""
    return [{"type": "raw", "value": "0x" + data[i:i + 32].hex()} for i in range(0, len(data), 32)]


def decode_params(data, types):
    """One entry per top-level type. Static types (address, uintN, intN, bool, bytes1-32) decode
    their head word; dynamic ones (string, bytes, arrays, tuples) keep it raw with value_undecoded,
    same policy as "without a signature, params are the raw 32-byte words" (S12)."""
    params = []
    offset = 0
    for param_type in types:
        word = data[offset:offset + 32]
        offset += 32
        if len(word) < 32:
            params.append({"type": param_type, "value": "0x" + word.hex(), "value_undecoded": True})
            continue
        if is_dynamic_type(param_type):
            params.append({"type": param_type, "value": "0x" + word.hex(), "value_undecoded": True})
        else:
            params.append({"type": param_type, "value": decode_static(param_type, word)})
    return params


# ---------------------------------------------------------------------------
# K7 (refined): filter candidate signatures of a selector by whether the calldata could actually
# be an encoding of them, before declaring ambiguity. A function is "fully static" when every
# parameter is non-dynamic (expand_static_leaves) and must then match the calldata length exactly;
# a function with dynamic parameters needs at least one head word per parameter and each dynamic
# head word must be a plausible offset (word-aligned, past the header, inside the data). `address`
# and `bool` head words are also checked for their required shape (upper 12 bytes zero / 0 or 1):
# this is what tells a real transfer(address,uint256) apart from the same-selector, same-length
#4byte.directory noise (odd/tasks/backend-backlog-c.md L43-L44).
# ---------------------------------------------------------------------------

def _head_slots(types):
    """One slot per top-level parameter: ("static", type) or ("dynamic", None)."""
    return [("dynamic", None) if is_dynamic_type(t) else ("static", t) for t in types]


def shape_fits(data, types):
    slots = _head_slots(types)
    if not slots:
        return len(data) == 0
    head_words = len(slots)
    if len(data) < head_words * 32:
        return False
    has_dynamic = any(kind == "dynamic" for kind, _ in slots)
    if not has_dynamic and len(data) != head_words * 32:
        return False
    for index, (kind, param_type) in enumerate(slots):
        word = data[index * 32:(index + 1) * 32]
        if kind == "static":
            if param_type == "address" and word[:12] != b"\x00" * 12:
                return False
            if param_type == "bool" and int.from_bytes(word, "big") not in (0, 1):
                return False
        else:
            offset = int.from_bytes(word, "big")
            if offset % 32 != 0 or offset < head_words * 32 or offset > len(data):
                return False
    return True


def resolve_signature_candidates(candidates, data):
    """candidates: text signatures sharing one selector. Returns (chosen_or_None, fitting,
    discarded_count): `fitting` is the shortlist actually used for a decision (either the single
    chosen one wrapped in a list, or every candidate tied by shape). Per K7, no preference is
    applied among several fitting candidates (R2-specificity): if more than one fits, none is
    chosen and every fitting one is reported (odd/tasks/backend-backlog-c.md T14)."""
    fitting = [sig for sig in candidates if shape_fits(data, parse_signature(sig)[1])]
    discarded = len(candidates) - len(fitting)
    if len(fitting) == 1:
        return fitting[0], fitting, discarded
    return None, fitting, discarded


# ---------------------------------------------------------------------------
# ABI fetch (Etherscan getabi) and its cache
# ---------------------------------------------------------------------------

class UpstreamError(Exception):
    """getabi failed for a reason other than "not verified" (rate limit, bad key, network)."""


class RecentFailure(UpstreamError):
    """S13(b): a getabi failure for this contract is still cached as failed (within
    FAILED_ABI_TTL_SECONDS); no call is made and no budget is spent retrying it."""


class BudgetExhausted(Exception):
    """The per-query getabi call budget ran out."""


class CallBudget:
    def __init__(self, max_calls):
        self.max_calls = max(1, min(MAX_CALLS_CEILING, max_calls))
        self.calls = 0

    def available(self):
        return self.calls < self.max_calls

    def spend(self):
        self.calls += 1


def _canonical_type(input_def):
    param_type = input_def["type"]
    if param_type.startswith("tuple"):
        components = input_def.get("components", [])
        inner = ",".join(_canonical_type(c) for c in components)
        suffix = param_type[len("tuple"):]
        return f"({inner}){suffix}"
    return param_type


def _abi_function_signature(entry):
    types = [_canonical_type(i) for i in entry.get("inputs", [])]
    return f"{entry['name']}({','.join(types)})", types


def match_abi_function(abi, method_id):
    """The function whose keccak256 selector matches method_id, or None."""
    for entry in abi:
        if entry.get("type") not in (None, "function") or "name" not in entry:
            continue
        try:
            signature_text, types = _abi_function_signature(entry)
        except (KeyError, TypeError):
            continue
        selector = "0x" + keccak256(signature_text.encode())[:4].hex()
        if selector == method_id:
            return signature_text, types
    return None


def fetch_abi(chainid, key, contract):
    """(verified, abi_or_None). Raises UpstreamError on anything other than "not verified" (R1-002/K6:
    network/transport exceptions from requests and a non-JSON body are caught here too, redacted so the
    API key never reaches a decision reason, same pattern as core/balances.py's on-chain comparison)."""
    url = api_client.explorer_url(chainid, module="contract", action="getabi", address=contract, apikey=key)
    try:
        response = api_client.api_get(url)
        payload = response.json()
    except Exception as error:
        raise UpstreamError(api_client.redact(error)) from None
    if payload.get("status") == "1":
        try:
            return True, json.loads(payload["result"])
        except (ValueError, TypeError):
            return True, []
    result_text = str(payload.get("result") or payload.get("message") or "")
    if "not verified" in result_text.lower():
        return False, None
    raise UpstreamError(result_text or "getabi failed")


class AbiCache:
    """Per blockchain+contract ABI cache (t_abi_cache), created idempotently like the other
    caches. A contract is fetched through Etherscan's getabi at most once per run and at most
    once ever when the DB cache already has it (K4).

    S13(a)/(b): a `status` column distinguishes a resolved entry ("verified"/"unverified") from
    a cached getabi failure ("failed", `abi` holds the redacted error text). "unverified" expires
    after UNVERIFIED_ABI_TTL_SECONDS (a contract may be verified later); "verified" never expires
    (immutable once verified); "failed" expires after FAILED_ABI_TTL_SECONDS (S13(b): long enough
    to absorb a rate limit without burning the call budget on every query, short enough that a
    fixed key or restored service is retried soon). An existing t_abi_cache from before this
    column existed is migrated idempotently: the column is added once, and its NULL rows (always
    "verified"/"unverified", since "failed" entries did not exist yet) are backfilled from
    `verified` so old never-expiring verified rows and old unverified rows (treated as past their
    TTL, since their real fetch time predates this cache design) behave correctly."""

    def __init__(self, dbname, now=None):
        self.dbname = dbname
        self.now = now or time.time
        self.cache_error = None
        self.served = []
        self.skipped_recent_failures = []

    def _connect(self):
        connection = sqlite3.connect(f"file:{self.dbname}?mode=rw", uri=True)
        connection.execute(
            "CREATE TABLE IF NOT EXISTS t_abi_cache (blockChain text, address text, verified integer, "
            "abi text, fetched_at integer, UNIQUE(blockChain, address))")
        try:
            connection.execute("ALTER TABLE t_abi_cache ADD COLUMN status text")
            connection.execute(
                "UPDATE t_abi_cache SET status = CASE WHEN verified THEN 'verified' ELSE 'unverified' END "
                "WHERE status IS NULL")
        except sqlite3.OperationalError as error:
            if "duplicate column name" not in str(error):
                raise
        return connection

    def _expired(self, status, fetched_at):
        if status == "verified":
            return False
        ttl = FAILED_ABI_TTL_SECONDS if status == "failed" else UNVERIFIED_ABI_TTL_SECONDS
        return fetched_at is None or self.now() - fetched_at >= ttl

    def read(self, blockchain, contract):
        if self.dbname is None or self.cache_error:
            return None
        try:
            with closing(self._connect()) as connection:
                return connection.execute(
                    "SELECT verified, abi, fetched_at, status FROM t_abi_cache "
                    "WHERE blockChain = ? AND address = ?", (blockchain, contract)).fetchone()
        except sqlite3.Error as error:
            self.cache_error = str(error)
            return None

    def write(self, blockchain, contract, status, verified, abi):
        if self.dbname is None or self.cache_error:
            return
        try:
            with closing(self._connect()) as connection:
                connection.execute(
                    "INSERT OR REPLACE INTO t_abi_cache (blockChain, address, verified, abi, fetched_at, status) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (blockchain, contract, int(verified), json.dumps(abi) if abi is not None else None,
                     int(self.now()), status))
                connection.commit()
        except sqlite3.Error as error:
            self.cache_error = str(error)

    def resolve(self, blockchain, contract, chainid, key, budget, fetcher=None):
        """(verified, abi) for contract, from cache or a fresh getabi call. Raises BudgetExhausted
        or UpstreamError (both leave the contract unresolved; callers fall back to signatures).
        A cached "failed" entry still within its TTL raises RecentFailure without spending budget
        or calling the fetcher (S13(b))."""
        fetcher = fetcher or fetch_abi
        cached = self.read(blockchain, contract)
        if cached:
            verified, abi_json, fetched_at, status = cached
            status = status or ("verified" if verified else "unverified")
            if not self._expired(status, fetched_at):
                if status == "failed":
                    self.skipped_recent_failures.append(contract)
                    raise RecentFailure(abi_json or "getabi failed")
                self.served.append({"address": contract, "fetched_at": fetched_at})
                try:
                    return bool(verified), json.loads(abi_json) if abi_json else None
                except ValueError:
                    self.cache_error = f"corrupt ABI cache entry for {contract}"
                    return bool(verified), None
        if not budget.available():
            raise BudgetExhausted(contract)
        try:
            verified, abi = fetcher(chainid, key, contract)
        except UpstreamError as error:
            budget.spend()
            self.write(blockchain, contract, "failed", False, str(error))
            raise
        budget.spend()
        self.write(blockchain, contract, "verified" if verified else "unverified", verified, abi)
        return verified, abi

    def decisions(self):
        found = []
        if self.cache_error:
            found.append(decisions.make("cache_unavailable", None, "t_abi_cache", self.cache_error))
        if self.served:
            found.append(decisions.make(
                "served_from_cache", None, "ABI already cached",
                f"{len(self.served)} contracts answered from the ABI cache; no getabi call made",
                {"addresses": self.served}))
        if self.skipped_recent_failures:
            # S13(b): a previous getabi failure is still cached for these contracts; no call spent
            # retrying them within FAILED_ABI_TTL_SECONDS of the failure.
            found.append(decisions.make(
                "upstream_error", None, "getabi",
                "A recent getabi failure is still cached for some contracts; not retried yet",
                {"skipped_recent_failure": sorted(set(self.skipped_recent_failures))}))
        return found


# ---------------------------------------------------------------------------
# Reading stored transactions and classifying their kind
# ---------------------------------------------------------------------------

def _read_transactions(dbname, blockchain, address=None, tx_hash=None):
    try:
        with closing(sqlite3.connect(f"file:{dbname}?mode=ro", uri=True)) as connection:
            cursor = connection.cursor()
            if tx_hash:
                cursor.execute(
                    "SELECT hash, `to`, input, methodId, functionName FROM t_transactions "
                    "WHERE blockChain = ? AND lower(hash) = ?", (blockchain, tx_hash.lower()))
            else:
                addr = str(address).lower()
                cursor.execute(
                    "SELECT hash, `to`, input, methodId, functionName FROM t_transactions "
                    "WHERE blockChain = ? AND (lower(`from`) = ? OR lower(`to`) = ?) ORDER BY timeStamp ASC",
                    (blockchain, addr, addr))
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except sqlite3.Error as error:
        if decisions.not_created_yet(error, dbname):
            return []
        logger.error("Calls: investigation database unreadable: %s", error)
        raise decisions.DatabaseUnreadable(str(error)) from None


def _calldata(input_hex):
    body = input_hex[2:] if input_hex.startswith("0x") else input_hex
    try:
        return bytes.fromhex(body)
    except ValueError:
        return b""


def _classify(row):
    if not row["to"]:
        return "creation"
    if decode_message(row["input"]) is not None:
        return "message"
    return "call"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def get_calls(dbname, blockchain="eth", chainid=None, key=None, address=None, tx_hash=None,
              max_calls=DEFAULT_MAX_CALLS, fetch_abi_fn=None, signatures=None, now=None):
    applied_max_calls = max(1, min(max_calls, MAX_CALLS_CEILING))
    signatures = _load_signatures() if signatures is None else signatures
    rows = _read_transactions(dbname, blockchain, address=address, tx_hash=tx_hash)

    abi_cache = AbiCache(dbname, now=now)
    budget = CallBudget(applied_max_calls)
    contract_abi = {}  # contract -> (verified, abi) once resolved this run
    contract_unresolved = {}  # contract -> "budget" | "upstream"
    pending_contracts = []
    unknown_tracker = {}
    ambiguous_tracker = {}
    undecoded_tracker = {}
    found_decisions = []

    def track(tracker, method_id, tx_hash_value, extra=None, discarded=0):
        entry = tracker.setdefault(method_id, {"count": 0, "hashes": [], "extra": extra, "discarded": discarded})
        entry["count"] += 1
        if len(entry["hashes"]) < EXAMPLE_HASHES:
            entry["hashes"].append(tx_hash_value)
        if extra is not None:
            entry["extra"] = extra
            entry["discarded"] = discarded

    results = []
    for row in rows:
        if not row["input"]:  # R3-004: a NULL/empty stored input is not a call, skip it
            continue

        kind = _classify(row)
        entry = {"hash": row["hash"], "to": row["to"], "kind": kind, "methodId": None,
                 "function": None, "candidates": None, "source": None, "params": None}

        if kind in ("creation", "message"):
            results.append(entry)
            continue

        raw = _calldata(row["input"])
        if len(raw) < 4:
            entry["params"] = []
            results.append(entry)
            continue

        method_id = "0x" + raw[:4].hex()
        data = raw[4:]
        entry["methodId"] = method_id

        if row["functionName"]:
            # R3-001: a malformed stored functionName (no "(", no closing ")") falls back to the
            # next source instead of crashing; parse_signature raises ValueError for it.
            try:
                signature_text = row["functionName"]
                _, types = parse_signature(signature_text)
            except ValueError:
                pass
            else:
                entry["function"], entry["source"] = signature_text, "etherscan"
                entry["params"] = decode_params(data, types)
                results.append(entry)
                continue

        contract = str(row["to"]).lower()
        if contract not in contract_abi and contract not in contract_unresolved:
            try:
                verified, abi = abi_cache.resolve(blockchain, contract, chainid, key, budget, fetch_abi_fn)
                contract_abi[contract] = (verified, abi)
            except BudgetExhausted:
                contract_unresolved[contract] = "budget"
                pending_contracts.append(contract)
            except RecentFailure:
                # S13(b): still within the failed-getabi TTL; no call spent, reported once below.
                contract_unresolved[contract] = "upstream"
            except UpstreamError as error:
                contract_unresolved[contract] = "upstream"
                found_decisions.append(decisions.make(
                    "upstream_error", contract, "getabi", str(error),
                    {"contract": contract}))

        resolved_from_abi = False
        if contract in contract_abi:
            verified, abi = contract_abi[contract]
            if verified and abi:
                match = match_abi_function(abi, method_id)
                if match:
                    signature_text, types = match
                    entry["function"], entry["source"] = signature_text, "abi"
                    entry["params"] = decode_params(data, types)
                    resolved_from_abi = True

        if not resolved_from_abi:
            candidates = signatures.get(method_id, [])
            if not candidates:
                track(unknown_tracker, method_id, row["hash"])
                entry["params"] = raw_words(data)
            else:
                chosen, fitting, discarded = resolve_signature_candidates(candidates, data)
                if chosen:
                    _, types = parse_signature(chosen)
                    entry["function"], entry["source"] = chosen, "signatures"
                    entry["params"] = decode_params(data, types)
                elif fitting:
                    track(ambiguous_tracker, method_id, row["hash"], extra=sorted(fitting), discarded=discarded)
                    entry["candidates"] = sorted(fitting)
                    entry["params"] = raw_words(data)
                else:
                    track(unknown_tracker, method_id, row["hash"])
                    entry["params"] = raw_words(data)

        if entry["params"] and any(param.get("value_undecoded") for param in entry["params"]):
            track(undecoded_tracker, method_id, row["hash"],
                  extra=sorted({param["type"] for param in entry["params"] if param.get("value_undecoded")}))

        results.append(entry)

    if pending_contracts:
        found_decisions.append(decisions.make(
            "call_cap_reached", address or tx_hash, f"max_calls={applied_max_calls}",
            "The getabi call budget ran out; some contracts were not queried",
            {"pending": sorted(set(pending_contracts)), "calls": budget.calls}))

    found_decisions.extend(abi_cache.decisions())

    for method_id, info in unknown_tracker.items():
        found_decisions.append(decisions.make(
            "selector_unknown", method_id, "no stored functionName, no verified ABI match, no pinned signature fits",
            "This selector could not be named; params are shown as raw 32-byte words",
            {"count": info["count"], "hashes": info["hashes"]}))
    for method_id, info in ambiguous_tracker.items():
        found_decisions.append(decisions.make(
            "selector_ambiguous", method_id, "several pinned signatures fit the calldata shape",
            "More than one signature could be the real function; none was chosen",
            {"candidates": info["extra"], "discarded": info["discarded"],
             "count": info["count"], "hashes": info["hashes"]}))
    for method_id, info in undecoded_tracker.items():
        found_decisions.append(decisions.make(
            "value_undecoded", method_id, "dynamic parameter type (string, bytes, array or tuple)",
            "Some parameters are dynamic types this decoder does not resolve; shown as a raw head word",
            {"types": info["extra"], "count": info["count"], "hashes": info["hashes"]}))

    return {"max_calls": applied_max_calls, "calls": results, "decisions": found_decisions}
