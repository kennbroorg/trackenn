#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""Decode the `input` hex of a transaction into on-chain message text (R2, S1).

A message is readable UTF-8 text, not an ABI-encoded contract call. Pure decoding,
no I/O: the DB query lives in get_messages below."""

import sqlite3
from contextlib import closing

MIN_MESSAGE_LENGTH = 4
MIN_PRINTABLE_RATIO = 0.9


def _is_message_char(char):
    return char.isprintable() or char.isspace()


def decode_message(input_hex):
    """Return the decoded message text, or None if `input_hex` is not a message."""
    if not input_hex or not input_hex.startswith("0x"):
        return None

    hex_body = input_hex[2:]
    try:
        raw = bytes.fromhex(hex_body)
    except ValueError:
        return None

    raw = raw.rstrip(b"\x00")

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None

    if len(text) < MIN_MESSAGE_LENGTH:
        return None

    printable = sum(1 for char in text if _is_message_char(char))
    if printable / len(text) < MIN_PRINTABLE_RATIO:
        return None

    return text


def get_messages(dbname, address, blockchain="eth"):
    """Read-only: messages of stored normal transactions where `address` is `from`
    or `to` (case-insensitive), ordered by timeStamp. Returns a list of dicts
    (hash, blockNumber, timeStamp, from, to, text)."""
    address = address.lower()
    # INFO: read-only URI never creates the file; a DB not initialised yet has no messages
    try:
        with closing(sqlite3.connect(f"file:{dbname}?mode=ro", uri=True)) as connection:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT hash, blockNumber, timeStamp, `from`, `to`, input "
                "FROM t_transactions "
                "WHERE blockChain = ? AND (lower(`from`) = ? OR lower(`to`) = ?) "
                "ORDER BY timeStamp ASC",
                (blockchain, address, address),
            )
            rows = cursor.fetchall()
    except sqlite3.OperationalError:
        return []

    messages = []
    for trx_hash, block_number, timestamp, from_addr, to_addr, input_hex in rows:
        text = decode_message(input_hex)
        if text is None:
            continue
        messages.append({
            "hash": trx_hash,
            "blockNumber": block_number,
            "timeStamp": timestamp,
            "from": from_addr,
            "to": to_addr,
            "text": text,
        })
    return messages
