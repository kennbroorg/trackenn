#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""Regenerate data/ofacSdnCryptoAddresses.json from the Hugging Face dataset
ai-decisions/ofac-sdn-crypto-addresses (https://huggingface.co/datasets/ai-decisions/ofac-sdn-crypto-addresses,
revision 72105dc742ce7891b11ee138a5defd9e498c1d99, license apache-2.0).

The dataset is a GraphSense TagPack of addresses designated on the US Treasury
OFAC SDN list, sourced primarily from https://www.treasury.gov/ofac/downloads/sdn.csv
(see documents/LABELS-HF.md for the research behind this import). Only the
Ethereum rows are kept; the output has the same shape as
data/etherscanCombinedAllLabels.json (lowercase address -> {name, labels}).

Usage: python install/labels/update_ofac_labels.py

The download is pinned to REVISION and refuses any other revision.
"""

import io
import json
import os

import pyarrow.parquet as pq
import requests

# INFO: pinned so a regeneration reproduces the reviewed data; bump it on purpose, with the
#       new revision checked, never by downloading whatever `main` holds today
REVISION = "72105dc742ce7891b11ee138a5defd9e498c1d99"
DATASET_PARQUET_URL = (
    "https://huggingface.co/datasets/ai-decisions/ofac-sdn-crypto-addresses/"
    f"resolve/{REVISION}/aidecisions_ofac.parquet"
)
LABELS = ["ofac-sanctions-lists", "ofac-sdn"]
OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "ofacSdnCryptoAddresses.json",
)


def download_parquet():
    """Bytes of the dataset parquet at REVISION. Hugging Face reports the commit it resolved
    in X-Repo-Commit on the first response (a redirect to the file storage, which drops the
    header), so the revision is checked before following it. Anything else, or nothing,
    stops the script instead of writing data from an unreviewed revision."""
    response = requests.get(DATASET_PARQUET_URL, timeout=30, allow_redirects=False)
    served = response.headers.get("X-Repo-Commit") or "none"
    if served != REVISION:
        raise SystemExit(f"Dataset revision mismatch: expected {REVISION}, got {served}")
    if response.is_redirect:
        location = response.headers.get("Location", "")
        # INFO: only follow an https redirect; the revision was already checked above
        if not location.startswith("https://"):
            raise SystemExit(f"Unexpected dataset redirect: {location or 'none'}")
        response = requests.get(location, timeout=30)
    response.raise_for_status()
    return response.content


def rows_to_labels(df):
    """Ethereum rows only, lowercase address -> {name, labels}, the shape of
    data/etherscanCombinedAllLabels.json."""
    eth = df[df["currency"] == "ETH"]
    return {
        address.lower(): {"name": label, "labels": list(LABELS)}
        for address, label in zip(eth["address"], eth["label"])
    }


def build_labels():
    table = pq.read_table(io.BytesIO(download_parquet()))
    return rows_to_labels(table.to_pandas())


def main():
    labels = build_labels()
    try:
        with open(OUTPUT_PATH, "w") as f:
            json.dump(labels, f, indent=2, sort_keys=True)
            f.write("\n")
    except OSError as e:
        raise SystemExit(f"Could not write {OUTPUT_PATH}: {e}") from e
    print(f"Wrote {len(labels)} addresses to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
