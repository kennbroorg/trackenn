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
"""

import io
import json
import os

import pyarrow.parquet as pq
import requests

DATASET_PARQUET_URL = (
    "https://huggingface.co/datasets/ai-decisions/ofac-sdn-crypto-addresses/"
    "resolve/main/aidecisions_ofac.parquet"
)
OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "ofacSdnCryptoAddresses.json",
)


def build_labels():
    response = requests.get(DATASET_PARQUET_URL, timeout=30)
    response.raise_for_status()
    table = pq.read_table(io.BytesIO(response.content))
    df = table.to_pandas()

    eth = df[df["currency"] == "ETH"]

    labels = {}
    for _, row in eth.iterrows():
        address = row["address"].lower()
        labels[address] = {
            "name": row["label"],
            "labels": ["ofac-sanctions-lists", "ofac-sdn"],
        }
    return labels


def main():
    labels = build_labels()
    try:
        with open(OUTPUT_PATH, "w") as f:
            json.dump(labels, f, indent=2, sort_keys=True)
            f.write("\n")
    except OSError as e:
        raise SystemExit(f"Could not write {OUTPUT_PATH}: {e}")
    print(f"Wrote {len(labels)} addresses to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
