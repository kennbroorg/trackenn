#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = "KennBro"
__copyright__ = "Copyright 2024, Personal Research"
__credits__ = ["KennBro"]
__license__ = "GPL"
__version__ = "0.0.1"
__maintainer__ = "KennBro"
__email__ = "kennbro <at> protonmail <dot> com"
__status__ = "Development"

import re
import logging
import requests

# INFO: (connect, read) seconds for Etherscan/BscScan calls
API_TIMEOUT = (10, 60)
SECRET_KEYS = ("ethscan", "bscscan")
APIKEY_PATTERN = re.compile(r"(apikey=)[^&\s'\"]*")

# INFO: urllib3 logs every requested url (apikey included) at DEBUG level
logging.getLogger("urllib3").setLevel(logging.WARNING)

# INFO: Reuse the TCP/TLS connection across calls instead of reconnecting on every
# requests.get (measured ~0.37s of ~0.54s per call was connect+TLS handshake)
_session = requests.Session()

# INFO: Etherscan API V1 was shut down; V2 unifies every chain behind a single host,
# selected by chainid. Module/action/param names are unchanged from V1.
API_V2_URL = "https://api.etherscan.io/v2/api"
ETH_CHAIN_ID = 1
BSC_CHAIN_ID = 56


def explorer_url(chainid, **params):
    """Build an Etherscan API V2 url for the given chainid with the given query params"""
    query = "&".join(f"{key}={value}" for key, value in params.items())
    return f"{API_V2_URL}?chainid={chainid}&{query}"


def redact(text):
    """Mask the apikey query value of any url in text"""
    return APIKEY_PATTERN.sub(r"\1***", str(text))


def public_params(params):
    """Copy of params/config safe to log, API keys masked (also inside params['config'])"""
    public = {key: ("***" if key in SECRET_KEYS else value) for key, value in params.items()}
    if isinstance(public.get("config"), dict):
        public["config"] = public_params(public["config"])
    return public


def api_get(url):
    """GET to the explorer APIs with timeout. Errors are re-raised without the apikey"""
    try:
        return _session.get(url, timeout=API_TIMEOUT)
    except requests.RequestException as error:
        # INFO: Messages include the url, and core/* streams tracebacks to the front
        raise type(error)(redact(error)) from None


def extract_result(response):
    """Return response.json()['result'], or raise with Etherscan's own error text when
    the explorer answered NOTOK (e.g. rate limit, invalid key) instead of data. 'result'
    is only a list on success; on NOTOK it's an error string, which callers must not feed
    straight into pd.DataFrame (it raises an opaque 'DataFrame constructor not properly
    called!' instead of the actual explorer error)."""
    payload = response.json()
    if payload.get("message") == "NOTOK":
        raise Exception(payload.get("result"))
    return payload["result"]
