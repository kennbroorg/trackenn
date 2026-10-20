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
        return requests.get(url, timeout=API_TIMEOUT)
    except requests.RequestException as error:
        # INFO: Messages include the url, and core/* streams tracebacks to the front
        raise type(error)(redact(error)) from None
