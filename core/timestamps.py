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


import pandas as pd


def epoch_ms(series):
    """Datetime series to epoch milliseconds (Int64, NaT -> null in to_json).

    INFO: The front reads timeStamp as epoch milliseconds; pandas 3 deprecates the implicit epoch to_json format
    """
    return ((series - pd.Timestamp(0)) // pd.Timedelta(1, "ms")).astype("Int64")
