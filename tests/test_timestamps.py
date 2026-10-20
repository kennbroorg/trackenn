import pandas as pd

from core.timestamps import epoch_ms

TIMESTAMP = 1_700_000_000


def test_epoch_ms_converts_datetimes_to_epoch_milliseconds():
    series = pd.to_datetime(pd.Series([TIMESTAMP, TIMESTAMP + 1]), unit="s")

    result = epoch_ms(series)

    assert result.tolist() == [TIMESTAMP * 1000, (TIMESTAMP + 1) * 1000]
    assert str(result.dtype) == "Int64"


def test_epoch_ms_keeps_missing_timestamps_as_null():
    series = pd.Series([pd.Timestamp(TIMESTAMP, unit="s"), pd.NaT])

    result = epoch_ms(series)

    assert result.iloc[0] == TIMESTAMP * 1000
    assert result.isna().tolist() == [False, True]
    assert pd.DataFrame({"timeStamp": result}).to_json(orient="records") == f'[{{"timeStamp":{TIMESTAMP * 1000}}},{{"timeStamp":null}}]'


def test_epoch_ms_truncates_sub_millisecond_precision():
    series = pd.Series([pd.Timestamp(TIMESTAMP, unit="s") + pd.Timedelta(1999, "us")])

    assert epoch_ms(series).tolist() == [TIMESTAMP * 1000 + 1]
