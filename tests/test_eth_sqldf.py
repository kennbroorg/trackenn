import pandas as pd

from core import eth


def test_sqldf_runs_union_query_over_dataframes():
    df_trx = pd.DataFrame([{"blockChain": "eth", "hash": "0x1", "from": "0xa", "to": "0xb", "value": 2 * 10**18}])
    df_trf = pd.DataFrame(columns=["blockChain", "hash", "from", "to", "value", "tokenDecimal"])
    query = """
        SELECT blockChain, type, hash, `from`, `to`, valConv
        FROM (
            SELECT df_trx.blockChain, 'transaction' as 'type', df_trx.hash, df_trx.`from`, df_trx.`to`,
                df_trx.value / POWER(10, 18) as valConv
            FROM df_trx
            UNION ALL
            SELECT df_trf.blockChain, 'transfers', df_trf.hash, df_trf.`from`, df_trf.`to`,
                df_trf.value / POWER(10, df_trf.tokenDecimal)
            FROM df_trf
        ) AS combined
    """

    df_all = eth.sqldf(query, {"df_trx": df_trx, "df_trf": df_trf})

    assert df_all.to_dict(orient="records") == [
        {"blockChain": "eth", "type": "transaction", "hash": "0x1", "from": "0xa", "to": "0xb", "valConv": 2.0}
    ]


def test_sqldf_does_not_touch_the_project_db(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    eth.sqldf("SELECT COUNT(*) AS n FROM df", {"df": pd.DataFrame({"a": [1, 2]})})

    assert list(tmp_path.iterdir()) == []
