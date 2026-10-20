import os
import shutil
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from core import misc

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def schema_db(tmp_path_factory):
    # INFO: Real schema, created by the same init flow the front triggers
    path = tmp_path_factory.mktemp("schema") / "schema.db"
    config = {"ethscan": "XXX", "bscscan": "XXX", "level": "INFO", "dbname": str(path), "action": "init"}
    cwd = os.getcwd()
    os.chdir(ROOT)  # INFO: init loads data/*.json with relative paths
    try:
        list(misc.event_stream_checking(config))
    finally:
        os.chdir(cwd)
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("DELETE FROM t_labels")
        conn.commit()
    return path


@pytest.fixture
def db(schema_db, tmp_path):
    path = tmp_path / "test.db"
    shutil.copy(schema_db, path)
    return path
