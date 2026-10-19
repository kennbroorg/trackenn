import sqlite3
import sys
from contextlib import closing
from pathlib import Path

from core import misc

ROOT = Path(__file__).resolve().parent.parent


def test_data_dir_is_the_project_data_folder():
    assert Path(misc.data_dir()) == ROOT / "data"


def test_data_dir_of_the_frozen_executable_is_next_to_it(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", str(tmp_path / "trackenn"))

    assert Path(misc.data_dir()) == tmp_path / "data"


def test_front_dir_is_the_project_front_folder():
    # INFO: Same resolution as data_dir(), the Angular build is a sibling of data/, not inside it
    assert Path(misc.front_dir()) == ROOT / "front"


def test_front_dir_of_the_frozen_executable_is_next_to_it(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", str(tmp_path / "trackenn"))

    assert Path(misc.front_dir()) == tmp_path / "front"


def test_front_dir_is_independent_of_the_working_directory(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    assert Path(misc.front_dir()) == ROOT / "front"


def test_init_loads_labels_from_any_working_directory(monkeypatch, tmp_path):
    # INFO: In Docker and from other folders cwd is not the project root
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "init.db"
    config = {"ethscan": "XXX", "bscscan": "XXX", "level": "INFO", "dbname": str(path), "action": "init"}

    events = list(misc.event_stream_checking(config))

    assert not any('"error": true' in event for event in events)
    with closing(sqlite3.connect(path)) as conn:
        chains = dict(conn.execute("SELECT blockChain, count(*) FROM t_labels GROUP BY blockChain").fetchall())
    assert chains["ethereum"] > 0
    assert chains["binance"] > 0
