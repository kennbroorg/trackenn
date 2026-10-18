from pathlib import Path

import pytest
import yaml

import app as app_module

ROOT = Path(__file__).resolve().parent.parent
CONFIG_TEXT = "ethscan: XXX\nbscscan: XXX\nlevel: INFO\ndbname: default.db\n"


@pytest.fixture(autouse=True)
def no_db_dir(monkeypatch):
    monkeypatch.delenv("TRACKENN_DB_DIR", raising=False)


def write_config(tmp_path, text=CONFIG_TEXT):
    path = tmp_path / "config.yaml"
    path.write_text(text)
    return path


def test_config_example_is_the_documented_template():
    assert yaml.safe_load((ROOT / "config.example.yaml").read_text()) == {
        "ethscan": "XXX", "bscscan": "XXX", "level": "INFO", "dbname": "default.db"}


def test_load_config_without_db_dir_keeps_dbname(tmp_path):
    assert app_module.load_config(write_config(tmp_path))["dbname"] == "default.db"


def test_load_config_puts_relative_dbname_in_db_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("TRACKENN_DB_DIR", "/app/db")

    config = app_module.load_config(write_config(tmp_path))

    assert config == {"ethscan": "XXX", "bscscan": "XXX", "level": "INFO", "dbname": "/app/db/default.db"}


def test_load_config_keeps_absolute_dbname_with_db_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("TRACKENN_DB_DIR", "/app/db")
    path = write_config(tmp_path, CONFIG_TEXT.replace("default.db", "/data/other.db"))

    assert app_module.load_config(path)["dbname"] == "/data/other.db"


def test_load_config_ignores_empty_db_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("TRACKENN_DB_DIR", "")

    assert app_module.load_config(write_config(tmp_path))["dbname"] == "default.db"


def test_load_config_missing_file_exits_with_a_clear_message(tmp_path):
    path = tmp_path / "config.yaml"

    with pytest.raises(SystemExit) as error:
        app_module.load_config(path)

    assert str(error.value) == f"Config file {path} not found, copy config.example.yaml to config.yaml and set your API keys"
