import pytest

import app as app_module

CONFIG = {"ethscan": "K1", "bscscan": "K2", "level": "INFO", "dbname": "default.db"}


class FakeProcess:
    started = []

    def __init__(self, name, target, kwargs):
        self.name, self.target, self.kwargs = name, target, kwargs
        self.daemon = False

    def start(self):
        FakeProcess.started.append((self.name, self.target, self.kwargs, self.daemon))

    def join(self):
        pass


class FakeContext:
    Process = FakeProcess


@pytest.fixture
def calls(monkeypatch):
    FakeProcess.started = []
    recorded = []
    monkeypatch.setattr(app_module, "load_config", lambda: dict(CONFIG))
    monkeypatch.setattr(app_module, "process_context", lambda: FakeContext)
    monkeypatch.setattr(app_module, "API_Server", lambda *args, **kwargs: recorded.append(("API_Server", args, kwargs)))
    monkeypatch.setattr(app_module, "httpServer", lambda *args, **kwargs: recorded.append(("httpServer", args, kwargs)))
    return recorded


def test_prod_starts_api_and_front_processes(calls):
    app_module.main([])

    assert [(name, kwargs, daemon) for name, _, kwargs, daemon in FakeProcess.started] == [
        ("API", {"ip": "127.0.0.1", "port": 5000, "config": CONFIG}, True),
        ("httpd", {"ip": "127.0.0.1"}, True),
    ]
    assert [target for _, target, _, _ in FakeProcess.started] == [app_module.API_Server, app_module.httpServer]
    assert calls == []


def test_prod_passes_the_given_ip_to_both_servers(calls):
    app_module.main(["-i", "10.0.0.5"])

    assert [kwargs["ip"] for _, _, kwargs, _ in FakeProcess.started] == ["10.0.0.5", "10.0.0.5"]


def test_dev_runs_only_the_api_in_debug(calls):
    app_module.main(["-e", "dev"])

    assert FakeProcess.started == []
    assert calls == [("API_Server", ("127.0.0.1",), {"port": 5000, "env": "desa", "config": CONFIG})]


def test_api_only_runs_the_api_without_the_front_server(calls):
    # INFO: Docker serves the front with nginx, the container runs a single uvicorn process
    app_module.main(["--api-only", "-i", "0.0.0.0"])

    assert FakeProcess.started == []
    assert calls == [("API_Server", (), {"ip": "0.0.0.0", "port": 5000, "config": CONFIG})]


def test_help_lists_api_only_without_config(capsys, monkeypatch, tmp_path):
    # INFO: --help must work before config.yaml exists (fresh clone, docker image)
    monkeypatch.undo()
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as exit_info:
        app_module.main(["--help"])

    assert exit_info.value.code == 0
    assert "--api-only" in capsys.readouterr().out


def test_bad_option_exits_2_without_reading_config(capsys, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as exit_info:
        app_module.main(["-e", "bad"])

    assert exit_info.value.code == 2
    assert "invalid choice: 'bad'" in capsys.readouterr().err
