import multiprocessing

import app as app_module


def test_servers_start_with_fork_on_linux(monkeypatch):
    # INFO: Python 3.14 defaults to forkserver on Linux; children of the frozen executable cannot import __main__ targets
    monkeypatch.setattr(app_module.sys, "platform", "linux")

    assert app_module.process_context().get_start_method() == "fork"


def test_other_platforms_keep_the_default_start_method(monkeypatch):
    # INFO: Windows uses spawn + freeze_support (fork does not exist there)
    monkeypatch.setattr(app_module.sys, "platform", "win32")

    assert app_module.process_context().get_start_method() == multiprocessing.get_context().get_start_method()
