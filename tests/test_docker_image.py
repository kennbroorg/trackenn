"""The backend image copies core/*.py one by one (install/docker/backend/Dockerfile), so a new
module imported by the app but missing from that COPY line crashes the container on startup."""
import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def imported_core_modules(path, seen=None):
    seen = set() if seen is None else seen
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.ImportFrom) and node.module == "core":
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("core."):
            names = [node.module.split(".", 1)[1]]
        for name in names:
            module = ROOT / "core" / f"{name}.py"
            if module.exists() and name not in seen:
                seen.add(name)
                imported_core_modules(module, seen)
    return seen


def test_backend_image_copies_every_core_module_the_app_imports():
    dockerfile = (ROOT / "install/docker/backend/Dockerfile").read_text()
    copied = set(re.findall(r"core/(\w+)\.py", dockerfile))

    missing = imported_core_modules(ROOT / "app.py") - copied

    assert missing == set()
