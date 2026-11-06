from pathlib import Path

import accessionflow
import accessionflow.stores


def test_namespace_packages() -> None:
    """An __init__.py in either package would hide plugins installed elsewhere"""
    for pkg in (accessionflow, accessionflow.stores):
        assert getattr(pkg, "__file__", None) is None
        for path in pkg.__path__:
            assert not (Path(path) / "__init__.py").exists()
