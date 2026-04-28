from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def sample_tree(tmp_path: Path) -> Path:
    root = tmp_path / "source"
    (root / "nested").mkdir(parents=True)
    (root / "a.txt").write_text("alpha\nbeta\ngamma\n", encoding="utf-8")
    (root / "nested" / "b.bin").write_bytes(bytes(range(64)))
    return root

