"""CLI wrapper for the Phase 2 deterministic cascade."""

from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from cbx250_model.phase2.runner import main


if __name__ == "__main__":
    raise SystemExit(main())
