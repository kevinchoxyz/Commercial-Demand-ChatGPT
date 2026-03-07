"""CLI wrapper for the Phase 1 scaffold."""

from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from cbx250_model.demand.phase1_runner import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())

