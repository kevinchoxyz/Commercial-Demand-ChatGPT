"""Input loading for the deterministic Phase 3 trade layer.

Phase 3 reads the accepted upstream Phase 2 contract only. It does not assume
anything about whether treated demand originated from direct census input or a
later starts / duration cohort derivation upstream.
"""

from __future__ import annotations

from csv import DictReader
from dataclasses import dataclass
from pathlib import Path

from .config_schema import Phase3Config
from .schemas import Phase2TradeInputRecord


@dataclass(frozen=True)
class Phase3InputBundle:
    phase2_deterministic_cascade: tuple[Phase2TradeInputRecord, ...]


def _load_csv_rows(path: Path, required_columns: tuple[str, ...]) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Required input file not found: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"Input file has no header row: {path}")
        missing_columns = [column for column in required_columns if column not in reader.fieldnames]
        if missing_columns:
            raise ValueError(f"Input file {path} is missing columns: {missing_columns}")
        return list(reader)


def load_phase2_deterministic_cascade(path: Path) -> tuple[Phase2TradeInputRecord, ...]:
    rows = _load_csv_rows(
        path,
        (
            "scenario_name",
            "geography_code",
            "module",
            "segment_code",
            "month_index",
            "calendar_month",
            "fg_units_required",
        ),
    )
    return tuple(Phase2TradeInputRecord.from_row(row) for row in rows)


def load_phase3_inputs(config: Phase3Config) -> Phase3InputBundle:
    return Phase3InputBundle(
        phase2_deterministic_cascade=load_phase2_deterministic_cascade(
            config.input_paths.phase2_deterministic_cascade
        )
    )
