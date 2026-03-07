"""Input loading for the Phase 2 deterministic cascade."""

from __future__ import annotations

from csv import DictReader
from dataclasses import dataclass
from pathlib import Path

from .config_schema import Phase2Config
from .schemas import Phase1MonthlyizedOutputRecord


@dataclass(frozen=True)
class Phase2InputBundle:
    phase1_monthlyized_output: tuple[Phase1MonthlyizedOutputRecord, ...]


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


def load_phase1_monthlyized_output(path: Path) -> tuple[Phase1MonthlyizedOutputRecord, ...]:
    rows = _load_csv_rows(
        path,
        (
            "scenario_name",
            "geography_code",
            "module",
            "segment_code",
            "month_index",
            "calendar_month",
            "patients_treated_monthly",
        ),
    )
    return tuple(Phase1MonthlyizedOutputRecord.from_row(row) for row in rows)


def load_phase2_inputs(config: Phase2Config) -> Phase2InputBundle:
    return Phase2InputBundle(
        phase1_monthlyized_output=load_phase1_monthlyized_output(
            config.input_paths.phase1_monthlyized_output
        )
    )
