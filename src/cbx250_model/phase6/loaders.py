"""Input loading for deterministic Phase 6 financial analytics."""

from __future__ import annotations

from csv import DictReader
from dataclasses import dataclass
from pathlib import Path

from .config_schema import Phase6Config
from .schemas import (
    Phase4FinancialInputRecord,
    Phase5FinancialDetailInputRecord,
    Phase5FinancialSummaryInputRecord,
)


@dataclass(frozen=True)
class Phase6InputBundle:
    phase4_monthly_summary: tuple[Phase4FinancialInputRecord, ...]
    phase5_inventory_detail: tuple[Phase5FinancialDetailInputRecord, ...]
    phase5_monthly_inventory_summary: tuple[Phase5FinancialSummaryInputRecord, ...]


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


def load_phase4_monthly_summary(path: Path) -> tuple[Phase4FinancialInputRecord, ...]:
    rows = _load_csv_rows(
        path,
        (
            "scenario_name",
            "geography_code",
            "module",
            "month_index",
            "calendar_month",
            "fg_release_units",
            "dp_release_units",
            "ds_release_quantity_mg",
            "ss_release_units",
        ),
    )
    return tuple(Phase4FinancialInputRecord.from_row(row) for row in rows)


def load_phase5_inventory_detail(path: Path) -> tuple[Phase5FinancialDetailInputRecord, ...]:
    rows = _load_csv_rows(
        path,
        (
            "scenario_name",
            "geography_code",
            "module",
            "month_index",
            "calendar_month",
            "material_node",
            "available_nonexpired_inventory",
            "expired_quantity",
            "matched_administrable_fg_units",
            "fg_ss_mismatch_units",
            "stockout_flag",
            "excess_inventory_flag",
            "expiry_flag",
            "fg_ss_mismatch_flag",
        ),
    )
    return tuple(Phase5FinancialDetailInputRecord.from_row(row) for row in rows)


def load_phase5_monthly_inventory_summary(path: Path) -> tuple[Phase5FinancialSummaryInputRecord, ...]:
    rows = _load_csv_rows(
        path,
        (
            "scenario_name",
            "geography_code",
            "module",
            "month_index",
            "calendar_month",
            "ds_inventory_mg",
            "dp_inventory_units",
            "fg_inventory_units",
            "ss_inventory_units",
            "sublayer1_fg_inventory_units",
            "sublayer2_fg_inventory_units",
            "expired_ds_mg",
            "expired_dp_units",
            "expired_fg_units",
            "expired_ss_units",
            "unmatched_fg_units",
            "matched_administrable_fg_units",
            "stockout_flag",
            "excess_inventory_flag",
            "expiry_flag",
            "fg_ss_mismatch_flag",
        ),
    )
    return tuple(Phase5FinancialSummaryInputRecord.from_row(row) for row in rows)


def load_phase6_inputs(config: Phase6Config) -> Phase6InputBundle:
    return Phase6InputBundle(
        phase4_monthly_summary=load_phase4_monthly_summary(config.input_paths.phase4_monthly_summary),
        phase5_inventory_detail=load_phase5_inventory_detail(config.input_paths.phase5_inventory_detail),
        phase5_monthly_inventory_summary=load_phase5_monthly_inventory_summary(
            config.input_paths.phase5_monthly_inventory_summary
        ),
    )
