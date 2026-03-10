"""CSV output writers for deterministic Phase 4 production scheduling."""

from __future__ import annotations

from csv import DictWriter
from pathlib import Path

from .schemas import ScheduleDetailRecord, ScheduleMonthlySummaryRecord

PHASE4_DETAIL_HEADERS = (
    "scenario_name",
    "stage",
    "module",
    "geography_code",
    "batch_number",
    "demand_month_index",
    "demand_calendar_month",
    "month_index",
    "calendar_month",
    "planned_start_month_index",
    "planned_start_month",
    "planned_release_month_index",
    "planned_release_month",
    "batch_quantity",
    "quantity_unit",
    "cumulative_released_quantity",
    "capacity_used",
    "capacity_limit",
    "capacity_metric",
    "capacity_flag",
    "supply_gap_flag",
    "excess_build_flag",
    "bullwhip_review_flag",
    "ss_fg_sync_flag",
    "notes",
)

PHASE4_SUMMARY_HEADERS = (
    "scenario_name",
    "geography_code",
    "module",
    "month_index",
    "calendar_month",
    "patient_fg_demand_units",
    "ex_factory_fg_demand_units",
    "underlying_patient_consumption_units",
    "channel_inventory_build_units",
    "launch_fill_component_units",
    "fg_release_units",
    "dp_release_units",
    "ds_release_quantity_mg",
    "ds_release_quantity_g",
    "ds_release_quantity_kg",
    "ss_release_units",
    "cumulative_fg_released",
    "cumulative_ss_released",
    "unmet_demand_units",
    "capacity_flag",
    "supply_gap_flag",
    "excess_build_flag",
    "bullwhip_review_flag",
    "ss_fg_sync_flag",
    "stepdown_applied",
    "notes",
)


def write_phase4_detail_outputs(path: Path, outputs: tuple[ScheduleDetailRecord, ...]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = DictWriter(handle, fieldnames=PHASE4_DETAIL_HEADERS)
        writer.writeheader()
        for record in outputs:
            writer.writerow(record.as_csv_row())
    return path


def write_phase4_monthly_summary(path: Path, outputs: tuple[ScheduleMonthlySummaryRecord, ...]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = DictWriter(handle, fieldnames=PHASE4_SUMMARY_HEADERS)
        writer.writeheader()
        for record in outputs:
            writer.writerow(record.as_csv_row())
    return path
