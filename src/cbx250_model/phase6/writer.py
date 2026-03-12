"""CSV output writers for deterministic Phase 6 financial analytics."""

from __future__ import annotations

from csv import DictWriter
from pathlib import Path

from .schemas import FinancialAnnualSummaryRecord, FinancialDetailRecord, FinancialMonthlySummaryRecord

PHASE6_DETAIL_HEADERS = (
    "scenario_name",
    "geography_code",
    "module",
    "month_index",
    "calendar_month",
    "financial_node_or_stage",
    "quantity_basis",
    "quantity_value",
    "standard_cost_rate",
    "inventory_value",
    "release_value",
    "expired_value",
    "carrying_cost_value",
    "matched_administrable_fg_value",
    "unmatched_fg_value_at_risk",
    "notes",
)

PHASE6_MONTHLY_SUMMARY_HEADERS = (
    "scenario_name",
    "geography_code",
    "module",
    "month_index",
    "calendar_month",
    "ds_inventory_value",
    "dp_inventory_value",
    "fg_inventory_value",
    "ss_inventory_value",
    "sublayer1_fg_inventory_value",
    "sublayer2_fg_inventory_value",
    "total_inventory_value",
    "ds_release_value",
    "dp_release_value",
    "fg_release_value",
    "ss_release_value",
    "total_release_value",
    "expired_ds_value",
    "expired_dp_value",
    "expired_fg_value",
    "expired_ss_value",
    "expired_value_total",
    "ds_carrying_cost",
    "dp_carrying_cost",
    "fg_carrying_cost",
    "ss_carrying_cost",
    "trade_node_fg_carrying_cost",
    "carrying_cost_total",
    "matched_administrable_fg_value",
    "unmatched_fg_value_at_risk",
    "stockout_flag",
    "excess_inventory_flag",
    "expiry_flag",
    "fg_ss_mismatch_flag",
    "notes",
)

PHASE6_ANNUAL_SUMMARY_HEADERS = (
    "scenario_name",
    "geography_code",
    "module",
    "calendar_year",
    "ending_total_inventory_value",
    "total_release_value",
    "total_expired_value",
    "total_carrying_cost",
    "ending_matched_administrable_fg_value",
    "ending_unmatched_fg_value_at_risk",
    "stockout_month_count",
    "excess_inventory_month_count",
    "expiry_month_count",
    "fg_ss_mismatch_month_count",
    "notes",
)


def write_phase6_financial_detail(path: Path, outputs: tuple[FinancialDetailRecord, ...]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = DictWriter(handle, fieldnames=PHASE6_DETAIL_HEADERS)
        writer.writeheader()
        for record in outputs:
            writer.writerow(record.as_csv_row())
    return path


def write_phase6_monthly_summary(path: Path, outputs: tuple[FinancialMonthlySummaryRecord, ...]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = DictWriter(handle, fieldnames=PHASE6_MONTHLY_SUMMARY_HEADERS)
        writer.writeheader()
        for record in outputs:
            writer.writerow(record.as_csv_row())
    return path


def write_phase6_annual_summary(path: Path, outputs: tuple[FinancialAnnualSummaryRecord, ...]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = DictWriter(handle, fieldnames=PHASE6_ANNUAL_SUMMARY_HEADERS)
        writer.writeheader()
        for record in outputs:
            writer.writerow(record.as_csv_row())
    return path
