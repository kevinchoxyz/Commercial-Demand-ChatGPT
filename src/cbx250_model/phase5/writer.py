"""CSV output writers for deterministic Phase 5 inventory and shelf-life."""

from __future__ import annotations

from csv import DictWriter
from pathlib import Path

from .schemas import CohortAuditRecord, InventoryDetailRecord, InventoryMonthlySummaryRecord

PHASE5_DETAIL_HEADERS = (
    "scenario_name",
    "geography_code",
    "module",
    "month_index",
    "calendar_month",
    "material_node",
    "opening_inventory",
    "receipts",
    "issues",
    "expired_quantity",
    "ending_inventory",
    "available_nonexpired_inventory",
    "demand_signal_units",
    "required_administrable_demand_units",
    "policy_excluded_channel_build_units",
    "inventory_policy_gap_units",
    "cover_demand_units",
    "effective_cover_demand_units",
    "shortfall_units",
    "months_of_cover",
    "stockout_flag",
    "excess_inventory_flag",
    "expiry_flag",
    "fg_ss_mismatch_flag",
    "matched_administrable_fg_units",
    "fg_ss_mismatch_units",
    "notes",
)

PHASE5_SUMMARY_HEADERS = (
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
    "notes",
)

PHASE5_COHORT_AUDIT_HEADERS = (
    "scenario_name",
    "geography_code",
    "module",
    "month_index",
    "calendar_month",
    "material_node",
    "cohort_id",
    "original_receipt_month_index",
    "receipt_month_index",
    "expiry_month_index",
    "ending_quantity",
    "notes",
)


def write_phase5_inventory_detail(path: Path, outputs: tuple[InventoryDetailRecord, ...]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = DictWriter(handle, fieldnames=PHASE5_DETAIL_HEADERS)
        writer.writeheader()
        for record in outputs:
            writer.writerow(record.as_csv_row())
    return path


def write_phase5_monthly_summary(
    path: Path,
    outputs: tuple[InventoryMonthlySummaryRecord, ...],
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = DictWriter(handle, fieldnames=PHASE5_SUMMARY_HEADERS)
        writer.writeheader()
        for record in outputs:
            writer.writerow(record.as_csv_row())
    return path


def write_phase5_cohort_audit(path: Path, outputs: tuple[CohortAuditRecord, ...]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = DictWriter(handle, fieldnames=PHASE5_COHORT_AUDIT_HEADERS)
        writer.writeheader()
        for record in outputs:
            writer.writerow(record.as_csv_row())
    return path
