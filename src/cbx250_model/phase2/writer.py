"""CSV output writer for the Phase 2 deterministic cascade."""

from __future__ import annotations

from csv import DictWriter
from pathlib import Path

from .schemas import Phase2CascadeRecord

PHASE2_OUTPUT_HEADERS = (
    "scenario_name",
    "geography_code",
    "module",
    "segment_code",
    "month_index",
    "calendar_month",
    "patients_treated",
    "doses_required",
    "mg_per_dose_before_reduction",
    "mg_per_dose_after_reduction",
    "mg_required",
    "fg_units_before_pack_yield",
    "fg_units_required",
    "ss_units_required",
    "dp_units_required",
    "ds_required",
    "ds_required_mg",
    "ds_required_g",
    "ds_required_kg",
    "dose_basis_used",
    "dose_reduction_applied",
    "dose_reduction_pct",
    "adherence_rate_used",
    "free_goods_pct_used",
    "fg_vialing_rule_used",
    "fg_mg_per_unit_used",
    "ss_ratio_to_fg_used",
    "planning_yields_used",
    "phase1_source_frequency",
    "phase1_source_grain",
    "phase1_source_sheet",
    "phase1_profile_id_used",
    "notes",
)


def write_phase2_outputs(path: Path, outputs: tuple[Phase2CascadeRecord, ...]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = DictWriter(handle, fieldnames=PHASE2_OUTPUT_HEADERS)
        writer.writeheader()
        for record in outputs:
            writer.writerow(record.as_csv_row())
    return path
