"""Summary helpers for deterministic Phase 4 production scheduling."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .runner import Phase4RunResult


def build_phase4_run_summary(
    result: "Phase4RunResult",
    detail_output_path: str | None = None,
    monthly_summary_path: str | None = None,
) -> dict[str, object]:
    stage_counts = Counter(record.stage for record in result.schedule_detail)
    return {
        "scenario_name": result.config.scenario_name,
        "input_row_count": len(result.inputs.phase3_trade_layer),
        "schedule_detail_row_count": len(result.schedule_detail),
        "schedule_detail_rows_by_stage": dict(stage_counts),
        "monthly_summary_row_count": len(result.monthly_summary),
        "total_fg_release_units": sum(row.fg_release_units for row in result.monthly_summary),
        "total_dp_release_units": sum(row.dp_release_units for row in result.monthly_summary),
        "total_ds_release_quantity_mg": sum(row.ds_release_quantity_mg for row in result.monthly_summary),
        "total_ss_release_units": sum(row.ss_release_units for row in result.monthly_summary),
        "supply_gap_row_count": sum(1 for row in result.monthly_summary if row.supply_gap_flag),
        "bullwhip_review_row_count": sum(1 for row in result.monthly_summary if row.bullwhip_review_flag),
        "validation_issue_count": len(result.validation.issues),
        "authoritative_schedule_detail_file": detail_output_path,
        "authoritative_monthly_summary_file": monthly_summary_path,
    }
