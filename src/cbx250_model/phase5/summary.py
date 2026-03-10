"""Summary helpers for deterministic Phase 5 inventory and shelf-life."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .runner import Phase5RunResult


def build_phase5_run_summary(
    result: "Phase5RunResult",
    detail_output_path: str | None = None,
    summary_output_path: str | None = None,
    cohort_audit_output_path: str | None = None,
) -> dict[str, object]:
    return {
        "scenario_name": result.config.scenario_name,
        "input_phase3_row_count": len(result.inputs.phase3_trade_layer),
        "input_phase4_schedule_detail_row_count": len(result.inputs.phase4_schedule_detail),
        "input_phase4_monthly_summary_row_count": len(result.inputs.phase4_monthly_summary),
        "inventory_detail_row_count": len(result.inventory_detail),
        "monthly_summary_row_count": len(result.monthly_summary),
        "cohort_audit_row_count": len(result.cohort_audit),
        "stockout_row_count": sum(1 for row in result.monthly_summary if row.stockout_flag),
        "excess_inventory_row_count": sum(1 for row in result.monthly_summary if row.excess_inventory_flag),
        "expiry_row_count": sum(1 for row in result.monthly_summary if row.expiry_flag),
        "fg_ss_mismatch_row_count": sum(1 for row in result.monthly_summary if row.fg_ss_mismatch_flag),
        "validation_issue_count": len(result.validation.issues),
        "authoritative_inventory_detail_file": detail_output_path,
        "authoritative_monthly_inventory_summary_file": summary_output_path,
        "authoritative_cohort_audit_file": cohort_audit_output_path,
    }
