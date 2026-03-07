"""Summary helpers for the Phase 2 deterministic cascade."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .runner import Phase2RunResult


def build_phase2_run_summary(result: "Phase2RunResult", output_path: str | None = None) -> dict[str, object]:
    module_counts = Counter(output.module for output in result.outputs)
    return {
        "scenario_name": result.config.scenario_name,
        "dose_basis": result.config.model.dose_basis,
        "input_row_count": len(result.inputs.phase1_monthlyized_output),
        "output_row_count": len(result.outputs),
        "output_rows_by_module": dict(module_counts),
        "total_patients_treated": sum(row.patients_treated for row in result.outputs),
        "total_fg_units_required": sum(row.fg_units_required for row in result.outputs),
        "total_ss_units_required": sum(row.ss_units_required for row in result.outputs),
        "total_dp_units_required": sum(row.dp_units_required for row in result.outputs),
        "total_ds_required": sum(row.ds_required for row in result.outputs),
        "validation_issue_count": len(result.validation.issues),
        "authoritative_output_file": output_path,
    }
