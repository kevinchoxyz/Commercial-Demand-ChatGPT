"""Summary helpers for the deterministic Phase 3 trade layer."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .runner import Phase3RunResult


def build_phase3_run_summary(
    result: "Phase3RunResult", output_path: str | None = None
) -> dict[str, object]:
    module_counts = Counter(output.module for output in result.outputs)
    flagged_rows = sum(1 for row in result.outputs if row.bullwhip_flag)
    return {
        "scenario_name": result.config.scenario_name,
        "input_row_count": len(result.inputs.phase2_deterministic_cascade),
        "output_row_count": len(result.outputs),
        "output_rows_by_module": dict(module_counts),
        "total_patient_fg_demand_units": sum(row.patient_fg_demand_units for row in result.outputs),
        "total_sublayer2_pull_units": sum(row.sublayer2_pull_units for row in result.outputs),
        "total_ex_factory_fg_demand_units": sum(
            row.ex_factory_fg_demand_units for row in result.outputs
        ),
        "total_ss_site_stocking_units": sum(row.ss_site_stocking_units for row in result.outputs),
        "bullwhip_flag_row_count": flagged_rows,
        "validation_issue_count": len(result.validation.issues),
        "authoritative_output_file": output_path,
    }
