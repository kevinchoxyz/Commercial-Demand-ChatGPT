"""Simple summary helpers for the Phase 1 scaffold."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

from ..validation.framework import ValidationReport

if TYPE_CHECKING:
    from ..demand.phase1_runner import Phase1RunResult


def build_run_summary(result: "Phase1RunResult") -> dict[str, object]:
    module_counts = Counter(output.module for output in result.outputs)
    segment_counts = Counter(output.segment_code for output in result.outputs)
    return {
        "scenario_name": result.config.scenario_name,
        "forecast_grain": result.config.model.forecast_grain,
        "demand_basis": result.config.model.demand_basis,
        "calendar_months": len(result.calendar.months),
        "geography_count": len(result.dimensions["dim_geography"]),
        "dimension_counts": {name: len(rows) for name, rows in result.dimensions.items()},
        "output_row_count": len(result.outputs),
        "output_rows_by_module": dict(module_counts),
        "output_rows_by_segment": dict(segment_counts),
        "validation_issue_count": len(result.validation.issues),
    }


def format_validation_report(report: ValidationReport) -> str:
    if not report.issues:
        return ""
    lines = ["Validation issues:"]
    for issue in report.issues:
        context = ", ".join(f"{key}={value}" for key, value in issue.context.items())
        suffix = f" [{context}]" if context else ""
        lines.append(f"- {issue.level.upper()} {issue.code}: {issue.message}{suffix}")
    return "\n".join(lines)
