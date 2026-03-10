"""Validation rules for deterministic Phase 4 production scheduling."""

from __future__ import annotations

from collections import Counter

from ..validation.framework import ValidationIssue, ValidationReport
from .config_schema import Phase4Config
from .schemas import Phase3SchedulingInputRecord, ScheduleDetailRecord, ScheduleMonthlySummaryRecord


def run_phase4_validations(
    config: Phase4Config,
    phase3_trade_layer: tuple[Phase3SchedulingInputRecord, ...],
    schedule_detail: tuple[ScheduleDetailRecord, ...],
    monthly_summary: tuple[ScheduleMonthlySummaryRecord, ...],
) -> ValidationReport:
    issues: list[ValidationIssue] = []
    upstream_scenarios = {row.scenario_name for row in phase3_trade_layer}
    if upstream_scenarios and upstream_scenarios != {config.scenario_name}:
        issues.append(
            ValidationIssue(
                code="PHASE4_SCENARIO_MISMATCH",
                severity="error",
                message=(
                    "Phase 4 scenario_name must match the upstream Phase 3 trade-layer scenario_name. "
                    f"Observed upstream values: {sorted(upstream_scenarios)}."
                ),
                location="phase3_trade_layer",
            )
        )
    if config.validation.enforce_unique_output_keys:
        issues.extend(_duplicate_detail_key_issues(schedule_detail))
        issues.extend(_duplicate_summary_key_issues(monthly_summary))
    return ValidationReport(tuple(issues))


def _duplicate_detail_key_issues(
    records: tuple[ScheduleDetailRecord, ...],
) -> list[ValidationIssue]:
    counter = Counter(record.key for record in records)
    return [
        ValidationIssue(
            code="PHASE4_DUPLICATE_DETAIL_KEY",
            severity="error",
            message=f"Phase 4 schedule detail key {key!r} is duplicated.",
            location="schedule_detail",
        )
        for key, count in counter.items()
        if count > 1
    ]


def _duplicate_summary_key_issues(
    records: tuple[ScheduleMonthlySummaryRecord, ...],
) -> list[ValidationIssue]:
    counter = Counter(record.key for record in records)
    return [
        ValidationIssue(
            code="PHASE4_DUPLICATE_MONTHLY_SUMMARY_KEY",
            severity="error",
            message=f"Phase 4 monthly summary key {key!r} is duplicated.",
            location="monthly_summary",
        )
        for key, count in counter.items()
        if count > 1
    ]
