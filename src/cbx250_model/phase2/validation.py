"""Validation rules for the Phase 2 deterministic cascade."""

from __future__ import annotations

from collections import Counter

from ..validation.framework import ValidationIssue, ValidationReport
from .config_schema import Phase2Config
from .schemas import Phase1MonthlyizedOutputRecord, Phase2CascadeRecord


def run_phase2_validations(
    config: Phase2Config,
    phase1_rows: tuple[Phase1MonthlyizedOutputRecord, ...],
    outputs: tuple[Phase2CascadeRecord, ...],
) -> ValidationReport:
    issues: list[ValidationIssue] = []
    issues.extend(_validate_input_scenario_name(config, phase1_rows))
    issues.extend(_validate_unique_keys(phase1_rows, code="phase2_input.duplicate_key"))
    if config.validation.enforce_unique_output_keys:
        issues.extend(_validate_unique_keys(outputs, code="phase2_output.duplicate_key"))
    return ValidationReport(issues=tuple(issues))


def _validate_input_scenario_name(
    config: Phase2Config,
    phase1_rows: tuple[Phase1MonthlyizedOutputRecord, ...],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for record in phase1_rows:
        if record.scenario_name == config.scenario_name:
            continue
        issues.append(
            ValidationIssue(
                code="phase2_input.scenario_name_mismatch",
                message=(
                    "Phase 2 scenario_name must match the upstream Phase 1 normalized output scenario_name."
                ),
                context=_context(record),
            )
        )
    return issues


def _validate_unique_keys(
    records: tuple[Phase1MonthlyizedOutputRecord, ...] | tuple[Phase2CascadeRecord, ...],
    *,
    code: str,
) -> list[ValidationIssue]:
    counts = Counter(record.key for record in records)
    duplicates = [record for record in records if counts[record.key] > 1]
    issues: list[ValidationIssue] = []
    seen_keys: set[tuple[str, str, str, str, int]] = set()
    for record in duplicates:
        if record.key in seen_keys:
            continue
        seen_keys.add(record.key)
        issues.append(
            ValidationIssue(
                code=code,
                message="Duplicate rows found at stable grain scenario x geography x module x segment x month.",
                context=_context(record),
            )
        )
    return issues


def _context(record: Phase1MonthlyizedOutputRecord | Phase2CascadeRecord) -> dict[str, str]:
    return {
        "scenario_name": record.scenario_name,
        "geography_code": record.geography_code,
        "module": record.module,
        "segment_code": record.segment_code,
        "month_index": str(record.month_index),
    }
