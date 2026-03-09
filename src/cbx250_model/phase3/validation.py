"""Validation rules for the deterministic Phase 3 trade layer."""

from __future__ import annotations

from collections import Counter

from ..validation.framework import ValidationIssue, ValidationReport
from .config_schema import Phase3Config
from .schemas import Phase2TradeInputRecord, Phase3TradeRecord


def run_phase3_validations(
    config: Phase3Config,
    phase2_rows: tuple[Phase2TradeInputRecord, ...],
    outputs: tuple[Phase3TradeRecord, ...],
) -> ValidationReport:
    issues: list[ValidationIssue] = []
    issues.extend(_validate_input_scenario_name(config, phase2_rows))
    issues.extend(_validate_required_trade_configuration(config, phase2_rows))
    issues.extend(_validate_unique_keys(phase2_rows, code="phase3_input.duplicate_key"))
    if config.validation.enforce_unique_output_keys:
        issues.extend(_validate_unique_keys(outputs, code="phase3_output.duplicate_key"))
    return ValidationReport(issues=tuple(issues))


def _validate_input_scenario_name(
    config: Phase3Config,
    phase2_rows: tuple[Phase2TradeInputRecord, ...],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for record in phase2_rows:
        if record.scenario_name == config.scenario_name:
            continue
        issues.append(
            ValidationIssue(
                code="phase3_input.scenario_name_mismatch",
                message=(
                    "Phase 3 scenario_name must match the upstream Phase 2 deterministic cascade "
                    "scenario_name."
                ),
                context=_context(record),
            )
        )
    return issues


def _validate_required_trade_configuration(
    config: Phase3Config,
    phase2_rows: tuple[Phase2TradeInputRecord, ...],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    seen_module_geographies = {(row.module, row.geography_code) for row in phase2_rows}
    for module, geography_code in sorted(seen_module_geographies):
        if geography_code not in config.geography_defaults:
            issues.append(
                ValidationIssue(
                    code="phase3_config.missing_geography_defaults",
                    message="Phase 3 is missing geography_defaults for an upstream Phase 2 geography.",
                    context={
                        "module": module,
                        "geography_code": geography_code,
                    },
                )
            )
        if (module, geography_code) not in config.launch_events:
            issues.append(
                ValidationIssue(
                    code="phase3_config.missing_launch_event",
                    message="Phase 3 is missing a launch_event for an upstream Phase 2 module/geography.",
                    context={
                        "module": module,
                        "geography_code": geography_code,
                    },
                )
            )
    return issues


def _validate_unique_keys(
    records: tuple[Phase2TradeInputRecord, ...] | tuple[Phase3TradeRecord, ...],
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


def _context(record: Phase2TradeInputRecord | Phase3TradeRecord) -> dict[str, str]:
    return {
        "scenario_name": record.scenario_name,
        "geography_code": record.geography_code,
        "module": record.module,
        "segment_code": record.segment_code,
        "month_index": str(record.month_index),
    }
