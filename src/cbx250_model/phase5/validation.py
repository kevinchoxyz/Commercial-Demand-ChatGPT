"""Validation rules for deterministic Phase 5 inventory and shelf-life."""

from __future__ import annotations

from collections import Counter, defaultdict

from ..validation.framework import ValidationIssue, ValidationReport
from .config_schema import Phase5Config
from .schemas import (
    CohortAuditRecord,
    InventoryDetailRecord,
    InventoryMonthlySummaryRecord,
    Phase3InventoryInputRecord,
    Phase4MonthlySummaryInputRecord,
    Phase4ScheduleDetailInputRecord,
)


def run_phase5_validations(
    config: Phase5Config,
    phase3_trade_layer: tuple[Phase3InventoryInputRecord, ...],
    phase4_schedule_detail: tuple[Phase4ScheduleDetailInputRecord, ...],
    phase4_monthly_summary: tuple[Phase4MonthlySummaryInputRecord, ...],
    inventory_detail: tuple[InventoryDetailRecord, ...],
    monthly_summary: tuple[InventoryMonthlySummaryRecord, ...],
    cohort_audit: tuple[CohortAuditRecord, ...],
) -> ValidationReport:
    issues: list[ValidationIssue] = []
    issues.extend(_scenario_mismatch_issues(config, phase3_trade_layer, phase4_schedule_detail, phase4_monthly_summary))
    if config.validation.reconcile_phase4_receipts:
        issues.extend(
            _phase4_reconciliation_issues(
                config,
                phase4_schedule_detail,
                phase4_monthly_summary,
            )
        )
    if config.validation.enforce_unique_output_keys:
        issues.extend(
            _duplicate_key_issues(
                "PHASE5_DUPLICATE_DETAIL_KEY",
                "inventory_detail",
                (record.key for record in inventory_detail),
            )
        )
        issues.extend(
            _duplicate_key_issues(
                "PHASE5_DUPLICATE_SUMMARY_KEY",
                "monthly_inventory_summary",
                (record.key for record in monthly_summary),
            )
        )
        issues.extend(
            _duplicate_key_issues(
                "PHASE5_DUPLICATE_COHORT_KEY",
                "cohort_audit",
                (record.key for record in cohort_audit),
            )
        )
    issues.extend(_negative_balance_issues(inventory_detail))
    return ValidationReport(tuple(issues))


def _scenario_mismatch_issues(
    config: Phase5Config,
    phase3_trade_layer: tuple[Phase3InventoryInputRecord, ...],
    phase4_schedule_detail: tuple[Phase4ScheduleDetailInputRecord, ...],
    phase4_monthly_summary: tuple[Phase4MonthlySummaryInputRecord, ...],
) -> list[ValidationIssue]:
    observed = {
        "phase3_trade_layer": {row.scenario_name for row in phase3_trade_layer},
        "phase4_schedule_detail": {row.scenario_name for row in phase4_schedule_detail},
        "phase4_monthly_summary": {row.scenario_name for row in phase4_monthly_summary},
    }
    issues: list[ValidationIssue] = []
    for location, values in observed.items():
        if values and values != {config.scenario_name}:
            issues.append(
                ValidationIssue(
                    code="PHASE5_SCENARIO_MISMATCH",
                    message=(
                        "Phase 5 scenario_name must match the upstream scenario_name values. "
                        f"Observed {location} values: {sorted(values)}."
                    ),
                    context={"location": location},
                )
            )
    return issues


def _phase4_reconciliation_issues(
    config: Phase5Config,
    phase4_schedule_detail: tuple[Phase4ScheduleDetailInputRecord, ...],
    phase4_monthly_summary: tuple[Phase4MonthlySummaryInputRecord, ...],
) -> list[ValidationIssue]:
    aggregated_detail: dict[tuple[str, str], float] = defaultdict(float)
    aggregated_summary: dict[tuple[str, str], float] = defaultdict(float)
    for record in phase4_schedule_detail:
        if record.stage == "FG":
            aggregated_detail[
                (
                    record.scenario_name,
                    record.geography_code,
                )
            ] += record.allocated_support_quantity

    for summary in phase4_monthly_summary:
        aggregated_summary[
            (
                summary.scenario_name,
                summary.geography_code,
            )
        ] += summary.fg_release_units

    issues: list[ValidationIssue] = []
    tolerance = config.validation.reconciliation_tolerance_units
    for key, summary_value in aggregated_summary.items():
        detail_value = aggregated_detail[key]
        if abs(detail_value - summary_value) > tolerance:
            issues.append(
                ValidationIssue(
                    code="PHASE5_PHASE4_RECONCILIATION_MISMATCH",
                    message=(
                        "Phase 4 allocated FG support quantities do not reconcile to the Phase 4 monthly summary."
                    ),
                    context={
                        "scenario_name": key[0],
                        "geography_code": key[1],
                        "module": "ALL",
                        "stage": "FG",
                    },
                )
            )
    return issues


def _duplicate_key_issues(
    code: str,
    location: str,
    keys,
) -> list[ValidationIssue]:
    counter = Counter(keys)
    return [
        ValidationIssue(
            code=code,
            message=f"{location} key {key!r} is duplicated.",
            context={"location": location},
        )
        for key, count in counter.items()
        if count > 1
    ]


def _negative_balance_issues(
    inventory_detail: tuple[InventoryDetailRecord, ...],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for record in inventory_detail:
        if record.ending_inventory < -1e-9:
            issues.append(
                ValidationIssue(
                    code="PHASE5_NEGATIVE_ENDING_INVENTORY",
                    message="Ending inventory must not be negative.",
                    context={
                        "geography_code": record.geography_code,
                        "module": record.module,
                        "month_index": str(record.month_index),
                        "material_node": record.material_node,
                    },
                )
            )
    return issues
