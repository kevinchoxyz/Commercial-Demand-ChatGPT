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
    aggregated_detail: dict[tuple[str, str, str, int], dict[str, float]] = defaultdict(
        lambda: {"FG": 0.0, "DP": 0.0, "DS": 0.0, "SS": 0.0}
    )
    for record in phase4_schedule_detail:
        aggregated_detail[
            (
                record.scenario_name,
                record.geography_code,
                record.module,
                record.demand_month_index,
            )
        ][record.stage] += record.batch_quantity

    issues: list[ValidationIssue] = []
    tolerance = config.validation.reconciliation_tolerance_units
    for summary in phase4_monthly_summary:
        key = (summary.scenario_name, summary.geography_code, summary.module, summary.month_index)
        detail_totals = aggregated_detail[key]
        comparisons = {
            "FG": (detail_totals["FG"], summary.fg_release_units),
        }
        for stage, (detail_value, summary_value) in comparisons.items():
            if abs(detail_value - summary_value) > tolerance:
                issues.append(
                    ValidationIssue(
                        code="PHASE5_PHASE4_RECONCILIATION_MISMATCH",
                        message=(
                            f"Phase 4 detail releases for stage {stage} do not reconcile to the Phase 4 monthly summary."
                        ),
                        context={
                            "geography_code": summary.geography_code,
                            "module": summary.module,
                            "month_index": str(summary.month_index),
                            "stage": stage,
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
