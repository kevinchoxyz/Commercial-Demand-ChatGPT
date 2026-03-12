"""Validation rules for deterministic Phase 6 financial analytics."""

from __future__ import annotations

from collections import Counter

from ..validation.framework import ValidationIssue, ValidationReport
from .config_schema import Phase6Config
from .schemas import (
    FinancialAnnualSummaryRecord,
    FinancialDetailRecord,
    FinancialMonthlySummaryRecord,
    Phase4FinancialInputRecord,
    Phase5FinancialDetailInputRecord,
    Phase5FinancialSummaryInputRecord,
)


def run_phase6_validations(
    config: Phase6Config,
    phase4_monthly_summary: tuple[Phase4FinancialInputRecord, ...],
    phase5_inventory_detail: tuple[Phase5FinancialDetailInputRecord, ...],
    phase5_monthly_summary: tuple[Phase5FinancialSummaryInputRecord, ...],
    financial_detail: tuple[FinancialDetailRecord, ...],
    monthly_summary: tuple[FinancialMonthlySummaryRecord, ...],
    annual_summary: tuple[FinancialAnnualSummaryRecord, ...],
) -> ValidationReport:
    issues: list[ValidationIssue] = []
    issues.extend(
        _scenario_mismatch_issues(
            config,
            phase4_monthly_summary,
            phase5_inventory_detail,
            phase5_monthly_summary,
        )
    )
    if config.validation.enforce_unique_output_keys:
        issues.extend(_duplicate_key_issues("PHASE6_DUPLICATE_DETAIL_KEY", "financial_detail", (row.key for row in financial_detail)))
        issues.extend(
            _duplicate_key_issues(
                "PHASE6_DUPLICATE_MONTHLY_SUMMARY_KEY",
                "monthly_financial_summary",
                (row.key for row in monthly_summary),
            )
        )
        issues.extend(
            _duplicate_key_issues(
                "PHASE6_DUPLICATE_ANNUAL_SUMMARY_KEY",
                "annual_financial_summary",
                (row.key for row in annual_summary),
            )
        )
    return ValidationReport(tuple(issues))


def _scenario_mismatch_issues(
    config: Phase6Config,
    phase4_monthly_summary: tuple[Phase4FinancialInputRecord, ...],
    phase5_inventory_detail: tuple[Phase5FinancialDetailInputRecord, ...],
    phase5_monthly_summary: tuple[Phase5FinancialSummaryInputRecord, ...],
) -> list[ValidationIssue]:
    observed = {
        "phase4_monthly_summary": {row.scenario_name for row in phase4_monthly_summary},
        "phase5_inventory_detail": {row.scenario_name for row in phase5_inventory_detail},
        "phase5_monthly_summary": {row.scenario_name for row in phase5_monthly_summary},
    }
    issues: list[ValidationIssue] = []
    for location, values in observed.items():
        if values and values != {config.scenario_name}:
            issues.append(
                ValidationIssue(
                    code="PHASE6_SCENARIO_MISMATCH",
                    message=(
                        "Phase 6 scenario_name must match the upstream scenario_name values. "
                        f"Observed {location} values: {sorted(values)}."
                    ),
                    context={"location": location},
                )
            )
    return issues


def _duplicate_key_issues(code: str, location: str, keys) -> list[ValidationIssue]:
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
