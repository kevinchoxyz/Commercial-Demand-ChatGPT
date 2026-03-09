"""Phase 1 validation rules."""

from __future__ import annotations

from collections import defaultdict

from ..calendar.monthly_calendar import MonthlyCalendar
from ..constants import (
    AML_SEGMENTS,
    DEMAND_BASIS_PATIENT_STARTS,
    DEMAND_BASIS_TREATED_CENSUS,
    FORECAST_GRAIN_MODULE_LEVEL,
    MDS_SEGMENTS,
)
from ..demand.cohort_engine import resolve_treatment_duration_months
from ..demand.base import DemandOutputRecord
from ..inputs.config_schema import Phase1Config
from ..inputs.loaders import InputBundle
from ..inputs.schemas import (
    CMLPrevalentPoolRecord,
    ModuleLevelForecastRecord,
    SegmentLevelForecastRecord,
    SegmentMixRecord,
    TreatmentDurationRecord,
)
from .framework import ValidationIssue, ValidationReport


def _context(
    scenario_name: str,
    geography_code: str,
    module: str,
    month_index: int,
) -> dict[str, str]:
    return {
        "scenario_name": scenario_name,
        "geography_code": geography_code,
        "module": module,
        "month_index": str(month_index),
    }


def validate_calendar_horizon(calendar: MonthlyCalendar, expected_months: int) -> list[ValidationIssue]:
    if len(calendar.months) == expected_months:
        return []
    return [
        ValidationIssue(
            code="calendar.horizon_length",
            message=f"Calendar contains {len(calendar.months)} months, expected {expected_months}.",
        )
    ]


def validate_segment_mix_totals(
    records: tuple[SegmentMixRecord, ...],
    scenario_name: str,
    module: str,
    expected_segments: tuple[str, ...],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    grouped: dict[tuple[str, int], list[SegmentMixRecord]] = defaultdict(list)
    for record in records:
        grouped[(record.geography_code, record.month_index)].append(record)

    expected_set = set(expected_segments)
    for (geography_code, month_index), group in grouped.items():
        seen_segments = [record.segment_code for record in group]
        duplicate_segments = sorted(
            {
                segment_code
                for segment_code in seen_segments
                if seen_segments.count(segment_code) > 1
            }
        )
        if duplicate_segments:
            issues.append(
                ValidationIssue(
                    code="segment_mix.duplicate_segment_row",
                    message=f"{module} mix contains duplicate segment rows: {duplicate_segments}.",
                    context=_context(scenario_name, geography_code, module, month_index),
                )
            )
        seen_segment_set = set(seen_segments)
        missing_segments = sorted(expected_set - seen_segment_set)
        unexpected_segments = sorted(seen_segment_set - expected_set)
        if missing_segments:
            issues.append(
                ValidationIssue(
                    code="segment_mix.missing_segments",
                    message=f"{module} mix is missing required segments: {missing_segments}.",
                    context=_context(scenario_name, geography_code, module, month_index),
                )
            )
        if unexpected_segments:
            issues.append(
                ValidationIssue(
                    code="segment_mix.unexpected_segments",
                    message=f"{module} mix contains unexpected segments: {unexpected_segments}.",
                    context=_context(scenario_name, geography_code, module, month_index),
                )
            )
        total_share = sum(record.segment_share for record in group)
        if abs(total_share - 1.0) > 1e-9:
            issues.append(
                ValidationIssue(
                    code="segment_mix.sum_not_one",
                    message=f"{module} segment shares sum to {total_share:.6f}, expected 1.0.",
                    context=_context(scenario_name, geography_code, module, month_index),
                )
            )
    return issues


def validate_required_mix_rows(
    forecast_rows: tuple[ModuleLevelForecastRecord, ...],
    mix_rows: tuple[SegmentMixRecord, ...],
    scenario_name: str,
    module: str,
    expected_segments: tuple[str, ...],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    grouped_mix_rows: dict[tuple[str, int], list[SegmentMixRecord]] = defaultdict(list)
    for record in mix_rows:
        grouped_mix_rows[(record.geography_code, record.month_index)].append(record)

    expected_set = set(expected_segments)
    for record in forecast_rows:
        if record.module != module:
            continue
        group = grouped_mix_rows.get((record.geography_code, record.month_index), [])
        seen_segment_set = {mix_record.segment_code for mix_record in group}
        missing_segments = sorted(expected_set - seen_segment_set)
        if missing_segments:
            issues.append(
                ValidationIssue(
                    code="segment_mix.missing_required_rows",
                    message=f"{module} allocation requires mix rows for segments: {missing_segments}.",
                    context=_context(
                        scenario_name,
                        record.geography_code,
                        module,
                        record.month_index,
                    ),
                )
            )
    return issues


def validate_module_level_forecast_contract(
    forecast_rows: tuple[ModuleLevelForecastRecord, ...],
    scenario_name: str,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    seen: set[tuple[str, str, int]] = set()
    for record in forecast_rows:
        key = (record.geography_code, record.module, record.month_index)
        if key not in seen:
            seen.add(key)
            continue
        issues.append(
            ValidationIssue(
                code="module_forecast.duplicate_row",
                message="Module-level forecast contains duplicate geography/module/month rows.",
                context=_context(
                    scenario_name,
                    record.geography_code,
                    record.module,
                    record.month_index,
                ),
            )
        )
    return issues


def validate_segment_level_forecast_contract(
    forecast_rows: tuple[SegmentLevelForecastRecord, ...],
    scenario_name: str,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    seen: set[tuple[str, str, str, int]] = set()
    for record in forecast_rows:
        key = (record.geography_code, record.module, record.segment_code, record.month_index)
        if key not in seen:
            seen.add(key)
            continue
        issues.append(
            ValidationIssue(
                code="segment_forecast.duplicate_row",
                message=(
                    "Segment-level forecast contains duplicate geography/module/segment/month rows, "
                    "so module totals cannot be aggregated deterministically."
                ),
                context=_context(
                    scenario_name,
                    record.geography_code,
                    record.module,
                    record.month_index,
                ),
            )
        )
    return issues


def validate_cml_prevalent_pool(
    config: Phase1Config,
    inputs: InputBundle,
    outputs: tuple[DemandOutputRecord, ...],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    pool_map: dict[tuple[str, int], CMLPrevalentPoolRecord] = {
        (record.geography_code, record.month_index): record for record in inputs.cml_prevalent
    }
    treated_map: dict[tuple[str, int], float] = defaultdict(float)
    for record in outputs:
        if record.module != "CML_Prevalent":
            continue
        treated_map[(record.geography_code, record.month_index)] += record.patients_treated

    for key, treated_patients in treated_map.items():
        geography_code, month_index = key
        pool_record = pool_map.get(key)
        if pool_record is None:
            issues.append(
                ValidationIssue(
                    code="cml_prevalent.missing_pool",
                    message="Missing addressable prevalent pool record for CML prevalent demand.",
                    context=_context(
                        config.scenario_name,
                        geography_code,
                        "CML_Prevalent",
                        month_index,
                    ),
                )
            )
            continue
        if treated_patients > pool_record.addressable_prevalent_pool:
            issues.append(
                ValidationIssue(
                    code="cml_prevalent.pool_exceeded",
                    message=(
                        "CML prevalent treated patients exceed addressable prevalent pool "
                        f"({treated_patients} > {pool_record.addressable_prevalent_pool})."
                    ),
                    context=_context(
                        config.scenario_name,
                        geography_code,
                        "CML_Prevalent",
                        month_index,
                    ),
                )
            )
    return issues


def validate_treatment_duration_contract(
    records: tuple[TreatmentDurationRecord, ...],
    scenario_name: str,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    seen_active_scopes: set[tuple[str, str, str]] = set()
    for record in records:
        if record.treatment_duration_months <= 0:
            issues.append(
                ValidationIssue(
                    code="treatment_duration.non_positive",
                    message="Treatment duration must be > 0 months.",
                    context={
                        "scenario_name": scenario_name,
                        "geography_code": record.geography_code,
                        "module": record.module,
                        "segment_code": record.segment_code,
                    },
                )
            )
        if not record.active_flag:
            continue
        key = (record.geography_code, record.module, record.segment_code)
        if key in seen_active_scopes:
            issues.append(
                ValidationIssue(
                    code="treatment_duration.duplicate_active_scope",
                    message="Treatment duration assumptions contain duplicate active scope rows.",
                    context={
                        "scenario_name": scenario_name,
                        "geography_code": record.geography_code,
                        "module": record.module,
                        "segment_code": record.segment_code,
                    },
                )
            )
            continue
        seen_active_scopes.add(key)
    return issues


def validate_demand_basis_audit(
    config: Phase1Config,
    inputs: InputBundle,
    outputs: tuple[DemandOutputRecord, ...],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if config.model.demand_basis == DEMAND_BASIS_TREATED_CENSUS:
        for output in outputs:
            if output.demand_basis_used != DEMAND_BASIS_TREATED_CENSUS:
                issues.append(
                    ValidationIssue(
                        code="outputs.unexpected_demand_basis",
                        message="treated_census mode must not apply cohort roll-forward audit fields.",
                        context={
                            "scenario_name": output.scenario_name,
                            "geography_code": output.geography_code,
                            "module": output.module,
                            "segment_code": output.segment_code,
                            "month_index": str(output.month_index),
                        },
                    )
                )
                continue
            if (
                abs(output.starts_input) > 1e-9
                or abs(output.continuing_patients) > 1e-9
                or abs(output.rolloff_patients) > 1e-9
                or output.treatment_duration_months_used is not None
            ):
                issues.append(
                    ValidationIssue(
                        code="outputs.unexpected_cohort_audit",
                        message="treated_census mode must not populate cohort audit fields.",
                        context={
                            "scenario_name": output.scenario_name,
                            "geography_code": output.geography_code,
                            "module": output.module,
                            "segment_code": output.segment_code,
                            "month_index": str(output.month_index),
                        },
                    )
                )
        return issues

    for output in outputs:
        try:
            expected_duration = resolve_treatment_duration_months(
                records=inputs.treatment_duration_assumptions,
                geography_code=output.geography_code,
                module=output.module,
                segment_code=output.segment_code,
            )
        except ValueError as exc:
            issues.append(
                ValidationIssue(
                    code="treatment_duration.missing_required_scope",
                    message=str(exc),
                    context={
                        "scenario_name": output.scenario_name,
                        "geography_code": output.geography_code,
                        "module": output.module,
                        "segment_code": output.segment_code,
                        "month_index": str(output.month_index),
                    },
                )
            )
            continue
        if output.demand_basis_used != DEMAND_BASIS_PATIENT_STARTS:
            issues.append(
                ValidationIssue(
                    code="outputs.missing_patient_starts_audit",
                    message="patient_starts mode must annotate outputs with demand_basis_used=patient_starts.",
                    context={
                        "scenario_name": output.scenario_name,
                        "geography_code": output.geography_code,
                        "module": output.module,
                        "segment_code": output.segment_code,
                        "month_index": str(output.month_index),
                    },
                )
            )
        if output.treatment_duration_months_used != expected_duration:
            issues.append(
                ValidationIssue(
                    code="outputs.duration_mismatch",
                    message=(
                        "Output treatment duration audit does not match the configured duration "
                        f"({output.treatment_duration_months_used} != {expected_duration})."
                    ),
                    context={
                        "scenario_name": output.scenario_name,
                        "geography_code": output.geography_code,
                        "module": output.module,
                        "segment_code": output.segment_code,
                        "month_index": str(output.month_index),
                    },
                )
            )
    return issues


def validate_month_indices(
    forecast_rows: tuple[ModuleLevelForecastRecord | SegmentLevelForecastRecord, ...],
    scenario_name: str,
    calendar: MonthlyCalendar,
    code_prefix: str,
) -> list[ValidationIssue]:
    valid_month_indices = calendar.month_indices()
    issues: list[ValidationIssue] = []
    for record in forecast_rows:
        if record.month_index in valid_month_indices:
            continue
        issues.append(
            ValidationIssue(
                code=f"{code_prefix}.month_out_of_horizon",
                message="Record falls outside the configured calendar horizon.",
                context=_context(
                    scenario_name,
                    record.geography_code,
                    record.module,
                    record.month_index,
                ),
            )
        )
    return issues


def validate_mix_month_indices(
    mix_rows: tuple[SegmentMixRecord, ...],
    scenario_name: str,
    calendar: MonthlyCalendar,
    module: str,
) -> list[ValidationIssue]:
    valid_month_indices = calendar.month_indices()
    issues: list[ValidationIssue] = []
    for record in mix_rows:
        if record.month_index in valid_month_indices:
            continue
        issues.append(
            ValidationIssue(
                code="segment_mix.month_out_of_horizon",
                message="Segment mix row falls outside the configured calendar horizon.",
                context=_context(scenario_name, record.geography_code, module, record.month_index),
            )
        )
    return issues


def validate_cml_prevalent_month_indices(
    records: tuple[CMLPrevalentPoolRecord, ...],
    scenario_name: str,
    calendar: MonthlyCalendar,
) -> list[ValidationIssue]:
    valid_month_indices = calendar.month_indices()
    issues: list[ValidationIssue] = []
    for record in records:
        if record.month_index in valid_month_indices:
            continue
        issues.append(
            ValidationIssue(
                code="cml_prevalent.month_out_of_horizon",
                message="CML prevalent pool row falls outside the configured calendar horizon.",
                context=_context(
                    scenario_name,
                    record.geography_code,
                    "CML_Prevalent",
                    record.month_index,
                ),
            )
        )
    return issues


def validate_unique_output_keys(outputs: tuple[DemandOutputRecord, ...]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    seen: set[tuple[str, str, str, str, int]] = set()
    for output in outputs:
        if output.key not in seen:
            seen.add(output.key)
            continue
        issues.append(
            ValidationIssue(
                code="outputs.duplicate_key",
                message="Duplicate output key detected.",
                context={
                    "scenario_name": output.scenario_name,
                    "geography_code": output.geography_code,
                    "module": output.module,
                    "segment_code": output.segment_code,
                    "month_index": str(output.month_index),
                },
            )
        )
    return issues


def run_phase1_validations(
    config: Phase1Config,
    inputs: InputBundle,
    calendar: MonthlyCalendar,
    outputs: tuple[DemandOutputRecord, ...],
) -> ValidationReport:
    report = ValidationReport()
    report = report.extend(validate_calendar_horizon(calendar, config.horizon.forecast_horizon_months))
    report = report.extend(validate_unique_output_keys(outputs))
    report = report.extend(
        validate_treatment_duration_contract(
            inputs.treatment_duration_assumptions,
            config.scenario_name,
        )
    )
    report = report.extend(validate_mix_month_indices(inputs.aml_segment_mix, config.scenario_name, calendar, "AML"))
    report = report.extend(validate_mix_month_indices(inputs.mds_segment_mix, config.scenario_name, calendar, "MDS"))
    report = report.extend(validate_cml_prevalent_month_indices(inputs.cml_prevalent, config.scenario_name, calendar))

    if config.model.forecast_grain == FORECAST_GRAIN_MODULE_LEVEL:
        report = report.extend(
            validate_month_indices(
                inputs.module_level_forecast,
                config.scenario_name,
                calendar,
                "module_forecast",
            )
        )
        report = report.extend(
            validate_module_level_forecast_contract(
                inputs.module_level_forecast,
                config.scenario_name,
            )
        )
        report = report.extend(
            validate_required_mix_rows(
                inputs.module_level_forecast,
                inputs.aml_segment_mix,
                config.scenario_name,
                "AML",
                AML_SEGMENTS,
            )
        )
        report = report.extend(
            validate_required_mix_rows(
                inputs.module_level_forecast,
                inputs.mds_segment_mix,
                config.scenario_name,
                "MDS",
                MDS_SEGMENTS,
            )
        )
    else:
        report = report.extend(
            validate_month_indices(
                inputs.segment_level_forecast,
                config.scenario_name,
                calendar,
                "segment_forecast",
            )
        )
        report = report.extend(
            validate_segment_level_forecast_contract(
                inputs.segment_level_forecast,
                config.scenario_name,
            )
        )

    if config.validation.enforce_segment_share_rules:
        report = report.extend(
            validate_segment_mix_totals(
                inputs.aml_segment_mix,
                config.scenario_name,
                "AML",
                AML_SEGMENTS,
            )
        )
        report = report.extend(
            validate_segment_mix_totals(
                inputs.mds_segment_mix,
                config.scenario_name,
                "MDS",
                MDS_SEGMENTS,
            )
        )

    if config.validation.enforce_cml_prevalent_pool_constraints:
        report = report.extend(validate_cml_prevalent_pool(config, inputs, outputs))

    report = report.extend(validate_demand_basis_audit(config, inputs, outputs))

    return report
