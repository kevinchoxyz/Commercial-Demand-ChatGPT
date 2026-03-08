"""Import the business-facing model assumptions workbook into normalized artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import csv
import json
import os
import re

from ..constants import (
    PHASE1_DISABLED_CAPABILITIES,
    PHASE1_MODULES,
    PHASE2_BUILD_SCOPE,
    PHASE2_UPSTREAM_DEMAND_CONTRACT,
    SUPPORTED_CO_PACK_MODES,
    SUPPORTED_DOSE_BASES,
    SUPPORTED_FG_VIALING_RULES,
    SUPPORTED_FORECAST_FREQUENCIES,
    SUPPORTED_FORECAST_GRAINS,
)
from .excel_import import WorkbookReader

EXCEL_EPOCH = date(1899, 12, 30)

SCENARIO_CONTROLS_HEADERS = (
    "scenario_name",
    "scenario_description",
    "active_flag",
    "forecast_grain",
    "forecast_frequency",
    "dose_basis_default",
    "base_currency",
    "notes",
)
LAUNCH_TIMING_HEADERS = (
    "scenario_name",
    "module",
    "geography_code",
    "initial_approval_date",
    "launch_offset_months",
    "active_flag",
    "notes",
)
DOSING_HEADERS = (
    "scenario_name",
    "module",
    "geography_code",
    "segment_code",
    "dose_basis",
    "fixed_dose_mg",
    "weight_based_dose_mg_per_kg",
    "average_patient_weight_kg",
    "doses_per_patient_per_month",
    "step_up_enabled",
    "step_up_schedule_id",
    "dose_reduction_enabled",
    "dose_reduction_pct",
    "adherence_rate",
    "free_goods_pct",
    "active_flag",
    "notes",
)
PRODUCT_HEADERS = (
    "scenario_name",
    "parameter_scope",
    "module",
    "geography_code",
    "ds_qty_per_dp_unit_mg",
    "dp_concentration_mg_per_ml",
    "dp_fill_volume_ml",
    "fg_mg_per_unit",
    "active_flag",
    "notes",
)
YIELD_HEADERS = (
    "scenario_name",
    "parameter_scope",
    "module",
    "geography_code",
    "ds_to_dp_yield",
    "dp_to_fg_yield",
    "fg_pack_yield",
    "ss_yield",
    "ds_overage_factor",
    "active_flag",
    "notes",
)
PACKAGING_HEADERS = (
    "scenario_name",
    "module",
    "geography_code",
    "fg_vialing_rule",
    "single_patient_use_only",
    "vial_sharing_allowed",
    "vials_per_carton",
    "partial_pack_handling",
    "active_flag",
    "notes",
)
SS_HEADERS = (
    "scenario_name",
    "module",
    "geography_code",
    "ss_ratio_to_fg",
    "co_pack_mode",
    "active_flag",
    "notes",
)
CML_PREVALENT_HEADERS = (
    "scenario_name",
    "geography_code",
    "addressable_prevalent_pool",
    "launch_year_index",
    "launch_month_index",
    "duration_months",
    "curve_profile_id",
    "bolus_start_year",
    "bolus_end_year",
    "exhaustion_year",
    "exhaustion_rule",
    "source",
    "active_flag",
    "notes",
)
TRADE_HEADERS = (
    "scenario_name",
    "module",
    "geography_code",
    "trade_rule_placeholder",
    "inventory_rule_placeholder",
    "active_flag",
    "notes",
)

ALLOWED_PARAMETER_SCOPES = ("scenario_default", "module_override")
ALLOWED_WORKBOOK_VIALING_RULES = ("patient_dose_ceiling",)
ALLOWED_PARTIAL_PACK_HANDLING = ("full_pack_consumed",)
ALLOWED_EXHAUSTION_RULES = ("track_vs_pool", "placeholder_metadata_only", "validate_only")
ENGINE_VIALING_RULE_BY_WORKBOOK_VALUE = {"patient_dose_ceiling": "ceil_mg_per_unit_no_sharing"}


@dataclass(frozen=True)
class AssumptionsWorkbookContext:
    scenario_name: str
    scenario_description: str
    forecast_grain: str
    forecast_frequency: str
    dose_basis_default: str
    base_currency: str
    notes: str


@dataclass(frozen=True)
class AssumptionsImportResult:
    workbook_path: Path
    output_dir: Path
    context: AssumptionsWorkbookContext
    file_paths: dict[str, Path]
    row_counts: dict[str, int]
    warnings: tuple[str, ...]


def import_model_assumptions_workbook(
    workbook_path: Path,
    *,
    output_dir: Path | None = None,
    scenario_name_override: str | None = None,
) -> AssumptionsImportResult:
    reader = WorkbookReader(workbook_path)
    context, allowed_scenario_names, scenario_controls_rows = _load_scenario_controls(
        reader,
        scenario_name_override=scenario_name_override,
    )
    launch_timing_rows = _normalize_launch_timing(
        reader.read_table("Launch_Timing", LAUNCH_TIMING_HEADERS),
        allowed_scenario_names=allowed_scenario_names,
        scenario_name=context.scenario_name,
    )
    dosing_rows = _normalize_dosing_assumptions(
        reader.read_table("Dosing_Assumptions", DOSING_HEADERS),
        allowed_scenario_names=allowed_scenario_names,
        scenario_name=context.scenario_name,
    )
    product_rows = _normalize_product_parameters(
        reader.read_table("Product_Parameters", PRODUCT_HEADERS),
        allowed_scenario_names=allowed_scenario_names,
        scenario_name=context.scenario_name,
    )
    yield_rows = _normalize_yield_assumptions(
        reader.read_table("Yield_Assumptions", YIELD_HEADERS),
        allowed_scenario_names=allowed_scenario_names,
        scenario_name=context.scenario_name,
    )
    packaging_rows = _normalize_packaging_and_vialing(
        reader.read_table("Packaging_and_Vialing", PACKAGING_HEADERS),
        allowed_scenario_names=allowed_scenario_names,
        scenario_name=context.scenario_name,
    )
    ss_rows = _normalize_ss_assumptions(
        reader.read_table("SS_Assumptions", SS_HEADERS),
        allowed_scenario_names=allowed_scenario_names,
        scenario_name=context.scenario_name,
    )
    cml_prevalent_rows = _normalize_cml_prevalent_assumptions(
        reader.read_table("CML_Prevalent_Assumptions", CML_PREVALENT_HEADERS),
        allowed_scenario_names=allowed_scenario_names,
        scenario_name=context.scenario_name,
    )
    trade_rows = _normalize_trade_future_hooks(
        reader.read_table("Trade_Inventory_FutureHooks", TRADE_HEADERS),
        allowed_scenario_names=allowed_scenario_names,
        scenario_name=context.scenario_name,
    )

    resolved_phase2, warnings = _resolve_phase2_config(
        context=context,
        dosing_rows=dosing_rows,
        product_rows=product_rows,
        yield_rows=yield_rows,
        packaging_rows=packaging_rows,
        ss_rows=ss_rows,
        launch_timing_rows=launch_timing_rows,
        cml_prevalent_rows=cml_prevalent_rows,
    )

    resolved_output_dir = _resolve_output_dir(
        workbook_path=workbook_path,
        output_dir=output_dir,
        scenario_name=context.scenario_name,
    )
    file_paths = _write_assumption_outputs(
        output_dir=resolved_output_dir,
        context=context,
        scenario_controls_rows=scenario_controls_rows,
        launch_timing_rows=launch_timing_rows,
        dosing_rows=dosing_rows,
        product_rows=product_rows,
        yield_rows=yield_rows,
        packaging_rows=packaging_rows,
        ss_rows=ss_rows,
        cml_prevalent_rows=cml_prevalent_rows,
        trade_rows=trade_rows,
        resolved_phase2=resolved_phase2,
        workbook_path=workbook_path.resolve(),
        warnings=warnings,
    )
    row_counts = {
        "scenario_controls": len(scenario_controls_rows),
        "launch_timing": len(launch_timing_rows),
        "dosing_assumptions": len(dosing_rows),
        "product_parameters": len(product_rows),
        "yield_assumptions": len(yield_rows),
        "packaging_and_vialing": len(packaging_rows),
        "ss_assumptions": len(ss_rows),
        "cml_prevalent_assumptions": len(cml_prevalent_rows),
        "trade_inventory_futurehooks": len(trade_rows),
    }
    return AssumptionsImportResult(
        workbook_path=workbook_path.resolve(),
        output_dir=resolved_output_dir,
        context=context,
        file_paths=file_paths,
        row_counts=row_counts,
        warnings=tuple(warnings),
    )


def _load_scenario_controls(
    reader: WorkbookReader,
    *,
    scenario_name_override: str | None,
) -> tuple[AssumptionsWorkbookContext, tuple[str, ...], list[dict[str, str]]]:
    raw_rows = reader.read_table("Scenario_Controls", SCENARIO_CONTROLS_HEADERS)
    normalized_rows: list[dict[str, str]] = []
    active_rows: list[dict[str, str]] = []
    for index, row in enumerate(raw_rows, start=2):
        if _is_blank_row(row, SCENARIO_CONTROLS_HEADERS):
            continue
        scenario_name = _require_nonempty(row, "scenario_name", "Scenario_Controls", index)
        scenario_description = _require_nonempty(row, "scenario_description", "Scenario_Controls", index)
        active_flag = _parse_boolish(row.get("active_flag", ""), "active_flag", "Scenario_Controls", index)
        forecast_grain = _require_nonempty(row, "forecast_grain", "Scenario_Controls", index)
        if forecast_grain not in SUPPORTED_FORECAST_GRAINS:
            raise ValueError(
                f"Scenario_Controls row {index} has unsupported forecast_grain {forecast_grain!r}."
            )
        forecast_frequency = _require_nonempty(row, "forecast_frequency", "Scenario_Controls", index)
        if forecast_frequency not in SUPPORTED_FORECAST_FREQUENCIES:
            raise ValueError(
                f"Scenario_Controls row {index} has unsupported forecast_frequency {forecast_frequency!r}."
            )
        dose_basis_default = _require_nonempty(row, "dose_basis_default", "Scenario_Controls", index)
        if dose_basis_default not in SUPPORTED_DOSE_BASES:
            raise ValueError(
                f"Scenario_Controls row {index} has unsupported dose_basis_default {dose_basis_default!r}."
            )
        base_currency = _require_nonempty(row, "base_currency", "Scenario_Controls", index)
        normalized = {
            "scenario_name": scenario_name,
            "scenario_description": scenario_description,
            "active_flag": _format_boolish(active_flag),
            "forecast_grain": forecast_grain,
            "forecast_frequency": forecast_frequency,
            "dose_basis_default": dose_basis_default,
            "base_currency": base_currency,
            "notes": row.get("notes", "").strip(),
        }
        normalized_rows.append(normalized)
        if active_flag:
            active_rows.append(normalized)

    if not active_rows:
        raise ValueError("Scenario_Controls requires exactly one active row, but none were provided.")
    if len(active_rows) > 1:
        raise ValueError(
            "Scenario_Controls requires exactly one active row. "
            f"Received {len(active_rows)} active rows."
        )

    active_row = active_rows[0]
    effective_scenario_name = (
        scenario_name_override.strip() if scenario_name_override and scenario_name_override.strip() else active_row["scenario_name"]
    )
    allowed_scenario_names = tuple(
        dict.fromkeys(name for name in (active_row["scenario_name"], effective_scenario_name) if name)
    )
    context = AssumptionsWorkbookContext(
        scenario_name=effective_scenario_name,
        scenario_description=active_row["scenario_description"],
        forecast_grain=active_row["forecast_grain"],
        forecast_frequency=active_row["forecast_frequency"],
        dose_basis_default=active_row["dose_basis_default"],
        base_currency=active_row["base_currency"],
        notes=active_row["notes"],
    )
    output_rows = [{**row, "scenario_name": effective_scenario_name} for row in normalized_rows]
    return context, allowed_scenario_names, output_rows


def _normalize_launch_timing(
    raw_rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    scenario_name: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    active_keys: set[tuple[str, str]] = set()
    for index, row in enumerate(raw_rows, start=2):
        if _is_blank_row(row, LAUNCH_TIMING_HEADERS):
            continue
        _validate_optional_scenario_name(row, allowed_scenario_names, "Launch_Timing", index)
        module = _require_module(row, "Launch_Timing", index)
        geography_code = _normalize_scope_value(row.get("geography_code", ""))
        approval_date = _parse_workbook_date(
            row.get("initial_approval_date", ""),
            "initial_approval_date",
            "Launch_Timing",
            index,
        )
        launch_offset_months = _parse_nonnegative_int(
            row.get("launch_offset_months", ""),
            "launch_offset_months",
            "Launch_Timing",
            index,
        )
        active_flag = _parse_boolish(row.get("active_flag", ""), "active_flag", "Launch_Timing", index)
        key = (module, geography_code)
        if active_flag and key in active_keys:
            raise ValueError(
                f"Launch_Timing row {index} duplicates active scope {key!r}. Keep launch timing unique by module and geography."
            )
        if active_flag:
            active_keys.add(key)
        rows.append(
            {
                "scenario_name": scenario_name,
                "module": module,
                "geography_code": geography_code,
                "initial_approval_date": approval_date.isoformat(),
                "launch_offset_months": str(launch_offset_months),
                "active_flag": _format_boolish(active_flag),
                "notes": row.get("notes", "").strip(),
            }
        )
    return rows


def _normalize_dosing_assumptions(
    raw_rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    scenario_name: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    active_keys: set[tuple[str, str, str]] = set()
    for index, row in enumerate(raw_rows, start=2):
        if _is_blank_row(row, DOSING_HEADERS):
            continue
        _validate_optional_scenario_name(row, allowed_scenario_names, "Dosing_Assumptions", index)
        module = _require_module(row, "Dosing_Assumptions", index)
        geography_code = _normalize_scope_value(row.get("geography_code", ""))
        segment_code = _normalize_scope_value(row.get("segment_code", ""))
        dose_basis = _require_nonempty(row, "dose_basis", "Dosing_Assumptions", index)
        if dose_basis not in SUPPORTED_DOSE_BASES:
            raise ValueError(
                f"Dosing_Assumptions row {index} has unsupported dose_basis {dose_basis!r}."
            )
        fixed_dose_mg = _parse_positive_float(row.get("fixed_dose_mg", ""), "fixed_dose_mg", "Dosing_Assumptions", index)
        weight_based_dose_mg_per_kg = _parse_positive_float(
            row.get("weight_based_dose_mg_per_kg", ""),
            "weight_based_dose_mg_per_kg",
            "Dosing_Assumptions",
            index,
        )
        average_patient_weight_kg = _parse_positive_float(
            row.get("average_patient_weight_kg", ""),
            "average_patient_weight_kg",
            "Dosing_Assumptions",
            index,
        )
        doses_per_patient_per_month = _parse_positive_float(
            row.get("doses_per_patient_per_month", ""),
            "doses_per_patient_per_month",
            "Dosing_Assumptions",
            index,
        )
        step_up_enabled = _parse_boolish(row.get("step_up_enabled", ""), "step_up_enabled", "Dosing_Assumptions", index)
        step_up_schedule_id = _require_nonempty(row, "step_up_schedule_id", "Dosing_Assumptions", index)
        dose_reduction_enabled = _parse_boolish(
            row.get("dose_reduction_enabled", ""),
            "dose_reduction_enabled",
            "Dosing_Assumptions",
            index,
        )
        dose_reduction_pct = _parse_probability(
            row.get("dose_reduction_pct", ""),
            "dose_reduction_pct",
            "Dosing_Assumptions",
            index,
        )
        adherence_rate = _parse_probability(
            row.get("adherence_rate", ""),
            "adherence_rate",
            "Dosing_Assumptions",
            index,
        )
        free_goods_pct = _parse_nonnegative_float(
            row.get("free_goods_pct", ""),
            "free_goods_pct",
            "Dosing_Assumptions",
            index,
        )
        active_flag = _parse_boolish(row.get("active_flag", ""), "active_flag", "Dosing_Assumptions", index)
        key = (module, geography_code, segment_code)
        if active_flag and key in active_keys:
            raise ValueError(
                f"Dosing_Assumptions row {index} duplicates active scope {key!r}. Keep module-specific dosing rows unique."
            )
        if active_flag:
            active_keys.add(key)
        rows.append(
            {
                "scenario_name": scenario_name,
                "module": module,
                "geography_code": geography_code,
                "segment_code": segment_code,
                "dose_basis": dose_basis,
                "fixed_dose_mg": _format_numeric(fixed_dose_mg),
                "weight_based_dose_mg_per_kg": _format_numeric(weight_based_dose_mg_per_kg),
                "average_patient_weight_kg": _format_numeric(average_patient_weight_kg),
                "doses_per_patient_per_month": _format_numeric(doses_per_patient_per_month),
                "step_up_enabled": _format_boolish(step_up_enabled),
                "step_up_schedule_id": step_up_schedule_id,
                "dose_reduction_enabled": _format_boolish(dose_reduction_enabled),
                "dose_reduction_pct": _format_numeric(dose_reduction_pct),
                "adherence_rate": _format_numeric(adherence_rate),
                "free_goods_pct": _format_numeric(free_goods_pct),
                "active_flag": _format_boolish(active_flag),
                "notes": row.get("notes", "").strip(),
            }
        )
    return rows


def _normalize_product_parameters(
    raw_rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    scenario_name: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    active_keys: set[tuple[str, str, str]] = set()
    for index, row in enumerate(raw_rows, start=2):
        if _is_blank_row(row, PRODUCT_HEADERS):
            continue
        _validate_optional_scenario_name(row, allowed_scenario_names, "Product_Parameters", index)
        parameter_scope = _require_parameter_scope(row, "Product_Parameters", index)
        module = _require_scope_module(
            row,
            parameter_scope=parameter_scope,
            sheet_name="Product_Parameters",
            row_number=index,
        )
        geography_code = _normalize_scope_value(row.get("geography_code", ""))
        ds_qty_per_dp_unit_mg = _parse_positive_float(
            row.get("ds_qty_per_dp_unit_mg", ""),
            "ds_qty_per_dp_unit_mg",
            "Product_Parameters",
            index,
        )
        dp_concentration_mg_per_ml = _parse_optional_positive_float(
            row.get("dp_concentration_mg_per_ml", ""),
            "dp_concentration_mg_per_ml",
            "Product_Parameters",
            index,
        )
        dp_fill_volume_ml = _parse_optional_positive_float(
            row.get("dp_fill_volume_ml", ""),
            "dp_fill_volume_ml",
            "Product_Parameters",
            index,
        )
        fg_mg_per_unit = _parse_positive_float(
            row.get("fg_mg_per_unit", ""),
            "fg_mg_per_unit",
            "Product_Parameters",
            index,
        )
        active_flag = _parse_boolish(row.get("active_flag", ""), "active_flag", "Product_Parameters", index)
        key = (parameter_scope, module, geography_code)
        if active_flag and key in active_keys:
            raise ValueError(
                f"Product_Parameters row {index} duplicates active scope {key!r}. Keep scoped product parameter rows unique."
            )
        if active_flag:
            active_keys.add(key)
        rows.append(
            {
                "scenario_name": scenario_name,
                "parameter_scope": parameter_scope,
                "module": module,
                "geography_code": geography_code,
                "ds_qty_per_dp_unit_mg": _format_numeric(ds_qty_per_dp_unit_mg),
                "dp_concentration_mg_per_ml": _format_optional_numeric(dp_concentration_mg_per_ml),
                "dp_fill_volume_ml": _format_optional_numeric(dp_fill_volume_ml),
                "fg_mg_per_unit": _format_numeric(fg_mg_per_unit),
                "active_flag": _format_boolish(active_flag),
                "notes": row.get("notes", "").strip(),
            }
        )
    return rows


def _normalize_yield_assumptions(
    raw_rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    scenario_name: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    active_keys: set[tuple[str, str, str]] = set()
    for index, row in enumerate(raw_rows, start=2):
        if _is_blank_row(row, YIELD_HEADERS):
            continue
        _validate_optional_scenario_name(row, allowed_scenario_names, "Yield_Assumptions", index)
        parameter_scope = _require_parameter_scope(row, "Yield_Assumptions", index)
        module = _require_scope_module(
            row,
            parameter_scope=parameter_scope,
            sheet_name="Yield_Assumptions",
            row_number=index,
        )
        geography_code = _normalize_scope_value(row.get("geography_code", ""))
        ds_to_dp_yield = _parse_probability(row.get("ds_to_dp_yield", ""), "ds_to_dp_yield", "Yield_Assumptions", index)
        dp_to_fg_yield = _parse_probability(row.get("dp_to_fg_yield", ""), "dp_to_fg_yield", "Yield_Assumptions", index)
        fg_pack_yield = _parse_probability(row.get("fg_pack_yield", ""), "fg_pack_yield", "Yield_Assumptions", index)
        ss_yield = _parse_probability(row.get("ss_yield", ""), "ss_yield", "Yield_Assumptions", index)
        ds_overage_factor = _parse_nonnegative_float(
            row.get("ds_overage_factor", ""),
            "ds_overage_factor",
            "Yield_Assumptions",
            index,
        )
        active_flag = _parse_boolish(row.get("active_flag", ""), "active_flag", "Yield_Assumptions", index)
        key = (parameter_scope, module, geography_code)
        if active_flag and key in active_keys:
            raise ValueError(
                f"Yield_Assumptions row {index} duplicates active scope {key!r}. Keep scoped yield rows unique."
            )
        if active_flag:
            active_keys.add(key)
        rows.append(
            {
                "scenario_name": scenario_name,
                "parameter_scope": parameter_scope,
                "module": module,
                "geography_code": geography_code,
                "ds_to_dp_yield": _format_numeric(ds_to_dp_yield),
                "dp_to_fg_yield": _format_numeric(dp_to_fg_yield),
                "fg_pack_yield": _format_numeric(fg_pack_yield),
                "ss_yield": _format_numeric(ss_yield),
                "ds_overage_factor": _format_numeric(ds_overage_factor),
                "active_flag": _format_boolish(active_flag),
                "notes": row.get("notes", "").strip(),
            }
        )
    return rows


def _normalize_packaging_and_vialing(
    raw_rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    scenario_name: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    active_keys: set[tuple[str, str]] = set()
    for index, row in enumerate(raw_rows, start=2):
        if _is_blank_row(row, PACKAGING_HEADERS):
            continue
        _validate_optional_scenario_name(row, allowed_scenario_names, "Packaging_and_Vialing", index)
        module = _require_module(row, "Packaging_and_Vialing", index)
        geography_code = _normalize_scope_value(row.get("geography_code", ""))
        fg_vialing_rule = _require_nonempty(row, "fg_vialing_rule", "Packaging_and_Vialing", index)
        if fg_vialing_rule not in ALLOWED_WORKBOOK_VIALING_RULES:
            raise ValueError(
                f"Packaging_and_Vialing row {index} has unsupported fg_vialing_rule {fg_vialing_rule!r}."
            )
        single_patient_use_only = _parse_boolish(
            row.get("single_patient_use_only", ""),
            "single_patient_use_only",
            "Packaging_and_Vialing",
            index,
        )
        vial_sharing_allowed = _parse_boolish(
            row.get("vial_sharing_allowed", ""),
            "vial_sharing_allowed",
            "Packaging_and_Vialing",
            index,
        )
        vials_per_carton = _parse_positive_int(
            row.get("vials_per_carton", ""),
            "vials_per_carton",
            "Packaging_and_Vialing",
            index,
        )
        partial_pack_handling = _require_nonempty(
            row,
            "partial_pack_handling",
            "Packaging_and_Vialing",
            index,
        )
        if partial_pack_handling not in ALLOWED_PARTIAL_PACK_HANDLING:
            raise ValueError(
                f"Packaging_and_Vialing row {index} has unsupported partial_pack_handling {partial_pack_handling!r}."
            )
        active_flag = _parse_boolish(row.get("active_flag", ""), "active_flag", "Packaging_and_Vialing", index)
        key = (module, geography_code)
        if active_flag and key in active_keys:
            raise ValueError(
                f"Packaging_and_Vialing row {index} duplicates active scope {key!r}. Keep packaging rows unique by module and geography."
            )
        if active_flag:
            active_keys.add(key)
        rows.append(
            {
                "scenario_name": scenario_name,
                "module": module,
                "geography_code": geography_code,
                "fg_vialing_rule": fg_vialing_rule,
                "single_patient_use_only": _format_boolish(single_patient_use_only),
                "vial_sharing_allowed": _format_boolish(vial_sharing_allowed),
                "vials_per_carton": str(vials_per_carton),
                "partial_pack_handling": partial_pack_handling,
                "active_flag": _format_boolish(active_flag),
                "notes": row.get("notes", "").strip(),
            }
        )
    return rows


def _normalize_ss_assumptions(
    raw_rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    scenario_name: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    active_keys: set[tuple[str, str]] = set()
    for index, row in enumerate(raw_rows, start=2):
        if _is_blank_row(row, SS_HEADERS):
            continue
        _validate_optional_scenario_name(row, allowed_scenario_names, "SS_Assumptions", index)
        module = _require_module_or_all(row, "SS_Assumptions", index)
        geography_code = _normalize_scope_value(row.get("geography_code", ""))
        ss_ratio_to_fg = _parse_positive_float(
            row.get("ss_ratio_to_fg", ""),
            "ss_ratio_to_fg",
            "SS_Assumptions",
            index,
        )
        co_pack_mode = _require_nonempty(row, "co_pack_mode", "SS_Assumptions", index)
        if co_pack_mode not in SUPPORTED_CO_PACK_MODES:
            raise ValueError(
                f"SS_Assumptions row {index} has unsupported co_pack_mode {co_pack_mode!r}."
            )
        active_flag = _parse_boolish(row.get("active_flag", ""), "active_flag", "SS_Assumptions", index)
        key = (module, geography_code)
        if active_flag and key in active_keys:
            raise ValueError(
                f"SS_Assumptions row {index} duplicates active scope {key!r}. Keep SS assumption rows unique."
            )
        if active_flag:
            active_keys.add(key)
        rows.append(
            {
                "scenario_name": scenario_name,
                "module": module,
                "geography_code": geography_code,
                "ss_ratio_to_fg": _format_numeric(ss_ratio_to_fg),
                "co_pack_mode": co_pack_mode,
                "active_flag": _format_boolish(active_flag),
                "notes": row.get("notes", "").strip(),
            }
        )
    return rows


def _normalize_cml_prevalent_assumptions(
    raw_rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    scenario_name: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    active_keys: set[str] = set()
    for index, row in enumerate(raw_rows, start=2):
        if _is_blank_row(row, CML_PREVALENT_HEADERS):
            continue
        _validate_optional_scenario_name(row, allowed_scenario_names, "CML_Prevalent_Assumptions", index)
        geography_code = _normalize_scope_value(_require_nonempty(row, "geography_code", "CML_Prevalent_Assumptions", index))
        active_flag = _parse_boolish(
            row.get("active_flag", ""),
            "active_flag",
            "CML_Prevalent_Assumptions",
            index,
        )
        if active_flag:
            addressable_prevalent_pool = _parse_positive_float(
                row.get("addressable_prevalent_pool", ""),
                "addressable_prevalent_pool",
                "CML_Prevalent_Assumptions",
                index,
            )
            launch_year_index = _parse_positive_int(
                row.get("launch_year_index", ""),
                "launch_year_index",
                "CML_Prevalent_Assumptions",
                index,
            )
            launch_month_index = _parse_positive_int(
                row.get("launch_month_index", ""),
                "launch_month_index",
                "CML_Prevalent_Assumptions",
                index,
            )
            duration_months = _parse_positive_int(
                row.get("duration_months", ""),
                "duration_months",
                "CML_Prevalent_Assumptions",
                index,
            )
            curve_profile_id = _require_nonempty(
                row,
                "curve_profile_id",
                "CML_Prevalent_Assumptions",
                index,
            )
            bolus_start_year = _parse_positive_int(
                row.get("bolus_start_year", ""),
                "bolus_start_year",
                "CML_Prevalent_Assumptions",
                index,
            )
            bolus_end_year = _parse_positive_int(
                row.get("bolus_end_year", ""),
                "bolus_end_year",
                "CML_Prevalent_Assumptions",
                index,
            )
            exhaustion_year = _parse_positive_int(
                row.get("exhaustion_year", ""),
                "exhaustion_year",
                "CML_Prevalent_Assumptions",
                index,
            )
            exhaustion_rule = _require_nonempty(
                row,
                "exhaustion_rule",
                "CML_Prevalent_Assumptions",
                index,
            )
            if exhaustion_rule not in ALLOWED_EXHAUSTION_RULES:
                raise ValueError(
                    f"CML_Prevalent_Assumptions row {index} has unsupported exhaustion_rule {exhaustion_rule!r}."
                )
            source = _require_nonempty(row, "source", "CML_Prevalent_Assumptions", index)
            if geography_code in active_keys:
                raise ValueError(
                    f"CML_Prevalent_Assumptions row {index} duplicates active geography {geography_code!r}."
                )
            active_keys.add(geography_code)
        else:
            addressable_prevalent_pool = _parse_optional_positive_float(
                row.get("addressable_prevalent_pool", ""),
                "addressable_prevalent_pool",
                "CML_Prevalent_Assumptions",
                index,
            )
            launch_year_index = _parse_optional_positive_int(
                row.get("launch_year_index", ""),
                "launch_year_index",
                "CML_Prevalent_Assumptions",
                index,
            )
            launch_month_index = _parse_optional_positive_int(
                row.get("launch_month_index", ""),
                "launch_month_index",
                "CML_Prevalent_Assumptions",
                index,
            )
            duration_months = _parse_optional_positive_int(
                row.get("duration_months", ""),
                "duration_months",
                "CML_Prevalent_Assumptions",
                index,
            )
            curve_profile_id = row.get("curve_profile_id", "").strip()
            bolus_start_year = _parse_optional_positive_int(
                row.get("bolus_start_year", ""),
                "bolus_start_year",
                "CML_Prevalent_Assumptions",
                index,
            )
            bolus_end_year = _parse_optional_positive_int(
                row.get("bolus_end_year", ""),
                "bolus_end_year",
                "CML_Prevalent_Assumptions",
                index,
            )
            exhaustion_year = _parse_optional_positive_int(
                row.get("exhaustion_year", ""),
                "exhaustion_year",
                "CML_Prevalent_Assumptions",
                index,
            )
            exhaustion_rule = row.get("exhaustion_rule", "").strip()
            if exhaustion_rule and exhaustion_rule not in ALLOWED_EXHAUSTION_RULES:
                raise ValueError(
                    f"CML_Prevalent_Assumptions row {index} has unsupported exhaustion_rule {exhaustion_rule!r}."
                )
            source = row.get("source", "").strip()
        rows.append(
            {
                "scenario_name": scenario_name,
                "geography_code": geography_code,
                "addressable_prevalent_pool": _format_optional_numeric(addressable_prevalent_pool),
                "launch_year_index": _format_optional_int(launch_year_index),
                "launch_month_index": _format_optional_int(launch_month_index),
                "duration_months": _format_optional_int(duration_months),
                "curve_profile_id": curve_profile_id,
                "bolus_start_year": _format_optional_int(bolus_start_year),
                "bolus_end_year": _format_optional_int(bolus_end_year),
                "exhaustion_year": _format_optional_int(exhaustion_year),
                "exhaustion_rule": exhaustion_rule,
                "source": source,
                "active_flag": _format_boolish(active_flag),
                "notes": row.get("notes", "").strip(),
            }
        )
    return rows


def _normalize_trade_future_hooks(
    raw_rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    scenario_name: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for index, row in enumerate(raw_rows, start=2):
        if _is_blank_row(row, TRADE_HEADERS):
            continue
        _validate_optional_scenario_name(row, allowed_scenario_names, "Trade_Inventory_FutureHooks", index)
        module = _require_module_or_all(row, "Trade_Inventory_FutureHooks", index)
        geography_code = _normalize_scope_value(row.get("geography_code", ""))
        active_flag = _parse_boolish(row.get("active_flag", ""), "active_flag", "Trade_Inventory_FutureHooks", index)
        rows.append(
            {
                "scenario_name": scenario_name,
                "module": module,
                "geography_code": geography_code,
                "trade_rule_placeholder": row.get("trade_rule_placeholder", "").strip(),
                "inventory_rule_placeholder": row.get("inventory_rule_placeholder", "").strip(),
                "active_flag": _format_boolish(active_flag),
                "notes": row.get("notes", "").strip(),
            }
        )
    return rows


def _resolve_phase2_config(
    *,
    context: AssumptionsWorkbookContext,
    dosing_rows: list[dict[str, str]],
    product_rows: list[dict[str, str]],
    yield_rows: list[dict[str, str]],
    packaging_rows: list[dict[str, str]],
    ss_rows: list[dict[str, str]],
    launch_timing_rows: list[dict[str, str]],
    cml_prevalent_rows: list[dict[str, str]],
) -> tuple[dict[str, object], list[str]]:
    warnings: list[str] = []
    active_dosing_rows = _active_rows(dosing_rows)
    active_product_rows = _active_rows(product_rows)
    active_yield_rows = _active_rows(yield_rows)
    active_packaging_rows = _active_rows(packaging_rows)
    active_ss_rows = _active_rows(ss_rows)

    resolved_module_settings: dict[str, dict[str, object]] = {}
    ignored_dosing_specific_rows = [
        row for row in active_dosing_rows if row["geography_code"] != "ALL" or row["segment_code"] != "ALL"
    ]
    if ignored_dosing_specific_rows:
        warnings.append(
            "Geography-specific or segment-specific Dosing_Assumptions rows are preserved in normalized artifacts, but the current Phase 2 engine still consumes module-level ALL/ALL rows only."
        )

    for module in PHASE1_MODULES:
        dosing_row = _find_active_row(
            active_dosing_rows,
            sheet_name="Dosing_Assumptions",
            required_fields={"module": module, "geography_code": "ALL", "segment_code": "ALL"},
            error_hint="Provide one active ALL geography / ALL segment row per module.",
        )
        packaging_row = _find_active_row(
            active_packaging_rows,
            sheet_name="Packaging_and_Vialing",
            required_fields={"module": module, "geography_code": "ALL"},
            error_hint="Provide one active packaging row per module with geography_code = ALL.",
        )
        fg_mg_per_unit = _resolve_product_value_for_module(
            active_product_rows,
            module=module,
            field_name="fg_mg_per_unit",
        )
        resolved_module_settings[module] = {
            "fixed_dose_mg": float(dosing_row["fixed_dose_mg"]),
            "weight_based_dose_mg_per_kg": float(dosing_row["weight_based_dose_mg_per_kg"]),
            "average_patient_weight_kg": float(dosing_row["average_patient_weight_kg"]),
            "patient_weight_distribution": "PLACEHOLDER_DETERMINISTIC_AVERAGE_ONLY",
            "doses_per_patient_per_month": float(dosing_row["doses_per_patient_per_month"]),
            "fg_vialing_rule": _map_workbook_vialing_rule(packaging_row["fg_vialing_rule"]),
            "fg_mg_per_unit": fg_mg_per_unit,
        }
        if dosing_row["dose_basis"] != context.dose_basis_default:
            warnings.append(
                f"Dosing_Assumptions active {module} row uses dose_basis={dosing_row['dose_basis']!r}, but the current engine still consumes Scenario_Controls.dose_basis_default={context.dose_basis_default!r} as a global setting."
            )

    step_up_enabled = _resolve_consistent_dosing_value(
        active_dosing_rows,
        field_name="step_up_enabled",
        cast=_parse_stored_bool,
        error_label="step_up_enabled",
    )
    step_up_schedule_id = _resolve_consistent_dosing_value(
        active_dosing_rows,
        field_name="step_up_schedule_id",
        cast=str,
        error_label="step_up_schedule_id",
    )
    dose_reduction_enabled = _resolve_consistent_dosing_value(
        active_dosing_rows,
        field_name="dose_reduction_enabled",
        cast=_parse_stored_bool,
        error_label="dose_reduction_enabled",
    )
    dose_reduction_pct = _resolve_consistent_dosing_value(
        active_dosing_rows,
        field_name="dose_reduction_pct",
        cast=float,
        error_label="dose_reduction_pct",
    )
    adherence_rate = _resolve_consistent_dosing_value(
        active_dosing_rows,
        field_name="adherence_rate",
        cast=float,
        error_label="adherence_rate",
    )
    free_goods_pct = _resolve_consistent_dosing_value(
        active_dosing_rows,
        field_name="free_goods_pct",
        cast=float,
        error_label="free_goods_pct",
    )

    yield_default_row = _find_active_row(
        active_yield_rows,
        sheet_name="Yield_Assumptions",
        required_fields={"parameter_scope": "scenario_default", "module": "ALL", "geography_code": "ALL"},
        error_hint="Provide one active scenario_default yield row with module = ALL and geography_code = ALL.",
    )
    product_default_row = _find_active_row(
        active_product_rows,
        sheet_name="Product_Parameters",
        required_fields={"parameter_scope": "scenario_default", "module": "ALL", "geography_code": "ALL"},
        error_hint="Provide one active scenario_default product row with module = ALL and geography_code = ALL.",
    )
    ss_default_row = _resolve_ss_default_row(active_ss_rows)

    if any(
        row["parameter_scope"] == "module_override" and row["active_flag"] == "true"
        for row in active_product_rows
    ):
        warnings.append(
            "Product_Parameters module_override rows are preserved in normalized artifacts. Current Phase 2 wiring applies fg_mg_per_unit overrides by module, but ds_qty_per_dp_unit_mg still uses the scenario_default row only."
        )
    if any(
        row["parameter_scope"] == "module_override" and row["active_flag"] == "true"
        for row in active_yield_rows
    ):
        warnings.append(
            "Yield_Assumptions module_override rows are preserved in normalized artifacts. Current Phase 2 wiring still uses the scenario_default row for ds_overage_factor and plan yields."
        )
    if any(row["active_flag"] == "true" and row["geography_code"] != "ALL" for row in active_product_rows + active_yield_rows + active_packaging_rows + active_ss_rows):
        warnings.append(
            "Geography-specific assumption rows are preserved in normalized artifacts, but the current generated Phase 2 config only consumes ALL-geography rows."
        )
    if any(row["active_flag"] == "true" for row in launch_timing_rows):
        warnings.append(
            "Launch_Timing rows are normalized and preserved for future integration, but current Phase 2 config generation does not wire launch timing directly."
        )
    if any(row["active_flag"] == "true" for row in cml_prevalent_rows):
        warnings.append(
            "CML_Prevalent_Assumptions rows are normalized and preserved, but current Phase 2 config generation does not consume them directly."
        )

    phase2_config = {
        "model": {
            "phase": 2,
            "build_scope": PHASE2_BUILD_SCOPE,
            "upstream_demand_contract": PHASE2_UPSTREAM_DEMAND_CONTRACT,
            "dose_basis": context.dose_basis_default,
            "co_pack_mode": ss_default_row["co_pack_mode"],
        },
        "modules": {
            "enabled": list(PHASE1_MODULES),
            "disabled": list(PHASE1_DISABLED_CAPABILITIES),
        },
        "module_settings": resolved_module_settings,
        "step_up": {
            "enabled": step_up_enabled,
            "schedule_id": step_up_schedule_id,
        },
        "dose_reduction": {
            "enabled": dose_reduction_enabled,
            "pct": dose_reduction_pct,
        },
        "commercial_adjustments": {
            "adherence_rate": adherence_rate,
            "free_goods_pct": free_goods_pct,
        },
        "yield": {
            "plan": {
                "ds_to_dp": float(yield_default_row["ds_to_dp_yield"]),
                "dp_to_fg": float(yield_default_row["dp_to_fg_yield"]),
                "fg_pack": float(yield_default_row["fg_pack_yield"]),
                "ss": float(yield_default_row["ss_yield"]),
            }
        },
        "ds": {
            "qty_per_dp_unit_mg": float(product_default_row["ds_qty_per_dp_unit_mg"]),
            "overage_factor": float(yield_default_row["ds_overage_factor"]),
        },
        "ss": {
            "ratio_to_fg": float(ss_default_row["ss_ratio_to_fg"]),
        },
        "validation": {
            "enforce_unique_output_keys": True,
        },
        "wiring_notes": [
            "dose_basis_default is wired into model.dose_basis as a scenario-level setting.",
            "fg_mg_per_unit uses module_override rows when present, otherwise the scenario_default row.",
            "ds_qty_per_dp_unit_mg and ds_overage_factor preserve module overrides in CSV artifacts, but the current engine still consumes the scenario_default row only.",
            "patient_weight_distribution remains a clearly labeled placeholder because the assumptions workbook currently captures deterministic average weight only.",
        ],
    }
    return phase2_config, warnings


def _write_assumption_outputs(
    *,
    output_dir: Path,
    context: AssumptionsWorkbookContext,
    scenario_controls_rows: list[dict[str, str]],
    launch_timing_rows: list[dict[str, str]],
    dosing_rows: list[dict[str, str]],
    product_rows: list[dict[str, str]],
    yield_rows: list[dict[str, str]],
    packaging_rows: list[dict[str, str]],
    ss_rows: list[dict[str, str]],
    cml_prevalent_rows: list[dict[str, str]],
    trade_rows: list[dict[str, str]],
    resolved_phase2: dict[str, object],
    workbook_path: Path,
    warnings: list[str],
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_paths = {
        "scenario_controls": output_dir / "scenario_controls.csv",
        "launch_timing": output_dir / "launch_timing.csv",
        "dosing_assumptions": output_dir / "dosing_assumptions.csv",
        "product_parameters": output_dir / "product_parameters.csv",
        "yield_assumptions": output_dir / "yield_assumptions.csv",
        "packaging_and_vialing": output_dir / "packaging_and_vialing.csv",
        "ss_assumptions": output_dir / "ss_assumptions.csv",
        "cml_prevalent_assumptions": output_dir / "cml_prevalent_assumptions.csv",
        "trade_inventory_futurehooks": output_dir / "trade_inventory_futurehooks.csv",
        "resolved_phase2_snapshot": output_dir / "resolved_phase2_config_snapshot.json",
        "import_summary": output_dir / "assumptions_import_summary.json",
        "generated_phase2_parameters": output_dir / "generated_phase2_parameters.toml",
        "generated_phase2_scenario": output_dir / "generated_phase2_scenario.toml",
    }
    _write_csv(file_paths["scenario_controls"], SCENARIO_CONTROLS_HEADERS, scenario_controls_rows)
    _write_csv(file_paths["launch_timing"], LAUNCH_TIMING_HEADERS, launch_timing_rows)
    _write_csv(file_paths["dosing_assumptions"], DOSING_HEADERS, dosing_rows)
    _write_csv(file_paths["product_parameters"], PRODUCT_HEADERS, product_rows)
    _write_csv(file_paths["yield_assumptions"], YIELD_HEADERS, yield_rows)
    _write_csv(file_paths["packaging_and_vialing"], PACKAGING_HEADERS, packaging_rows)
    _write_csv(file_paths["ss_assumptions"], SS_HEADERS, ss_rows)
    _write_csv(file_paths["cml_prevalent_assumptions"], CML_PREVALENT_HEADERS, cml_prevalent_rows)
    _write_csv(file_paths["trade_inventory_futurehooks"], TRADE_HEADERS, trade_rows)

    file_paths["resolved_phase2_snapshot"].write_text(
        json.dumps(
            {
                "scenario_name": context.scenario_name,
                "resolved_phase2": resolved_phase2,
                "warnings": warnings,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    _write_generated_phase2_parameters(
        file_paths["generated_phase2_parameters"],
        resolved_phase2,
    )
    _write_generated_phase2_scenario(
        output_dir=output_dir,
        scenario_name=context.scenario_name,
        parameter_config_path=file_paths["generated_phase2_parameters"],
        scenario_output_path=file_paths["generated_phase2_scenario"],
    )

    summary_payload = {
        "workbook_path": str(workbook_path),
        "output_dir": str(output_dir),
        "scenario_name": context.scenario_name,
        "forecast_grain": context.forecast_grain,
        "forecast_frequency": context.forecast_frequency,
        "dose_basis_default": context.dose_basis_default,
        "generated_files": {name: str(path) for name, path in file_paths.items()},
        "row_counts": {
            "scenario_controls": len(scenario_controls_rows),
            "launch_timing": len(launch_timing_rows),
            "dosing_assumptions": len(dosing_rows),
            "product_parameters": len(product_rows),
            "yield_assumptions": len(yield_rows),
            "packaging_and_vialing": len(packaging_rows),
            "ss_assumptions": len(ss_rows),
            "cml_prevalent_assumptions": len(cml_prevalent_rows),
            "trade_inventory_futurehooks": len(trade_rows),
        },
        "wired_into_current_engine": [
            "Scenario_Controls.dose_basis_default -> model.dose_basis",
            "Dosing_Assumptions module rows -> module_settings.<module> fixed_dose_mg / weight_based_dose_mg_per_kg / average_patient_weight_kg / doses_per_patient_per_month",
            "Product_Parameters scenario_default + module_override -> fg_mg_per_unit resolution",
            "Packaging_and_Vialing module rows -> module_settings.<module>.fg_vialing_rule",
            "Yield_Assumptions scenario_default row -> yield.plan.* and ds.overage_factor",
            "Product_Parameters scenario_default row -> ds.qty_per_dp_unit_mg",
            "SS_Assumptions scenario default row -> ss.ratio_to_fg and model.co_pack_mode",
        ],
        "future_ready_only": [
            "Launch_Timing normalized only; not yet wired into active engine logic.",
            "CML_Prevalent_Assumptions normalized only; not yet wired into active Phase 2 config generation.",
            "Trade_Inventory_FutureHooks normalized only; future-phase placeholder.",
            "Product_Parameters module_override ds_qty_per_dp_unit_mg rows are preserved but not yet consumed by the current engine.",
            "Yield_Assumptions module_override ds_overage_factor rows are preserved but not yet consumed by the current engine.",
            "dp_concentration_mg_per_ml and dp_fill_volume_ml are preserved but not yet consumed by the current engine.",
        ],
        "warnings": warnings,
    }
    file_paths["import_summary"].write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    return file_paths


def _write_generated_phase2_parameters(path: Path, resolved_phase2: dict[str, object]) -> None:
    model = resolved_phase2["model"]
    modules = resolved_phase2["modules"]
    module_settings = resolved_phase2["module_settings"]
    step_up = resolved_phase2["step_up"]
    dose_reduction = resolved_phase2["dose_reduction"]
    commercial_adjustments = resolved_phase2["commercial_adjustments"]
    plan_yield = resolved_phase2["yield"]["plan"]
    ds = resolved_phase2["ds"]
    ss = resolved_phase2["ss"]
    validation = resolved_phase2["validation"]

    lines = [
        "# GENERATED BY scripts/assumptions_import.py",
        "# Business-facing assumptions workbook bridge into the current Phase 2 config path.",
        "",
        "[model]",
        f'phase = {model["phase"]}',
        f'build_scope = "{model["build_scope"]}"',
        f'upstream_demand_contract = "{model["upstream_demand_contract"]}"',
        f'dose_basis = "{model["dose_basis"]}"',
        f'co_pack_mode = "{model["co_pack_mode"]}"',
        "",
        "[modules]",
        f'enabled = {_toml_list(modules["enabled"])}',
        f'disabled = {_toml_list(modules["disabled"])}',
        "",
    ]
    for module in PHASE1_MODULES:
        module_values = module_settings[module]
        lines.extend(
            [
                f"[module_settings.{module}]",
                f'fixed_dose_mg = {module_values["fixed_dose_mg"]}',
                f'weight_based_dose_mg_per_kg = {module_values["weight_based_dose_mg_per_kg"]}',
                f'average_patient_weight_kg = {module_values["average_patient_weight_kg"]}',
                f'patient_weight_distribution = "{module_values["patient_weight_distribution"]}"',
                f'doses_per_patient_per_month = {module_values["doses_per_patient_per_month"]}',
                f'fg_vialing_rule = "{module_values["fg_vialing_rule"]}"',
                f'fg_mg_per_unit = {module_values["fg_mg_per_unit"]}',
                "",
            ]
        )
    lines.extend(
        [
            "[step_up]",
            f'enabled = {_toml_bool(step_up["enabled"])}',
            f'schedule_id = "{step_up["schedule_id"]}"',
            "",
            "[dose_reduction]",
            f'enabled = {_toml_bool(dose_reduction["enabled"])}',
            f'pct = {dose_reduction["pct"]}',
            "",
            "[commercial_adjustments]",
            f'adherence_rate = {commercial_adjustments["adherence_rate"]}',
            f'free_goods_pct = {commercial_adjustments["free_goods_pct"]}',
            "",
            "[yield.plan]",
            f'ds_to_dp = {plan_yield["ds_to_dp"]}',
            f'dp_to_fg = {plan_yield["dp_to_fg"]}',
            f'fg_pack = {plan_yield["fg_pack"]}',
            f'ss = {plan_yield["ss"]}',
            "",
            "[ds]",
            f'qty_per_dp_unit_mg = {ds["qty_per_dp_unit_mg"]}',
            f'overage_factor = {ds["overage_factor"]}',
            "",
            "[ss]",
            f'ratio_to_fg = {ss["ratio_to_fg"]}',
            "",
            "[validation]",
            f'enforce_unique_output_keys = {_toml_bool(validation["enforce_unique_output_keys"])}',
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_generated_phase2_scenario(
    *,
    output_dir: Path,
    scenario_name: str,
    parameter_config_path: Path,
    scenario_output_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    default_phase1_output = repo_root / "data" / "curated" / _slugify(scenario_name) / "monthlyized_output.csv"
    deterministic_cascade_output = output_dir.parent / "phase2_deterministic_cascade.csv"
    parameter_ref = _relative_path_for_toml(parameter_config_path, start=output_dir)
    input_ref = _relative_path_for_toml(default_phase1_output, start=output_dir)
    output_ref = _relative_path_for_toml(deterministic_cascade_output, start=output_dir)
    scenario_output_path.write_text(
        "\n".join(
            [
                "# GENERATED BY scripts/assumptions_import.py",
                "# Use directly with scripts/run_phase2.py or pass as --phase2-scenario to scripts/run_forecast_workflow.py.",
                f'scenario_name = "{scenario_name}"',
                f'parameter_config = "{parameter_ref}"',
                "",
                "[inputs]",
                f'phase1_monthlyized_output = "{input_ref}"',
                "",
                "[outputs]",
                f'deterministic_cascade = "{output_ref}"',
                "",
            ]
        ),
        encoding="utf-8",
    )


def _resolve_output_dir(
    *,
    workbook_path: Path,
    output_dir: Path | None,
    scenario_name: str,
) -> Path:
    if output_dir is not None:
        return output_dir.resolve()
    repo_root = Path(__file__).resolve().parents[3]
    return (repo_root / "data" / "outputs" / _slugify(scenario_name) / "assumptions").resolve()


def _active_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [row for row in rows if row.get("active_flag") == "true"]


def _find_active_row(
    rows: list[dict[str, str]],
    *,
    sheet_name: str,
    required_fields: dict[str, str],
    error_hint: str,
) -> dict[str, str]:
    matches = [
        row
        for row in rows
        if all(row.get(field_name, "") == expected_value for field_name, expected_value in required_fields.items())
    ]
    if not matches:
        raise ValueError(
            f"{sheet_name} is missing a required active row for scope {required_fields!r}. {error_hint}"
        )
    if len(matches) > 1:
        raise ValueError(
            f"{sheet_name} has ambiguous active rows for scope {required_fields!r}. Keep active assumption scopes unique."
        )
    return matches[0]


def _resolve_product_value_for_module(
    active_rows: list[dict[str, str]],
    *,
    module: str,
    field_name: str,
) -> float:
    override_rows = [
        row
        for row in active_rows
        if row["parameter_scope"] == "module_override"
        and row["module"] == module
        and row["geography_code"] == "ALL"
    ]
    if len(override_rows) > 1:
        raise ValueError(
            f"Product_Parameters has ambiguous active module_override rows for module {module!r}."
        )
    if override_rows:
        return float(override_rows[0][field_name])
    default_row = _find_active_row(
        active_rows,
        sheet_name="Product_Parameters",
        required_fields={"parameter_scope": "scenario_default", "module": "ALL", "geography_code": "ALL"},
        error_hint="Provide an active scenario_default product row to supply fallback values.",
    )
    return float(default_row[field_name])


def _resolve_ss_default_row(active_rows: list[dict[str, str]]) -> dict[str, str]:
    default_rows = [
        row for row in active_rows if row["module"] == "ALL" and row["geography_code"] == "ALL"
    ]
    if len(default_rows) == 1:
        return default_rows[0]
    if len(default_rows) > 1:
        raise ValueError(
            "SS_Assumptions has ambiguous ALL/ALL active rows. Keep the scenario default SS row unique."
        )
    if not active_rows:
        raise ValueError("SS_Assumptions requires at least one active row.")
    ratios = {row["ss_ratio_to_fg"] for row in active_rows}
    co_pack_modes = {row["co_pack_mode"] for row in active_rows}
    if len(ratios) == 1 and len(co_pack_modes) == 1:
        return active_rows[0]
    raise ValueError(
        "SS_Assumptions currently feeds a global engine setting. Provide an ALL/ALL active row or keep all active rows consistent."
    )


def _resolve_consistent_dosing_value(
    active_rows: list[dict[str, str]],
    *,
    field_name: str,
    cast,
    error_label: str,
):
    relevant_rows = [
        row
        for row in active_rows
        if row["geography_code"] == "ALL" and row["segment_code"] == "ALL"
    ]
    if not relevant_rows:
        raise ValueError(
            f"Dosing_Assumptions must include active ALL/ALL module rows before resolving {error_label}."
        )
    values = {cast(row[field_name]) for row in relevant_rows}
    if len(values) != 1:
        raise ValueError(
            f"Dosing_Assumptions currently feeds a global engine setting for {error_label}. Keep active module rows consistent."
        )
    return values.pop()


def _map_workbook_vialing_rule(value: str) -> str:
    mapped = ENGINE_VIALING_RULE_BY_WORKBOOK_VALUE.get(value)
    if mapped is None or mapped not in SUPPORTED_FG_VIALING_RULES:
        raise ValueError(f"Unsupported workbook vialing rule mapping for {value!r}.")
    return mapped


def _require_module(row: dict[str, str], sheet_name: str, row_number: int) -> str:
    module = _require_nonempty(row, "module", sheet_name, row_number)
    if module not in PHASE1_MODULES:
        raise ValueError(f"{sheet_name} row {row_number} has unsupported module {module!r}.")
    return module


def _require_module_or_all(row: dict[str, str], sheet_name: str, row_number: int) -> str:
    module = _require_nonempty(row, "module", sheet_name, row_number)
    if module not in PHASE1_MODULES and module != "ALL":
        raise ValueError(f"{sheet_name} row {row_number} has unsupported module {module!r}.")
    return module


def _require_parameter_scope(row: dict[str, str], sheet_name: str, row_number: int) -> str:
    parameter_scope = _require_nonempty(row, "parameter_scope", sheet_name, row_number)
    if parameter_scope not in ALLOWED_PARAMETER_SCOPES:
        raise ValueError(
            f"{sheet_name} row {row_number} has unsupported parameter_scope {parameter_scope!r}."
        )
    return parameter_scope


def _require_scope_module(
    row: dict[str, str],
    *,
    parameter_scope: str,
    sheet_name: str,
    row_number: int,
) -> str:
    module = _require_nonempty(row, "module", sheet_name, row_number)
    if parameter_scope == "scenario_default" and module != "ALL":
        raise ValueError(
            f"{sheet_name} row {row_number} must use module='ALL' for parameter_scope='scenario_default'."
        )
    if parameter_scope == "module_override" and module not in PHASE1_MODULES:
        raise ValueError(
            f"{sheet_name} row {row_number} must use a supported module for parameter_scope='module_override'."
        )
    return module


def _validate_optional_scenario_name(
    row: dict[str, str],
    allowed_scenario_names: tuple[str, ...],
    sheet_name: str,
    row_number: int,
) -> None:
    provided = row.get("scenario_name", "").strip()
    if not provided or provided in allowed_scenario_names:
        return
    raise ValueError(
        f"{sheet_name} row {row_number} has scenario_name {provided!r}, expected one of {allowed_scenario_names!r}."
    )


def _require_nonempty(
    row: dict[str, str],
    field_name: str,
    sheet_name: str,
    row_number: int,
) -> str:
    value = row.get(field_name, "").strip()
    if not value:
        raise ValueError(f"{sheet_name} row {row_number} is missing required value for {field_name!r}.")
    return value


def _parse_boolish(value: str, field_name: str, sheet_name: str, row_number: int) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "yes", "1"}:
        return True
    if normalized in {"false", "no", "0"}:
        return False
    raise ValueError(f"{sheet_name} row {row_number} has invalid boolean-like value for {field_name!r}: {value!r}.")


def _parse_stored_bool(value: str) -> bool:
    return value == "true"


def _parse_positive_float(value: str, field_name: str, sheet_name: str, row_number: int) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(
            f"{sheet_name} row {row_number} has non-numeric {field_name!r}: {value!r}."
        ) from exc
    if parsed <= 0:
        raise ValueError(f"{sheet_name} row {row_number} requires {field_name!r} > 0, received {parsed}.")
    return parsed


def _parse_optional_positive_float(value: str, field_name: str, sheet_name: str, row_number: int) -> float | None:
    if not value.strip():
        return None
    return _parse_positive_float(value, field_name, sheet_name, row_number)


def _parse_nonnegative_float(value: str, field_name: str, sheet_name: str, row_number: int) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(
            f"{sheet_name} row {row_number} has non-numeric {field_name!r}: {value!r}."
        ) from exc
    if parsed < 0:
        raise ValueError(f"{sheet_name} row {row_number} requires {field_name!r} >= 0, received {parsed}.")
    return parsed


def _parse_probability(value: str, field_name: str, sheet_name: str, row_number: int) -> float:
    parsed = _parse_nonnegative_float(value, field_name, sheet_name, row_number)
    if parsed > 1:
        raise ValueError(
            f"{sheet_name} row {row_number} requires {field_name!r} between 0 and 1, received {parsed}."
        )
    return parsed


def _parse_positive_int(value: str, field_name: str, sheet_name: str, row_number: int) -> int:
    try:
        parsed = int(float(value))
    except ValueError as exc:
        raise ValueError(
            f"{sheet_name} row {row_number} has non-integer {field_name!r}: {value!r}."
        ) from exc
    if parsed <= 0:
        raise ValueError(f"{sheet_name} row {row_number} requires {field_name!r} > 0, received {parsed}.")
    return parsed


def _parse_optional_positive_int(value: str, field_name: str, sheet_name: str, row_number: int) -> int | None:
    if not value.strip():
        return None
    return _parse_positive_int(value, field_name, sheet_name, row_number)


def _parse_nonnegative_int(value: str, field_name: str, sheet_name: str, row_number: int) -> int:
    try:
        parsed = int(float(value))
    except ValueError as exc:
        raise ValueError(
            f"{sheet_name} row {row_number} has non-integer {field_name!r}: {value!r}."
        ) from exc
    if parsed < 0:
        raise ValueError(f"{sheet_name} row {row_number} requires {field_name!r} >= 0, received {parsed}.")
    return parsed


def _parse_workbook_date(value: str, field_name: str, sheet_name: str, row_number: int) -> date:
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{sheet_name} row {row_number} is missing required value for {field_name!r}.")
    try:
        serial = float(stripped)
    except ValueError:
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(stripped, fmt).date()
            except ValueError:
                continue
        raise ValueError(
            f"{sheet_name} row {row_number} has invalid date for {field_name!r}: {value!r}."
        )
    return EXCEL_EPOCH + timedelta(days=int(round(serial)))


def _normalize_scope_value(value: str) -> str:
    stripped = value.strip()
    return stripped if stripped else "ALL"


def _format_boolish(value: bool) -> str:
    return "true" if value else "false"


def _format_numeric(value: float) -> str:
    return format(value, ".15g")


def _format_optional_numeric(value: float | None) -> str:
    return "" if value is None else _format_numeric(value)


def _format_optional_int(value: int | None) -> str:
    return "" if value is None else str(value)


def _is_blank_row(row: dict[str, str], headers: tuple[str, ...]) -> bool:
    return all(not row.get(header, "").strip() for header in headers)


def _write_csv(path: Path, fieldnames: tuple[str, ...], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _relative_path_for_toml(path: Path, *, start: Path) -> str:
    return Path(os.path.relpath(path, start=start)).as_posix()


def _toml_bool(value: bool) -> str:
    return "true" if value else "false"


def _toml_list(values: list[str]) -> str:
    return "[" + ", ".join(f'"{value}"' for value in values) + "]"


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", value.strip()).strip("_").lower()
    return normalized or "assumptions"
