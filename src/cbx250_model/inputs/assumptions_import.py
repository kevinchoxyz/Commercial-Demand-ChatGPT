"""Import the business-facing model assumptions workbook into normalized artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import csv
import json
import os
import re
import tomllib

from ..constants import (
    MODULE_TO_SEGMENTS,
    PHASE1_DISABLED_CAPABILITIES,
    PHASE1_MODULES,
    PHASE2_BUILD_SCOPE,
    PHASE2_UPSTREAM_DEMAND_CONTRACT,
    PHASE4_BUILD_SCOPE,
    PHASE4_DISABLED_CAPABILITIES,
    PHASE4_UPSTREAM_DEMAND_CONTRACT,
    PHASE5_BUILD_SCOPE,
    PHASE5_DISABLED_CAPABILITIES,
    PHASE5_UPSTREAM_SUPPLY_CONTRACT,
    PHASE6_BUILD_SCOPE,
    PHASE6_DISABLED_CAPABILITIES,
    PHASE6_UPSTREAM_VALUE_CONTRACT,
    PHASE3_BUILD_SCOPE,
    PHASE3_DISABLED_CAPABILITIES,
    PHASE3_UPSTREAM_DEMAND_CONTRACT,
    SUPPORTED_CO_PACK_MODES,
    SUPPORTED_DEMAND_BASES,
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
    "demand_basis",
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
TREATMENT_DURATION_HEADERS = (
    "scenario_name",
    "module",
    "segment_code",
    "geography_code",
    "treatment_duration_months",
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
LEGACY_TRADE_HEADERS = (
    "scenario_name",
    "trade_row_type",
    "module",
    "geography_code",
    "sublayer1_target_weeks_on_hand",
    "sublayer2_target_weeks_on_hand",
    "sublayer2_wastage_rate",
    "initial_stocking_units_per_new_site",
    "ss_units_per_new_site",
    "sublayer1_launch_fill_months_of_demand",
    "rems_certification_lag_weeks",
    "january_softening_enabled",
    "january_softening_factor",
    "bullwhip_flag_threshold",
    "channel_fill_start_prelaunch_weeks",
    "sublayer2_fill_distribution_weeks",
    "weeks_per_month",
    "site_activation_rate",
    "certified_sites_at_launch",
    "certified_sites_at_peak",
    "launch_month_index",
    "bullwhip_amplification_threshold",
    "bullwhip_review_window_months",
    "excess_build_threshold_ratio",
    "supply_gap_tolerance_units",
    "capacity_clip_tolerance_units",
    "cml_prevalent_forward_window_months",
    "projected_cml_prevalent_bolus_exhaustion_month_index",
    "fg_lead_time_from_dp_release_weeks",
    "fg_packaging_cycle_weeks",
    "fg_release_qa_weeks",
    "fg_total_order_to_release_weeks",
    "fg_packaging_campaign_size_units",
    "dp_lead_time_from_ds_release_weeks",
    "dp_manufacturing_cycle_weeks",
    "dp_release_testing_weeks",
    "dp_total_order_to_release_weeks",
    "dp_min_batch_size_units",
    "dp_max_batch_size_units",
    "dp_min_campaign_batches",
    "dp_annual_capacity_batches",
    "ds_lead_time_to_batch_start_planning_horizon_weeks",
    "ds_manufacturing_cycle_weeks",
    "ds_release_testing_weeks",
    "ds_total_order_to_release_weeks",
    "ds_min_batch_size_kg",
    "ds_max_batch_size_kg",
    "ds_min_campaign_batches",
    "ds_annual_capacity_batches",
    "ss_order_to_release_lead_time_weeks",
    "ss_batch_size_units",
    "ss_min_campaign_batches",
    "ss_annual_capacity_batches",
    "ss_release_must_coincide_with_or_precede_fg",
    "starting_inventory_ds_mg",
    "starting_inventory_dp_units",
    "starting_inventory_fg_units",
    "starting_inventory_ss_units",
    "starting_inventory_sublayer1_fg_units",
    "starting_inventory_sublayer2_fg_units",
    "shelf_life_ds_months",
    "shelf_life_dp_months",
    "shelf_life_fg_months",
    "shelf_life_ss_months",
    "excess_inventory_threshold_months_of_cover",
    "stockout_tolerance_units",
    "fefo_enabled",
    "ss_fg_match_required",
    "allow_prelaunch_inventory_build",
    "phase5_enforce_unique_output_keys",
    "phase5_reconcile_phase4_receipts",
    "phase5_reconciliation_tolerance_units",
    "active_flag",
    "notes",
)
PHASE6_TRADE_HEADERS = (
    "ds_standard_cost_basis_unit",
    "ds_standard_cost_per_mg",
    "dp_conversion_cost_per_unit",
    "fg_packaging_labeling_cost_per_unit",
    "ss_standard_cost_per_unit",
    "annual_inventory_carry_rate",
    "monthly_inventory_carry_rate",
    "expired_inventory_writeoff_rate",
    "expired_inventory_salvage_rate",
    "value_unmatched_fg_at_fg_standard_cost",
    "include_trade_node_fg_value",
    "use_matched_administrable_fg_value",
    "phase6_enforce_unique_output_keys",
    "phase6_reconciliation_tolerance_value",
    "us_fg_sub1_to_sub2_cost_per_unit",
    "eu_fg_sub1_to_sub2_cost_per_unit",
    "us_ss_sub1_to_sub2_cost_per_unit",
    "eu_ss_sub1_to_sub2_cost_per_unit",
)
TRADE_HEADERS = LEGACY_TRADE_HEADERS + PHASE6_TRADE_HEADERS

ALLOWED_PARAMETER_SCOPES = ("scenario_default", "module_override")
ALLOWED_WORKBOOK_VIALING_RULES = ("patient_dose_ceiling",)
ALLOWED_PARTIAL_PACK_HANDLING = ("full_pack_consumed",)
ALLOWED_EXHAUSTION_RULES = ("track_vs_pool", "placeholder_metadata_only", "validate_only")
ALLOWED_TRADE_ROW_TYPES = ("scenario_default", "geography_default", "launch_event")
ENGINE_VIALING_RULE_BY_WORKBOOK_VALUE = {"patient_dose_ceiling": "ceil_mg_per_unit_no_sharing"}


@dataclass(frozen=True)
class AssumptionsWorkbookContext:
    scenario_name: str
    scenario_description: str
    forecast_grain: str
    forecast_frequency: str
    demand_basis: str
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
    treatment_duration_rows = _normalize_treatment_duration_assumptions(
        reader.read_table("Treatment_Duration_Assumptions", TREATMENT_DURATION_HEADERS),
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
    raw_trade_rows, trade_warnings = _read_trade_future_hooks_rows(
        reader,
        workbook_path=workbook_path.resolve(),
    )
    trade_rows = _normalize_trade_future_hooks(
        raw_trade_rows,
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
    resolved_phase3 = _resolve_phase3_config(
        trade_rows=trade_rows,
    )
    resolved_phase4 = _resolve_phase4_config(
        product_rows=product_rows,
        yield_rows=yield_rows,
        ss_rows=ss_rows,
        trade_rows=trade_rows,
    )
    resolved_phase5 = _resolve_phase5_config(
        product_rows=product_rows,
        yield_rows=yield_rows,
        ss_rows=ss_rows,
        trade_rows=trade_rows,
    )
    resolved_phase6 = _resolve_phase6_config(
        product_rows=product_rows,
        yield_rows=yield_rows,
        ss_rows=ss_rows,
        trade_rows=trade_rows,
    )
    warnings = [*trade_warnings, *warnings]

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
        treatment_duration_rows=treatment_duration_rows,
        product_rows=product_rows,
        yield_rows=yield_rows,
        packaging_rows=packaging_rows,
        ss_rows=ss_rows,
        cml_prevalent_rows=cml_prevalent_rows,
        trade_rows=trade_rows,
        resolved_phase2=resolved_phase2,
        resolved_phase3=resolved_phase3,
        resolved_phase4=resolved_phase4,
        resolved_phase5=resolved_phase5,
        resolved_phase6=resolved_phase6,
        workbook_path=workbook_path.resolve(),
        warnings=warnings,
    )
    row_counts = {
        "scenario_controls": len(scenario_controls_rows),
        "launch_timing": len(launch_timing_rows),
        "dosing_assumptions": len(dosing_rows),
        "treatment_duration_assumptions": len(treatment_duration_rows),
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


def _read_trade_future_hooks_rows(
    reader: WorkbookReader,
    *,
    workbook_path: Path,
) -> tuple[list[dict[str, str]], list[str]]:
    try:
        return reader.read_table("Trade_Inventory_FutureHooks", TRADE_HEADERS), []
    except ValueError as exc:
        message = str(exc)
        if "headers do not match the expected contract" not in message:
            raise

    legacy_rows = reader.read_table("Trade_Inventory_FutureHooks", LEGACY_TRADE_HEADERS)
    phase6_defaults = _load_phase6_trade_default_values()
    normalized_rows: list[dict[str, str]] = []
    for row in legacy_rows:
        merged_row = dict(row)
        if row.get("trade_row_type", "").strip() == "scenario_default":
            merged_row.update(phase6_defaults)
        else:
            for field_name in PHASE6_TRADE_HEADERS:
                merged_row.setdefault(field_name, "")
        normalized_rows.append(merged_row)
    warnings = [
        "Trade_Inventory_FutureHooks uses the legacy Phase 5-era header contract in "
        f"{workbook_path.name}; Phase 6 workbook fields were not present, so generated Phase 6 config "
        "used the current deterministic baseline defaults from config/parameters/phase6_financial_layer.toml, "
        "including us_fg_sub1_to_sub2_cost_per_unit, eu_fg_sub1_to_sub2_cost_per_unit, "
        "us_ss_sub1_to_sub2_cost_per_unit, and eu_ss_sub1_to_sub2_cost_per_unit. "
        "Regenerate or update the assumptions workbook template to edit Phase 6 values directly.",
    ]
    return normalized_rows, warnings


def _load_phase6_trade_default_values() -> dict[str, str]:
    config_path = Path(__file__).resolve().parents[3] / "config" / "parameters" / "phase6_financial_layer.toml"
    with config_path.open("rb") as handle:
        config_data = tomllib.load(handle)
    cost_basis = config_data["cost_basis"]
    carrying_cost = config_data["carrying_cost"]
    expiry_writeoff = config_data["expiry_writeoff"]
    valuation_policy = config_data["valuation_policy"]
    shipping_cold_chain = config_data.get("shipping_cold_chain", {})
    validation = config_data["validation"]
    return {
        "ds_standard_cost_basis_unit": str(cost_basis["ds_standard_cost_basis_unit"]),
        "ds_standard_cost_per_mg": _format_numeric(float(cost_basis["ds_standard_cost_per_mg"])),
        "dp_conversion_cost_per_unit": _format_numeric(float(cost_basis["dp_conversion_cost_per_unit"])),
        "fg_packaging_labeling_cost_per_unit": _format_numeric(
            float(cost_basis["fg_packaging_labeling_cost_per_unit"])
        ),
        "ss_standard_cost_per_unit": _format_numeric(float(cost_basis["ss_standard_cost_per_unit"])),
        "annual_inventory_carry_rate": _format_numeric(float(carrying_cost["annual_inventory_carry_rate"])),
        "monthly_inventory_carry_rate": _format_numeric(float(carrying_cost["monthly_inventory_carry_rate"])),
        "expired_inventory_writeoff_rate": _format_numeric(
            float(expiry_writeoff["expired_inventory_writeoff_rate"])
        ),
        "expired_inventory_salvage_rate": _format_numeric(
            float(expiry_writeoff["expired_inventory_salvage_rate"])
        ),
        "value_unmatched_fg_at_fg_standard_cost": _format_boolish(
            bool(valuation_policy["value_unmatched_fg_at_fg_standard_cost"])
        ),
        "include_trade_node_fg_value": _format_boolish(bool(valuation_policy["include_trade_node_fg_value"])),
        "use_matched_administrable_fg_value": _format_boolish(
            bool(valuation_policy["use_matched_administrable_fg_value"])
        ),
        "phase6_enforce_unique_output_keys": _format_boolish(bool(validation["enforce_unique_output_keys"])),
        "phase6_reconciliation_tolerance_value": _format_numeric(
            float(validation["reconciliation_tolerance_value"])
        ),
        "us_fg_sub1_to_sub2_cost_per_unit": _format_numeric(
            float(shipping_cold_chain.get("us_fg_sub1_to_sub2_cost_per_unit", 0.0))
        ),
        "eu_fg_sub1_to_sub2_cost_per_unit": _format_numeric(
            float(shipping_cold_chain.get("eu_fg_sub1_to_sub2_cost_per_unit", 0.0))
        ),
        "us_ss_sub1_to_sub2_cost_per_unit": _format_numeric(
            float(
                shipping_cold_chain.get(
                    "us_ss_sub1_to_sub2_cost_per_unit",
                    shipping_cold_chain.get("us_fg_sub1_to_sub2_cost_per_unit", 0.0),
                )
            )
        ),
        "eu_ss_sub1_to_sub2_cost_per_unit": _format_numeric(
            float(
                shipping_cold_chain.get(
                    "eu_ss_sub1_to_sub2_cost_per_unit",
                    shipping_cold_chain.get("eu_fg_sub1_to_sub2_cost_per_unit", 0.0),
                )
            )
        ),
    }


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
        demand_basis = _require_nonempty(row, "demand_basis", "Scenario_Controls", index)
        if demand_basis not in SUPPORTED_DEMAND_BASES:
            raise ValueError(
                f"Scenario_Controls row {index} has unsupported demand_basis {demand_basis!r}."
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
            "demand_basis": demand_basis,
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
        demand_basis=active_row["demand_basis"],
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


def _normalize_treatment_duration_assumptions(
    raw_rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    scenario_name: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    active_keys: set[tuple[str, str, str]] = set()
    for index, row in enumerate(raw_rows, start=2):
        if _is_blank_row(row, TREATMENT_DURATION_HEADERS):
            continue
        _validate_optional_scenario_name(
            row,
            allowed_scenario_names,
            "Treatment_Duration_Assumptions",
            index,
        )
        module = _require_module(row, "Treatment_Duration_Assumptions", index)
        segment_code = _require_nonempty(
            row,
            "segment_code",
            "Treatment_Duration_Assumptions",
            index,
        )
        if segment_code not in set(MODULE_TO_SEGMENTS[module]) | {"ALL"}:
            raise ValueError(
                "Treatment_Duration_Assumptions row "
                f"{index} has unsupported segment_code {segment_code!r} for module {module!r}."
            )
        geography_code = _normalize_scope_value(row.get("geography_code", ""))
        treatment_duration_months = _parse_positive_int(
            row.get("treatment_duration_months", ""),
            "treatment_duration_months",
            "Treatment_Duration_Assumptions",
            index,
        )
        active_flag = _parse_boolish(
            row.get("active_flag", ""),
            "active_flag",
            "Treatment_Duration_Assumptions",
            index,
        )
        key = (module, segment_code, geography_code)
        if active_flag and key in active_keys:
            raise ValueError(
                "Treatment_Duration_Assumptions row "
                f"{index} duplicates active scope {key!r}. Keep treatment duration rows unique by module, segment, and geography."
            )
        if active_flag:
            active_keys.add(key)
        rows.append(
            {
                "scenario_name": scenario_name,
                "module": module,
                "segment_code": segment_code,
                "geography_code": geography_code,
                "treatment_duration_months": str(treatment_duration_months),
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
    active_keys: set[tuple[str, str, str]] = set()
    for index, row in enumerate(raw_rows, start=2):
        if _is_blank_row(row, TRADE_HEADERS):
            continue
        _validate_optional_scenario_name(row, allowed_scenario_names, "Trade_Inventory_FutureHooks", index)
        trade_row_type = _require_nonempty(row, "trade_row_type", "Trade_Inventory_FutureHooks", index)
        if trade_row_type not in ALLOWED_TRADE_ROW_TYPES:
            raise ValueError(
                f"Trade_Inventory_FutureHooks row {index} has unsupported trade_row_type {trade_row_type!r}."
            )
        module = _normalize_scope_value(row.get("module", ""))
        geography_code = _normalize_scope_value(row.get("geography_code", ""))
        active_flag = _parse_boolish(row.get("active_flag", ""), "active_flag", "Trade_Inventory_FutureHooks", index)
        key = (trade_row_type, module, geography_code)
        if active_flag and key in active_keys:
            raise ValueError(
                f"Trade_Inventory_FutureHooks row {index} duplicates active scope {key!r}. Keep trade rows unique by trade_row_type, module, and geography."
            )
        if active_flag:
            active_keys.add(key)

        scenario_values: dict[str, str] = {
            "sublayer1_target_weeks_on_hand": "",
            "sublayer2_target_weeks_on_hand": "",
            "sublayer2_wastage_rate": "",
            "initial_stocking_units_per_new_site": "",
            "ss_units_per_new_site": "",
            "sublayer1_launch_fill_months_of_demand": "",
            "rems_certification_lag_weeks": "",
            "january_softening_enabled": "",
            "january_softening_factor": "",
            "bullwhip_flag_threshold": "",
            "channel_fill_start_prelaunch_weeks": "",
            "sublayer2_fill_distribution_weeks": "",
            "weeks_per_month": "",
            "site_activation_rate": "",
            "certified_sites_at_launch": "",
            "certified_sites_at_peak": "",
            "launch_month_index": "",
            "bullwhip_amplification_threshold": "",
            "bullwhip_review_window_months": "",
            "excess_build_threshold_ratio": "",
            "supply_gap_tolerance_units": "",
            "capacity_clip_tolerance_units": "",
            "cml_prevalent_forward_window_months": "",
            "projected_cml_prevalent_bolus_exhaustion_month_index": "",
            "fg_lead_time_from_dp_release_weeks": "",
            "fg_packaging_cycle_weeks": "",
            "fg_release_qa_weeks": "",
            "fg_total_order_to_release_weeks": "",
            "fg_packaging_campaign_size_units": "",
            "dp_lead_time_from_ds_release_weeks": "",
            "dp_manufacturing_cycle_weeks": "",
            "dp_release_testing_weeks": "",
            "dp_total_order_to_release_weeks": "",
            "dp_min_batch_size_units": "",
            "dp_max_batch_size_units": "",
            "dp_min_campaign_batches": "",
            "dp_annual_capacity_batches": "",
            "ds_lead_time_to_batch_start_planning_horizon_weeks": "",
            "ds_manufacturing_cycle_weeks": "",
            "ds_release_testing_weeks": "",
            "ds_total_order_to_release_weeks": "",
            "ds_min_batch_size_kg": "",
            "ds_max_batch_size_kg": "",
            "ds_min_campaign_batches": "",
            "ds_annual_capacity_batches": "",
            "ss_order_to_release_lead_time_weeks": "",
            "ss_batch_size_units": "",
            "ss_min_campaign_batches": "",
            "ss_annual_capacity_batches": "",
            "ss_release_must_coincide_with_or_precede_fg": "",
            "starting_inventory_ds_mg": "",
            "starting_inventory_dp_units": "",
            "starting_inventory_fg_units": "",
            "starting_inventory_ss_units": "",
            "starting_inventory_sublayer1_fg_units": "",
            "starting_inventory_sublayer2_fg_units": "",
            "shelf_life_ds_months": "",
            "shelf_life_dp_months": "",
            "shelf_life_fg_months": "",
            "shelf_life_ss_months": "",
            "excess_inventory_threshold_months_of_cover": "",
            "stockout_tolerance_units": "",
            "fefo_enabled": "",
            "ss_fg_match_required": "",
            "allow_prelaunch_inventory_build": "",
            "phase5_enforce_unique_output_keys": "",
            "phase5_reconcile_phase4_receipts": "",
            "phase5_reconciliation_tolerance_units": "",
            "ds_standard_cost_basis_unit": "",
            "ds_standard_cost_per_mg": "",
            "dp_conversion_cost_per_unit": "",
            "fg_packaging_labeling_cost_per_unit": "",
            "ss_standard_cost_per_unit": "",
            "annual_inventory_carry_rate": "",
            "monthly_inventory_carry_rate": "",
            "expired_inventory_writeoff_rate": "",
            "expired_inventory_salvage_rate": "",
            "value_unmatched_fg_at_fg_standard_cost": "",
            "include_trade_node_fg_value": "",
            "use_matched_administrable_fg_value": "",
            "phase6_enforce_unique_output_keys": "",
            "phase6_reconciliation_tolerance_value": "",
        }

        if trade_row_type == "scenario_default":
            if module != "ALL" or geography_code != "ALL":
                raise ValueError(
                    f"Trade_Inventory_FutureHooks row {index} must use module='ALL' and geography_code='ALL' for trade_row_type='scenario_default'."
                )
            scenario_values.update(
                {
                    "sublayer1_target_weeks_on_hand": _format_numeric(
                        _parse_positive_float(
                            row.get("sublayer1_target_weeks_on_hand", ""),
                            "sublayer1_target_weeks_on_hand",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "sublayer2_target_weeks_on_hand": _format_numeric(
                        _parse_positive_float(
                            row.get("sublayer2_target_weeks_on_hand", ""),
                            "sublayer2_target_weeks_on_hand",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "sublayer2_wastage_rate": _format_numeric(
                        _parse_probability(
                            row.get("sublayer2_wastage_rate", ""),
                            "sublayer2_wastage_rate",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "initial_stocking_units_per_new_site": _format_numeric(
                        _parse_positive_float(
                            row.get("initial_stocking_units_per_new_site", ""),
                            "initial_stocking_units_per_new_site",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ss_units_per_new_site": _format_numeric(
                        _parse_positive_float(
                            row.get("ss_units_per_new_site", ""),
                            "ss_units_per_new_site",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "sublayer1_launch_fill_months_of_demand": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("sublayer1_launch_fill_months_of_demand", ""),
                            "sublayer1_launch_fill_months_of_demand",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "rems_certification_lag_weeks": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("rems_certification_lag_weeks", ""),
                            "rems_certification_lag_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "january_softening_enabled": _format_boolish(
                        _parse_boolish(
                            row.get("january_softening_enabled", ""),
                            "january_softening_enabled",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "january_softening_factor": _format_numeric(
                        _parse_probability(
                            row.get("january_softening_factor", ""),
                            "january_softening_factor",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "bullwhip_flag_threshold": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("bullwhip_flag_threshold", ""),
                            "bullwhip_flag_threshold",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "channel_fill_start_prelaunch_weeks": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("channel_fill_start_prelaunch_weeks", ""),
                            "channel_fill_start_prelaunch_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "sublayer2_fill_distribution_weeks": _format_numeric(
                        _parse_positive_float(
                            row.get("sublayer2_fill_distribution_weeks", ""),
                            "sublayer2_fill_distribution_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "weeks_per_month": _format_numeric(
                        _parse_positive_float(
                            row.get("weeks_per_month", ""),
                            "weeks_per_month",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "bullwhip_amplification_threshold": _format_numeric(
                        _parse_positive_float(
                            row.get("bullwhip_amplification_threshold", ""),
                            "bullwhip_amplification_threshold",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "bullwhip_review_window_months": _format_optional_int(
                        _parse_positive_int(
                            row.get("bullwhip_review_window_months", ""),
                            "bullwhip_review_window_months",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "excess_build_threshold_ratio": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("excess_build_threshold_ratio", ""),
                            "excess_build_threshold_ratio",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "supply_gap_tolerance_units": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("supply_gap_tolerance_units", ""),
                            "supply_gap_tolerance_units",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "capacity_clip_tolerance_units": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("capacity_clip_tolerance_units", ""),
                            "capacity_clip_tolerance_units",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "cml_prevalent_forward_window_months": _format_optional_int(
                        _parse_positive_int(
                            row.get("cml_prevalent_forward_window_months", ""),
                            "cml_prevalent_forward_window_months",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "projected_cml_prevalent_bolus_exhaustion_month_index": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("projected_cml_prevalent_bolus_exhaustion_month_index", ""),
                            "projected_cml_prevalent_bolus_exhaustion_month_index",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "fg_lead_time_from_dp_release_weeks": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("fg_lead_time_from_dp_release_weeks", ""),
                            "fg_lead_time_from_dp_release_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "fg_packaging_cycle_weeks": _format_numeric(
                        _parse_positive_float(
                            row.get("fg_packaging_cycle_weeks", ""),
                            "fg_packaging_cycle_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "fg_release_qa_weeks": _format_numeric(
                        _parse_positive_float(
                            row.get("fg_release_qa_weeks", ""),
                            "fg_release_qa_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "fg_total_order_to_release_weeks": _format_numeric(
                        _parse_positive_float(
                            row.get("fg_total_order_to_release_weeks", ""),
                            "fg_total_order_to_release_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "fg_packaging_campaign_size_units": _format_numeric(
                        _parse_positive_float(
                            row.get("fg_packaging_campaign_size_units", ""),
                            "fg_packaging_campaign_size_units",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "dp_lead_time_from_ds_release_weeks": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("dp_lead_time_from_ds_release_weeks", ""),
                            "dp_lead_time_from_ds_release_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "dp_manufacturing_cycle_weeks": _format_numeric(
                        _parse_positive_float(
                            row.get("dp_manufacturing_cycle_weeks", ""),
                            "dp_manufacturing_cycle_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "dp_release_testing_weeks": _format_numeric(
                        _parse_positive_float(
                            row.get("dp_release_testing_weeks", ""),
                            "dp_release_testing_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "dp_total_order_to_release_weeks": _format_numeric(
                        _parse_positive_float(
                            row.get("dp_total_order_to_release_weeks", ""),
                            "dp_total_order_to_release_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "dp_min_batch_size_units": _format_numeric(
                        _parse_positive_float(
                            row.get("dp_min_batch_size_units", ""),
                            "dp_min_batch_size_units",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "dp_max_batch_size_units": _format_numeric(
                        _parse_positive_float(
                            row.get("dp_max_batch_size_units", ""),
                            "dp_max_batch_size_units",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "dp_min_campaign_batches": _format_optional_int(
                        _parse_positive_int(
                            row.get("dp_min_campaign_batches", ""),
                            "dp_min_campaign_batches",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "dp_annual_capacity_batches": _format_optional_int(
                        _parse_positive_int(
                            row.get("dp_annual_capacity_batches", ""),
                            "dp_annual_capacity_batches",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ds_lead_time_to_batch_start_planning_horizon_weeks": _format_numeric(
                        _parse_positive_float(
                            row.get("ds_lead_time_to_batch_start_planning_horizon_weeks", ""),
                            "ds_lead_time_to_batch_start_planning_horizon_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ds_manufacturing_cycle_weeks": _format_numeric(
                        _parse_positive_float(
                            row.get("ds_manufacturing_cycle_weeks", ""),
                            "ds_manufacturing_cycle_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ds_release_testing_weeks": _format_numeric(
                        _parse_positive_float(
                            row.get("ds_release_testing_weeks", ""),
                            "ds_release_testing_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ds_total_order_to_release_weeks": _format_numeric(
                        _parse_positive_float(
                            row.get("ds_total_order_to_release_weeks", ""),
                            "ds_total_order_to_release_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ds_min_batch_size_kg": _format_numeric(
                        _parse_positive_float(
                            row.get("ds_min_batch_size_kg", ""),
                            "ds_min_batch_size_kg",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ds_max_batch_size_kg": _format_numeric(
                        _parse_positive_float(
                            row.get("ds_max_batch_size_kg", ""),
                            "ds_max_batch_size_kg",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ds_min_campaign_batches": _format_optional_int(
                        _parse_positive_int(
                            row.get("ds_min_campaign_batches", ""),
                            "ds_min_campaign_batches",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ds_annual_capacity_batches": _format_optional_int(
                        _parse_positive_int(
                            row.get("ds_annual_capacity_batches", ""),
                            "ds_annual_capacity_batches",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ss_order_to_release_lead_time_weeks": _format_numeric(
                        _parse_positive_float(
                            row.get("ss_order_to_release_lead_time_weeks", ""),
                            "ss_order_to_release_lead_time_weeks",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ss_batch_size_units": _format_numeric(
                        _parse_positive_float(
                            row.get("ss_batch_size_units", ""),
                            "ss_batch_size_units",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ss_min_campaign_batches": _format_optional_int(
                        _parse_positive_int(
                            row.get("ss_min_campaign_batches", ""),
                            "ss_min_campaign_batches",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ss_annual_capacity_batches": _format_optional_int(
                        _parse_positive_int(
                            row.get("ss_annual_capacity_batches", ""),
                            "ss_annual_capacity_batches",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ss_release_must_coincide_with_or_precede_fg": _format_boolish(
                        _parse_boolish(
                            row.get("ss_release_must_coincide_with_or_precede_fg", ""),
                            "ss_release_must_coincide_with_or_precede_fg",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "starting_inventory_ds_mg": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("starting_inventory_ds_mg", ""),
                            "starting_inventory_ds_mg",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "starting_inventory_dp_units": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("starting_inventory_dp_units", ""),
                            "starting_inventory_dp_units",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "starting_inventory_fg_units": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("starting_inventory_fg_units", ""),
                            "starting_inventory_fg_units",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "starting_inventory_ss_units": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("starting_inventory_ss_units", ""),
                            "starting_inventory_ss_units",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "starting_inventory_sublayer1_fg_units": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("starting_inventory_sublayer1_fg_units", ""),
                            "starting_inventory_sublayer1_fg_units",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "starting_inventory_sublayer2_fg_units": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("starting_inventory_sublayer2_fg_units", ""),
                            "starting_inventory_sublayer2_fg_units",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "shelf_life_ds_months": _format_optional_int(
                        _parse_positive_int(
                            row.get("shelf_life_ds_months", ""),
                            "shelf_life_ds_months",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "shelf_life_dp_months": _format_optional_int(
                        _parse_positive_int(
                            row.get("shelf_life_dp_months", ""),
                            "shelf_life_dp_months",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "shelf_life_fg_months": _format_optional_int(
                        _parse_positive_int(
                            row.get("shelf_life_fg_months", ""),
                            "shelf_life_fg_months",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "shelf_life_ss_months": _format_optional_int(
                        _parse_positive_int(
                            row.get("shelf_life_ss_months", ""),
                            "shelf_life_ss_months",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "excess_inventory_threshold_months_of_cover": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("excess_inventory_threshold_months_of_cover", ""),
                            "excess_inventory_threshold_months_of_cover",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "stockout_tolerance_units": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("stockout_tolerance_units", ""),
                            "stockout_tolerance_units",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "fefo_enabled": _format_boolish(
                        _parse_boolish(
                            row.get("fefo_enabled", ""),
                            "fefo_enabled",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ss_fg_match_required": _format_boolish(
                        _parse_boolish(
                            row.get("ss_fg_match_required", ""),
                            "ss_fg_match_required",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "allow_prelaunch_inventory_build": _format_boolish(
                        _parse_boolish(
                            row.get("allow_prelaunch_inventory_build", ""),
                            "allow_prelaunch_inventory_build",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "phase5_enforce_unique_output_keys": _format_boolish(
                        _parse_boolish(
                            row.get("phase5_enforce_unique_output_keys", ""),
                            "phase5_enforce_unique_output_keys",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "phase5_reconcile_phase4_receipts": _format_boolish(
                        _parse_boolish(
                            row.get("phase5_reconcile_phase4_receipts", ""),
                            "phase5_reconcile_phase4_receipts",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "phase5_reconciliation_tolerance_units": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("phase5_reconciliation_tolerance_units", ""),
                            "phase5_reconciliation_tolerance_units",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ds_standard_cost_basis_unit": _require_nonempty(
                        row,
                        "ds_standard_cost_basis_unit",
                        "Trade_Inventory_FutureHooks",
                        index,
                    ),
                    "ds_standard_cost_per_mg": _format_numeric(
                        _parse_positive_float(
                            row.get("ds_standard_cost_per_mg", ""),
                            "ds_standard_cost_per_mg",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "dp_conversion_cost_per_unit": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("dp_conversion_cost_per_unit", ""),
                            "dp_conversion_cost_per_unit",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "fg_packaging_labeling_cost_per_unit": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("fg_packaging_labeling_cost_per_unit", ""),
                            "fg_packaging_labeling_cost_per_unit",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "ss_standard_cost_per_unit": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("ss_standard_cost_per_unit", ""),
                            "ss_standard_cost_per_unit",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "annual_inventory_carry_rate": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("annual_inventory_carry_rate", ""),
                            "annual_inventory_carry_rate",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "monthly_inventory_carry_rate": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("monthly_inventory_carry_rate", ""),
                            "monthly_inventory_carry_rate",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "expired_inventory_writeoff_rate": _format_numeric(
                        _parse_probability(
                            row.get("expired_inventory_writeoff_rate", ""),
                            "expired_inventory_writeoff_rate",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "expired_inventory_salvage_rate": _format_numeric(
                        _parse_probability(
                            row.get("expired_inventory_salvage_rate", ""),
                            "expired_inventory_salvage_rate",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "value_unmatched_fg_at_fg_standard_cost": _format_boolish(
                        _parse_boolish(
                            row.get("value_unmatched_fg_at_fg_standard_cost", ""),
                            "value_unmatched_fg_at_fg_standard_cost",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "include_trade_node_fg_value": _format_boolish(
                        _parse_boolish(
                            row.get("include_trade_node_fg_value", ""),
                            "include_trade_node_fg_value",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "use_matched_administrable_fg_value": _format_boolish(
                        _parse_boolish(
                            row.get("use_matched_administrable_fg_value", ""),
                            "use_matched_administrable_fg_value",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "phase6_enforce_unique_output_keys": _format_boolish(
                        _parse_boolish(
                            row.get("phase6_enforce_unique_output_keys", ""),
                            "phase6_enforce_unique_output_keys",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "phase6_reconciliation_tolerance_value": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("phase6_reconciliation_tolerance_value", ""),
                            "phase6_reconciliation_tolerance_value",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "us_fg_sub1_to_sub2_cost_per_unit": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("us_fg_sub1_to_sub2_cost_per_unit", ""),
                            "us_fg_sub1_to_sub2_cost_per_unit",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "eu_fg_sub1_to_sub2_cost_per_unit": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("eu_fg_sub1_to_sub2_cost_per_unit", ""),
                            "eu_fg_sub1_to_sub2_cost_per_unit",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "us_ss_sub1_to_sub2_cost_per_unit": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("us_ss_sub1_to_sub2_cost_per_unit", ""),
                            "us_ss_sub1_to_sub2_cost_per_unit",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "eu_ss_sub1_to_sub2_cost_per_unit": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("eu_ss_sub1_to_sub2_cost_per_unit", ""),
                            "eu_ss_sub1_to_sub2_cost_per_unit",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                }
            )
        elif trade_row_type == "geography_default":
            if module != "ALL":
                raise ValueError(
                    f"Trade_Inventory_FutureHooks row {index} must use module='ALL' for trade_row_type='geography_default'."
                )
            if geography_code == "ALL":
                raise ValueError(
                    f"Trade_Inventory_FutureHooks row {index} must provide a real geography_code for trade_row_type='geography_default'."
                )
            scenario_values.update(
                {
                    "site_activation_rate": _format_numeric(
                        _parse_positive_float(
                            row.get("site_activation_rate", ""),
                            "site_activation_rate",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "certified_sites_at_launch": _format_numeric(
                        _parse_nonnegative_float(
                            row.get("certified_sites_at_launch", ""),
                            "certified_sites_at_launch",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                    "certified_sites_at_peak": _format_numeric(
                        _parse_positive_float(
                            row.get("certified_sites_at_peak", ""),
                            "certified_sites_at_peak",
                            "Trade_Inventory_FutureHooks",
                            index,
                        )
                    ),
                }
            )
        else:
            if module not in PHASE1_MODULES:
                raise ValueError(
                    f"Trade_Inventory_FutureHooks row {index} must use a supported module for trade_row_type='launch_event'."
                )
            if geography_code == "ALL":
                raise ValueError(
                    f"Trade_Inventory_FutureHooks row {index} must provide a real geography_code for trade_row_type='launch_event'."
                )
            scenario_values["launch_month_index"] = _format_optional_int(
                _parse_positive_int(
                    row.get("launch_month_index", ""),
                    "launch_month_index",
                    "Trade_Inventory_FutureHooks",
                    index,
                )
            )

        rows.append(
            {
                "scenario_name": scenario_name,
                "trade_row_type": trade_row_type,
                "module": module,
                "geography_code": geography_code,
                **scenario_values,
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
            "Yield_Assumptions module_override ds_overage_factor rows are preserved in normalized artifacts. Current Phase 2 wiring still uses the scenario_default row for ds_overage_factor and plan yields."
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


def _resolve_phase3_config(
    *,
    trade_rows: list[dict[str, str]],
) -> dict[str, object]:
    active_trade_rows = _active_rows(trade_rows)
    scenario_default_row = _find_active_row(
        active_trade_rows,
        sheet_name="Trade_Inventory_FutureHooks",
        required_fields={"trade_row_type": "scenario_default", "module": "ALL", "geography_code": "ALL"},
        error_hint="Provide one active scenario_default row with module = ALL and geography_code = ALL.",
    )

    geography_default_rows = [
        row for row in active_trade_rows if row["trade_row_type"] == "geography_default"
    ]
    if not geography_default_rows:
        raise ValueError(
            "Trade_Inventory_FutureHooks requires at least one active geography_default row for Phase 3."
        )

    launch_event_rows = [
        row for row in active_trade_rows if row["trade_row_type"] == "launch_event"
    ]
    if not launch_event_rows:
        raise ValueError(
            "Trade_Inventory_FutureHooks requires active launch_event rows for Phase 3."
        )

    geography_defaults: dict[str, dict[str, float]] = {}
    launch_events: dict[str, dict[str, dict[str, int]]] = {}
    for geography_row in geography_default_rows:
        geography_code = geography_row["geography_code"]
        geography_defaults[geography_code] = {
            "site_activation_rate": float(geography_row["site_activation_rate"]),
            "certified_sites_at_launch": float(geography_row["certified_sites_at_launch"]),
            "certified_sites_at_peak": float(geography_row["certified_sites_at_peak"]),
        }
        for module in PHASE1_MODULES:
            launch_row = _find_active_row(
                active_trade_rows,
                sheet_name="Trade_Inventory_FutureHooks",
                required_fields={
                    "trade_row_type": "launch_event",
                    "module": module,
                    "geography_code": geography_code,
                },
                error_hint=(
                    "Provide one active launch_event row per module and geography to build the active deterministic Phase 3 config."
                ),
            )
            launch_events.setdefault(module, {})[geography_code] = {
                "launch_month_index": int(launch_row["launch_month_index"])
            }

    return {
        "model": {
            "phase": 3,
            "build_scope": PHASE3_BUILD_SCOPE,
            "upstream_demand_contract": PHASE3_UPSTREAM_DEMAND_CONTRACT,
        },
        "modules": {
            "enabled": list(PHASE1_MODULES),
            "disabled": list(PHASE3_DISABLED_CAPABILITIES),
        },
        "trade": {
            "sublayer1_target_weeks_on_hand": float(scenario_default_row["sublayer1_target_weeks_on_hand"]),
            "sublayer2_target_weeks_on_hand": float(scenario_default_row["sublayer2_target_weeks_on_hand"]),
            "sublayer2_wastage_rate": float(scenario_default_row["sublayer2_wastage_rate"]),
            "initial_stocking_units_per_new_site": float(scenario_default_row["initial_stocking_units_per_new_site"]),
            "ss_units_per_new_site": float(scenario_default_row["ss_units_per_new_site"]),
            "sublayer1_launch_fill_months_of_demand": float(
                scenario_default_row["sublayer1_launch_fill_months_of_demand"]
            ),
            "rems_certification_lag_weeks": float(scenario_default_row["rems_certification_lag_weeks"]),
            "january_softening_enabled": _parse_stored_bool(
                scenario_default_row["january_softening_enabled"]
            ),
            "january_softening_factor": float(scenario_default_row["january_softening_factor"]),
            "bullwhip_flag_threshold": float(scenario_default_row["bullwhip_flag_threshold"]),
            "channel_fill_start_prelaunch_weeks": float(
                scenario_default_row["channel_fill_start_prelaunch_weeks"]
            ),
            "sublayer2_fill_distribution_weeks": float(
                scenario_default_row["sublayer2_fill_distribution_weeks"]
            ),
            "weeks_per_month": float(scenario_default_row["weeks_per_month"]),
        },
        "geography_defaults": geography_defaults,
        "launch_events": launch_events,
        "validation": {
            "enforce_unique_output_keys": True,
        },
        "wiring_notes": [
            "Trade_Inventory_FutureHooks scenario_default row feeds trade.* in the active deterministic Phase 3 config.",
            "Trade_Inventory_FutureHooks geography_default rows feed geography_defaults.<geography> site activation and certified site assumptions.",
            "Trade_Inventory_FutureHooks launch_event rows feed launch_events.<module>.<geography>.launch_month_index.",
        ],
    }


def _resolve_phase4_config(
    *,
    product_rows: list[dict[str, str]],
    yield_rows: list[dict[str, str]],
    ss_rows: list[dict[str, str]],
    trade_rows: list[dict[str, str]],
) -> dict[str, object]:
    active_product_rows = _active_rows(product_rows)
    active_yield_rows = _active_rows(yield_rows)
    active_ss_rows = _active_rows(ss_rows)
    active_trade_rows = _active_rows(trade_rows)

    product_default_row = _find_active_row(
        active_product_rows,
        sheet_name="Product_Parameters",
        required_fields={"parameter_scope": "scenario_default", "module": "ALL", "geography_code": "ALL"},
        error_hint="Provide an active scenario_default product row with module = ALL and geography_code = ALL.",
    )
    yield_default_row = _find_active_row(
        active_yield_rows,
        sheet_name="Yield_Assumptions",
        required_fields={"parameter_scope": "scenario_default", "module": "ALL", "geography_code": "ALL"},
        error_hint="Provide one active scenario_default yield row with module = ALL and geography_code = ALL.",
    )
    ss_default_row = _resolve_ss_default_row(active_ss_rows)
    scenario_default_trade_row = _find_active_row(
        active_trade_rows,
        sheet_name="Trade_Inventory_FutureHooks",
        required_fields={"trade_row_type": "scenario_default", "module": "ALL", "geography_code": "ALL"},
        error_hint="Provide one active scenario_default row with module = ALL and geography_code = ALL.",
    )

    return {
        "model": {
            "phase": 4,
            "build_scope": PHASE4_BUILD_SCOPE,
            "upstream_demand_contract": PHASE4_UPSTREAM_DEMAND_CONTRACT,
        },
        "modules": {
            "enabled": list(PHASE1_MODULES),
            "disabled": list(PHASE4_DISABLED_CAPABILITIES),
        },
        "conversion": {
            "dp_to_fg_yield": float(yield_default_row["dp_to_fg_yield"]),
            "ds_to_dp_yield": float(yield_default_row["ds_to_dp_yield"]),
            "ds_qty_per_dp_unit_mg": float(product_default_row["ds_qty_per_dp_unit_mg"]),
            "ds_overage_factor": float(yield_default_row["ds_overage_factor"]),
            "ss_ratio_to_fg": float(ss_default_row["ss_ratio_to_fg"]),
            "weeks_per_month": float(scenario_default_trade_row["weeks_per_month"]),
        },
        "review": {
            "bullwhip_amplification_threshold": float(
                scenario_default_trade_row["bullwhip_amplification_threshold"]
            ),
            "bullwhip_review_window_months": int(
                scenario_default_trade_row["bullwhip_review_window_months"]
            ),
            "excess_build_threshold_ratio": float(
                scenario_default_trade_row["excess_build_threshold_ratio"]
            ),
            "supply_gap_tolerance_units": float(
                scenario_default_trade_row["supply_gap_tolerance_units"]
            ),
            "capacity_clip_tolerance_units": float(
                scenario_default_trade_row["capacity_clip_tolerance_units"]
            ),
        },
        "stepdown": {
            "cml_prevalent_forward_window_months": int(
                scenario_default_trade_row["cml_prevalent_forward_window_months"]
            ),
            "projected_cml_prevalent_bolus_exhaustion_month_index": int(
                float(scenario_default_trade_row["projected_cml_prevalent_bolus_exhaustion_month_index"])
            ),
        },
        "fg": {
            "lead_time_from_dp_release_weeks": float(
                scenario_default_trade_row["fg_lead_time_from_dp_release_weeks"]
            ),
            "packaging_cycle_weeks": float(scenario_default_trade_row["fg_packaging_cycle_weeks"]),
            "release_qa_weeks": float(scenario_default_trade_row["fg_release_qa_weeks"]),
            "total_order_to_release_weeks": float(
                scenario_default_trade_row["fg_total_order_to_release_weeks"]
            ),
            "packaging_campaign_size_units": float(
                scenario_default_trade_row["fg_packaging_campaign_size_units"]
            ),
        },
        "dp": {
            "lead_time_from_ds_release_weeks": float(
                scenario_default_trade_row["dp_lead_time_from_ds_release_weeks"]
            ),
            "manufacturing_cycle_weeks": float(
                scenario_default_trade_row["dp_manufacturing_cycle_weeks"]
            ),
            "release_testing_weeks": float(scenario_default_trade_row["dp_release_testing_weeks"]),
            "total_order_to_release_weeks": float(
                scenario_default_trade_row["dp_total_order_to_release_weeks"]
            ),
            "min_batch_size_units": float(scenario_default_trade_row["dp_min_batch_size_units"]),
            "max_batch_size_units": float(scenario_default_trade_row["dp_max_batch_size_units"]),
            "min_campaign_batches": int(scenario_default_trade_row["dp_min_campaign_batches"]),
            "annual_capacity_batches": int(scenario_default_trade_row["dp_annual_capacity_batches"]),
        },
        "ds": {
            "lead_time_to_batch_start_planning_horizon_weeks": float(
                scenario_default_trade_row["ds_lead_time_to_batch_start_planning_horizon_weeks"]
            ),
            "manufacturing_cycle_weeks": float(
                scenario_default_trade_row["ds_manufacturing_cycle_weeks"]
            ),
            "release_testing_weeks": float(scenario_default_trade_row["ds_release_testing_weeks"]),
            "total_order_to_release_weeks": float(
                scenario_default_trade_row["ds_total_order_to_release_weeks"]
            ),
            "min_batch_size_kg": float(scenario_default_trade_row["ds_min_batch_size_kg"]),
            "max_batch_size_kg": float(scenario_default_trade_row["ds_max_batch_size_kg"]),
            "min_campaign_batches": int(scenario_default_trade_row["ds_min_campaign_batches"]),
            "annual_capacity_batches": int(scenario_default_trade_row["ds_annual_capacity_batches"]),
        },
        "ss": {
            "order_to_release_lead_time_weeks": float(
                scenario_default_trade_row["ss_order_to_release_lead_time_weeks"]
            ),
            "batch_size_units": float(scenario_default_trade_row["ss_batch_size_units"]),
            "min_campaign_batches": int(scenario_default_trade_row["ss_min_campaign_batches"]),
            "annual_capacity_batches": int(scenario_default_trade_row["ss_annual_capacity_batches"]),
            "release_must_coincide_with_or_precede_fg": _parse_stored_bool(
                scenario_default_trade_row["ss_release_must_coincide_with_or_precede_fg"]
            ),
        },
        "validation": {
            "enforce_unique_output_keys": True,
        },
        "wiring_notes": [
            "Product_Parameters scenario_default row feeds conversion.ds_qty_per_dp_unit_mg in the active deterministic Phase 4 config.",
            "Yield_Assumptions scenario_default row feeds conversion.dp_to_fg_yield / conversion.ds_to_dp_yield / conversion.ds_overage_factor in the active deterministic Phase 4 config.",
            "SS_Assumptions scenario default row feeds conversion.ss_ratio_to_fg in the active deterministic Phase 4 config.",
            "Trade_Inventory_FutureHooks scenario_default row feeds active deterministic Phase 4 review, stepdown, FG, DP, DS, and SS scheduling parameters.",
        ],
    }


def _resolve_phase5_config(
    *,
    product_rows: list[dict[str, str]],
    yield_rows: list[dict[str, str]],
    ss_rows: list[dict[str, str]],
    trade_rows: list[dict[str, str]],
) -> dict[str, object]:
    active_product_rows = _active_rows(product_rows)
    active_yield_rows = _active_rows(yield_rows)
    active_ss_rows = _active_rows(ss_rows)
    active_trade_rows = _active_rows(trade_rows)

    product_default_row = _find_active_row(
        active_product_rows,
        sheet_name="Product_Parameters",
        required_fields={"parameter_scope": "scenario_default", "module": "ALL", "geography_code": "ALL"},
        error_hint="Provide an active scenario_default product row with module = ALL and geography_code = ALL.",
    )
    yield_default_row = _find_active_row(
        active_yield_rows,
        sheet_name="Yield_Assumptions",
        required_fields={"parameter_scope": "scenario_default", "module": "ALL", "geography_code": "ALL"},
        error_hint="Provide one active scenario_default yield row with module = ALL and geography_code = ALL.",
    )
    ss_default_row = _resolve_ss_default_row(active_ss_rows)
    scenario_default_trade_row = _find_active_row(
        active_trade_rows,
        sheet_name="Trade_Inventory_FutureHooks",
        required_fields={"trade_row_type": "scenario_default", "module": "ALL", "geography_code": "ALL"},
        error_hint="Provide one active scenario_default row with module = ALL and geography_code = ALL.",
    )

    return {
        "model": {
            "phase": 5,
            "build_scope": PHASE5_BUILD_SCOPE,
            "upstream_supply_contract": PHASE5_UPSTREAM_SUPPLY_CONTRACT,
        },
        "modules": {
            "enabled": list(PHASE1_MODULES),
            "disabled": list(PHASE5_DISABLED_CAPABILITIES),
        },
        "starting_inventory": {
            "ds_mg": float(scenario_default_trade_row["starting_inventory_ds_mg"]),
            "dp_units": float(scenario_default_trade_row["starting_inventory_dp_units"]),
            "fg_units": float(scenario_default_trade_row["starting_inventory_fg_units"]),
            "ss_units": float(scenario_default_trade_row["starting_inventory_ss_units"]),
            "sublayer1_fg_units": float(
                scenario_default_trade_row["starting_inventory_sublayer1_fg_units"]
            ),
            "sublayer2_fg_units": float(
                scenario_default_trade_row["starting_inventory_sublayer2_fg_units"]
            ),
        },
        "shelf_life": {
            "ds_months": int(scenario_default_trade_row["shelf_life_ds_months"]),
            "dp_months": int(scenario_default_trade_row["shelf_life_dp_months"]),
            "fg_months": int(scenario_default_trade_row["shelf_life_fg_months"]),
            "ss_months": int(scenario_default_trade_row["shelf_life_ss_months"]),
        },
        "policy": {
            "excess_inventory_threshold_months_of_cover": float(
                scenario_default_trade_row["excess_inventory_threshold_months_of_cover"]
            ),
            "stockout_tolerance_units": float(scenario_default_trade_row["stockout_tolerance_units"]),
            "fefo_enabled": _parse_stored_bool(scenario_default_trade_row["fefo_enabled"]),
            "ss_fg_match_required": _parse_stored_bool(
                scenario_default_trade_row["ss_fg_match_required"]
            ),
            "allow_prelaunch_inventory_build": _parse_stored_bool(
                scenario_default_trade_row["allow_prelaunch_inventory_build"]
            ),
        },
        "conversion": {
            "dp_to_fg_yield": float(yield_default_row["dp_to_fg_yield"]),
            "ds_to_dp_yield": float(yield_default_row["ds_to_dp_yield"]),
            "ds_qty_per_dp_unit_mg": float(product_default_row["ds_qty_per_dp_unit_mg"]),
            "ds_overage_factor": float(yield_default_row["ds_overage_factor"]),
            "ss_ratio_to_fg": float(ss_default_row["ss_ratio_to_fg"]),
        },
        "validation": {
            "enforce_unique_output_keys": _parse_stored_bool(
                scenario_default_trade_row["phase5_enforce_unique_output_keys"]
            ),
            "reconcile_phase4_receipts": _parse_stored_bool(
                scenario_default_trade_row["phase5_reconcile_phase4_receipts"]
            ),
            "reconciliation_tolerance_units": float(
                scenario_default_trade_row["phase5_reconciliation_tolerance_units"]
            ),
        },
        "wiring_notes": [
            "Trade_Inventory_FutureHooks scenario_default row feeds active deterministic Phase 5 starting inventory, shelf life, policy, and validation parameters.",
            "Product_Parameters scenario_default row feeds conversion.ds_qty_per_dp_unit_mg in the active deterministic Phase 5 config.",
            "Yield_Assumptions scenario_default row feeds conversion.dp_to_fg_yield / conversion.ds_to_dp_yield / conversion.ds_overage_factor in the active deterministic Phase 5 config.",
            "SS_Assumptions scenario default row feeds conversion.ss_ratio_to_fg in the active deterministic Phase 5 config.",
        ],
    }


def _resolve_phase6_config(
    *,
    product_rows: list[dict[str, str]],
    yield_rows: list[dict[str, str]],
    ss_rows: list[dict[str, str]],
    trade_rows: list[dict[str, str]],
) -> dict[str, object]:
    active_product_rows = _active_rows(product_rows)
    active_yield_rows = _active_rows(yield_rows)
    active_ss_rows = _active_rows(ss_rows)
    active_trade_rows = _active_rows(trade_rows)

    product_default_row = _find_active_row(
        active_product_rows,
        sheet_name="Product_Parameters",
        required_fields={"parameter_scope": "scenario_default", "module": "ALL", "geography_code": "ALL"},
        error_hint="Provide an active scenario_default product row with module = ALL and geography_code = ALL.",
    )
    yield_default_row = _find_active_row(
        active_yield_rows,
        sheet_name="Yield_Assumptions",
        required_fields={"parameter_scope": "scenario_default", "module": "ALL", "geography_code": "ALL"},
        error_hint="Provide one active scenario_default yield row with module = ALL and geography_code = ALL.",
    )
    ss_default_row = _resolve_ss_default_row(active_ss_rows)
    scenario_default_trade_row = _find_active_row(
        active_trade_rows,
        sheet_name="Trade_Inventory_FutureHooks",
        required_fields={"trade_row_type": "scenario_default", "module": "ALL", "geography_code": "ALL"},
        error_hint="Provide one active scenario_default row with module = ALL and geography_code = ALL.",
    )

    return {
        "model": {
            "phase": 6,
            "build_scope": PHASE6_BUILD_SCOPE,
            "upstream_value_contract": PHASE6_UPSTREAM_VALUE_CONTRACT,
        },
        "modules": {
            "enabled": list(PHASE1_MODULES),
            "disabled": list(PHASE6_DISABLED_CAPABILITIES),
        },
        "cost_basis": {
            "ds_standard_cost_basis_unit": scenario_default_trade_row["ds_standard_cost_basis_unit"],
            "ds_standard_cost_per_mg": float(scenario_default_trade_row["ds_standard_cost_per_mg"]),
            "dp_conversion_cost_per_unit": float(scenario_default_trade_row["dp_conversion_cost_per_unit"]),
            "fg_packaging_labeling_cost_per_unit": float(
                scenario_default_trade_row["fg_packaging_labeling_cost_per_unit"]
            ),
            "ss_standard_cost_per_unit": float(scenario_default_trade_row["ss_standard_cost_per_unit"]),
            "geography_fg_packaging_labeling_cost_overrides": {},
        },
        "carrying_cost": {
            "annual_inventory_carry_rate": float(scenario_default_trade_row["annual_inventory_carry_rate"]),
            "monthly_inventory_carry_rate": float(scenario_default_trade_row["monthly_inventory_carry_rate"]),
        },
        "expiry_writeoff": {
            "expired_inventory_writeoff_rate": float(
                scenario_default_trade_row["expired_inventory_writeoff_rate"]
            ),
            "expired_inventory_salvage_rate": float(
                scenario_default_trade_row["expired_inventory_salvage_rate"]
            ),
        },
        "valuation_policy": {
            "value_unmatched_fg_at_fg_standard_cost": _parse_stored_bool(
                scenario_default_trade_row["value_unmatched_fg_at_fg_standard_cost"]
            ),
            "include_trade_node_fg_value": _parse_stored_bool(
                scenario_default_trade_row["include_trade_node_fg_value"]
            ),
            "use_matched_administrable_fg_value": _parse_stored_bool(
                scenario_default_trade_row["use_matched_administrable_fg_value"]
            ),
        },
        "shipping_cold_chain": {
            "us_fg_sub1_to_sub2_cost_per_unit": float(
                scenario_default_trade_row["us_fg_sub1_to_sub2_cost_per_unit"]
            ),
            "eu_fg_sub1_to_sub2_cost_per_unit": float(
                scenario_default_trade_row["eu_fg_sub1_to_sub2_cost_per_unit"]
            ),
            "us_ss_sub1_to_sub2_cost_per_unit": float(
                scenario_default_trade_row["us_ss_sub1_to_sub2_cost_per_unit"]
            ),
            "eu_ss_sub1_to_sub2_cost_per_unit": float(
                scenario_default_trade_row["eu_ss_sub1_to_sub2_cost_per_unit"]
            ),
        },
        "conversion": {
            "dp_to_fg_yield": float(yield_default_row["dp_to_fg_yield"]),
            "ds_to_dp_yield": float(yield_default_row["ds_to_dp_yield"]),
            "ds_qty_per_dp_unit_mg": float(product_default_row["ds_qty_per_dp_unit_mg"]),
            "ds_overage_factor": float(yield_default_row["ds_overage_factor"]),
            "ss_ratio_to_fg": float(ss_default_row["ss_ratio_to_fg"]),
        },
        "validation": {
            "enforce_unique_output_keys": _parse_stored_bool(
                scenario_default_trade_row["phase6_enforce_unique_output_keys"]
            ),
            "reconciliation_tolerance_value": float(
                scenario_default_trade_row["phase6_reconciliation_tolerance_value"]
            ),
        },
        "wiring_notes": [
            "Trade_Inventory_FutureHooks scenario_default row feeds active deterministic Phase 6 cost_basis, carrying_cost, expiry_writeoff, valuation_policy, and validation parameters.",
            "Trade_Inventory_FutureHooks scenario_default row also feeds active deterministic Phase 6 geography-bucketed Sub-Layer 1 -> Sub-Layer 2 shipping/cold-chain cost parameters.",
            "Product_Parameters scenario_default row feeds conversion.ds_qty_per_dp_unit_mg in the active deterministic Phase 6 config.",
            "Yield_Assumptions scenario_default row feeds conversion.dp_to_fg_yield / conversion.ds_to_dp_yield / conversion.ds_overage_factor in the active deterministic Phase 6 config.",
            "SS_Assumptions scenario default row feeds conversion.ss_ratio_to_fg in the active deterministic Phase 6 config.",
        ],
    }


def _write_assumption_outputs(
    *,
    output_dir: Path,
    context: AssumptionsWorkbookContext,
    scenario_controls_rows: list[dict[str, str]],
    launch_timing_rows: list[dict[str, str]],
    dosing_rows: list[dict[str, str]],
    treatment_duration_rows: list[dict[str, str]],
    product_rows: list[dict[str, str]],
    yield_rows: list[dict[str, str]],
    packaging_rows: list[dict[str, str]],
    ss_rows: list[dict[str, str]],
    cml_prevalent_rows: list[dict[str, str]],
    trade_rows: list[dict[str, str]],
    resolved_phase2: dict[str, object],
    resolved_phase3: dict[str, object],
    resolved_phase4: dict[str, object],
    resolved_phase5: dict[str, object],
    resolved_phase6: dict[str, object],
    workbook_path: Path,
    warnings: list[str],
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_paths = {
        "scenario_controls": output_dir / "scenario_controls.csv",
        "launch_timing": output_dir / "launch_timing.csv",
        "dosing_assumptions": output_dir / "dosing_assumptions.csv",
        "treatment_duration_assumptions": output_dir / "treatment_duration_assumptions.csv",
        "product_parameters": output_dir / "product_parameters.csv",
        "yield_assumptions": output_dir / "yield_assumptions.csv",
        "packaging_and_vialing": output_dir / "packaging_and_vialing.csv",
        "ss_assumptions": output_dir / "ss_assumptions.csv",
        "cml_prevalent_assumptions": output_dir / "cml_prevalent_assumptions.csv",
        "trade_inventory_futurehooks": output_dir / "trade_inventory_futurehooks.csv",
        "resolved_phase2_snapshot": output_dir / "resolved_phase2_config_snapshot.json",
        "resolved_phase3_snapshot": output_dir / "resolved_phase3_config_snapshot.json",
        "resolved_phase4_snapshot": output_dir / "resolved_phase4_config_snapshot.json",
        "resolved_phase5_snapshot": output_dir / "resolved_phase5_config_snapshot.json",
        "resolved_phase6_snapshot": output_dir / "resolved_phase6_config_snapshot.json",
        "import_summary": output_dir / "assumptions_import_summary.json",
        "generated_phase2_parameters": output_dir / "generated_phase2_parameters.toml",
        "generated_phase2_scenario": output_dir / "generated_phase2_scenario.toml",
        "generated_phase3_parameters": output_dir / "generated_phase3_parameters.toml",
        "generated_phase3_scenario": output_dir / "generated_phase3_scenario.toml",
        "generated_phase4_parameters": output_dir / "generated_phase4_parameters.toml",
        "generated_phase4_scenario": output_dir / "generated_phase4_scenario.toml",
        "generated_phase5_parameters": output_dir / "generated_phase5_parameters.toml",
        "generated_phase5_scenario": output_dir / "generated_phase5_scenario.toml",
        "generated_phase6_parameters": output_dir / "generated_phase6_parameters.toml",
        "generated_phase6_scenario": output_dir / "generated_phase6_scenario.toml",
    }
    _write_csv(file_paths["scenario_controls"], SCENARIO_CONTROLS_HEADERS, scenario_controls_rows)
    _write_csv(file_paths["launch_timing"], LAUNCH_TIMING_HEADERS, launch_timing_rows)
    _write_csv(file_paths["dosing_assumptions"], DOSING_HEADERS, dosing_rows)
    _write_csv(
        file_paths["treatment_duration_assumptions"],
        TREATMENT_DURATION_HEADERS,
        treatment_duration_rows,
    )
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
    file_paths["resolved_phase3_snapshot"].write_text(
        json.dumps(
            {
                "scenario_name": context.scenario_name,
                "resolved_phase3": resolved_phase3,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    file_paths["resolved_phase4_snapshot"].write_text(
        json.dumps(
            {
                "scenario_name": context.scenario_name,
                "resolved_phase4": resolved_phase4,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    file_paths["resolved_phase5_snapshot"].write_text(
        json.dumps(
            {
                "scenario_name": context.scenario_name,
                "resolved_phase5": resolved_phase5,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    file_paths["resolved_phase6_snapshot"].write_text(
        json.dumps(
            {
                "scenario_name": context.scenario_name,
                "resolved_phase6": resolved_phase6,
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
    _write_generated_phase3_parameters(
        file_paths["generated_phase3_parameters"],
        resolved_phase3,
    )
    _write_generated_phase3_scenario(
        output_dir=output_dir,
        scenario_name=context.scenario_name,
        parameter_config_path=file_paths["generated_phase3_parameters"],
        scenario_output_path=file_paths["generated_phase3_scenario"],
    )
    _write_generated_phase4_parameters(
        file_paths["generated_phase4_parameters"],
        resolved_phase4,
    )
    _write_generated_phase4_scenario(
        output_dir=output_dir,
        scenario_name=context.scenario_name,
        parameter_config_path=file_paths["generated_phase4_parameters"],
        scenario_output_path=file_paths["generated_phase4_scenario"],
    )
    _write_generated_phase5_parameters(
        file_paths["generated_phase5_parameters"],
        resolved_phase5,
    )
    _write_generated_phase5_scenario(
        output_dir=output_dir,
        scenario_name=context.scenario_name,
        parameter_config_path=file_paths["generated_phase5_parameters"],
        scenario_output_path=file_paths["generated_phase5_scenario"],
    )
    _write_generated_phase6_parameters(
        file_paths["generated_phase6_parameters"],
        resolved_phase6,
    )
    _write_generated_phase6_scenario(
        output_dir=output_dir,
        scenario_name=context.scenario_name,
        parameter_config_path=file_paths["generated_phase6_parameters"],
        scenario_output_path=file_paths["generated_phase6_scenario"],
    )

    summary_payload = {
        "workbook_path": str(workbook_path),
        "output_dir": str(output_dir),
        "scenario_name": context.scenario_name,
        "forecast_grain": context.forecast_grain,
        "forecast_frequency": context.forecast_frequency,
        "demand_basis": context.demand_basis,
        "dose_basis_default": context.dose_basis_default,
        "generated_files": {name: str(path) for name, path in file_paths.items()},
        "row_counts": {
            "scenario_controls": len(scenario_controls_rows),
            "launch_timing": len(launch_timing_rows),
            "dosing_assumptions": len(dosing_rows),
            "treatment_duration_assumptions": len(treatment_duration_rows),
            "product_parameters": len(product_rows),
            "yield_assumptions": len(yield_rows),
            "packaging_and_vialing": len(packaging_rows),
            "ss_assumptions": len(ss_rows),
            "cml_prevalent_assumptions": len(cml_prevalent_rows),
            "trade_inventory_futurehooks": len(trade_rows),
        },
        "wired_into_current_engine": [
            "Scenario_Controls.demand_basis and Treatment_Duration_Assumptions -> Phase 1 starts-based treated census build when demand_basis=patient_starts.",
            "Scenario_Controls.dose_basis_default -> model.dose_basis",
            "Dosing_Assumptions module rows -> module_settings.<module> fixed_dose_mg / weight_based_dose_mg_per_kg / average_patient_weight_kg / doses_per_patient_per_month",
            "Product_Parameters scenario_default + module_override -> fg_mg_per_unit resolution",
            "Packaging_and_Vialing module rows -> module_settings.<module>.fg_vialing_rule",
            "Yield_Assumptions scenario_default row -> yield.plan.* and ds.overage_factor",
            "Product_Parameters scenario_default row -> ds.qty_per_dp_unit_mg",
            "SS_Assumptions scenario default row -> ss.ratio_to_fg and model.co_pack_mode",
            "Trade_Inventory_FutureHooks scenario_default / geography_default / launch_event rows -> active deterministic Phase 3 config generation",
            "Trade_Inventory_FutureHooks scenario_default row plus Product_Parameters / Yield_Assumptions / SS_Assumptions scenario defaults -> active deterministic Phase 4 config generation",
            "Trade_Inventory_FutureHooks scenario_default row plus Product_Parameters / Yield_Assumptions / SS_Assumptions scenario defaults -> active deterministic Phase 5 config generation",
            "Trade_Inventory_FutureHooks scenario_default row plus Product_Parameters / Yield_Assumptions / SS_Assumptions scenario defaults -> active deterministic Phase 6 config generation",
        ],
        "future_ready_only": [
            "Launch_Timing normalized only; not yet wired into active engine logic.",
            "CML_Prevalent_Assumptions normalized only; not yet wired into active Phase 2 config generation.",
            "Product_Parameters module_override ds_qty_per_dp_unit_mg rows are preserved but not yet consumed by the current engine.",
            "Yield_Assumptions module_override ds_overage_factor rows are preserved but not yet consumed by the current engine.",
            "dp_concentration_mg_per_ml and dp_fill_volume_ml are preserved but not yet consumed by the current engine.",
            "Broader future-phase revenue, Monte Carlo, and optimization logic remains deferred even though the assumptions workbook now feeds the active deterministic Phase 3, Phase 4, Phase 5, and Phase 6 configs.",
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


def _write_generated_phase3_parameters(path: Path, resolved_phase3: dict[str, object]) -> None:
    model = resolved_phase3["model"]
    modules = resolved_phase3["modules"]
    trade = resolved_phase3["trade"]
    geography_defaults = resolved_phase3["geography_defaults"]
    launch_events = resolved_phase3["launch_events"]
    validation = resolved_phase3["validation"]

    lines = [
        "# GENERATED BY scripts/assumptions_import.py",
        "# Business-facing assumptions workbook bridge into the current Phase 3 config path.",
        "",
        "[model]",
        f'phase = {model["phase"]}',
        f'build_scope = "{model["build_scope"]}"',
        f'upstream_demand_contract = "{model["upstream_demand_contract"]}"',
        "",
        "[modules]",
        f'enabled = {_toml_list(modules["enabled"])}',
        f'disabled = {_toml_list(modules["disabled"])}',
        "",
        "[trade]",
        f'sublayer1_target_weeks_on_hand = {trade["sublayer1_target_weeks_on_hand"]}',
        f'sublayer2_target_weeks_on_hand = {trade["sublayer2_target_weeks_on_hand"]}',
        f'sublayer2_wastage_rate = {trade["sublayer2_wastage_rate"]}',
        f'initial_stocking_units_per_new_site = {trade["initial_stocking_units_per_new_site"]}',
        f'ss_units_per_new_site = {trade["ss_units_per_new_site"]}',
        f'sublayer1_launch_fill_months_of_demand = {trade["sublayer1_launch_fill_months_of_demand"]}',
        f'rems_certification_lag_weeks = {trade["rems_certification_lag_weeks"]}',
        f'january_softening_enabled = {_toml_bool(trade["january_softening_enabled"])}',
        f'january_softening_factor = {trade["january_softening_factor"]}',
        f'bullwhip_flag_threshold = {trade["bullwhip_flag_threshold"]}',
        f'channel_fill_start_prelaunch_weeks = {trade["channel_fill_start_prelaunch_weeks"]}',
        f'sublayer2_fill_distribution_weeks = {trade["sublayer2_fill_distribution_weeks"]}',
        f'weeks_per_month = {trade["weeks_per_month"]}',
        "",
    ]
    for geography_code, geography_values in geography_defaults.items():
        lines.extend(
            [
                f"[geography_defaults.{geography_code}]",
                f'site_activation_rate = {geography_values["site_activation_rate"]}',
                f'certified_sites_at_launch = {geography_values["certified_sites_at_launch"]}',
                f'certified_sites_at_peak = {geography_values["certified_sites_at_peak"]}',
                "",
            ]
        )
    for module in PHASE1_MODULES:
        module_launch_events = launch_events[module]
        for geography_code, event_values in module_launch_events.items():
            lines.extend(
                [
                    f"[launch_events.{module}.{geography_code}]",
                    f'launch_month_index = {event_values["launch_month_index"]}',
                    "",
                ]
            )
    lines.extend(
        [
            "[validation]",
            f'enforce_unique_output_keys = {_toml_bool(validation["enforce_unique_output_keys"])}',
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_generated_phase3_scenario(
    *,
    output_dir: Path,
    scenario_name: str,
    parameter_config_path: Path,
    scenario_output_path: Path,
) -> None:
    default_phase2_output = output_dir.parent / "phase2_deterministic_cascade.csv"
    deterministic_trade_output = output_dir.parent / "phase3_trade_layer.csv"
    parameter_ref = _relative_path_for_toml(parameter_config_path, start=output_dir)
    input_ref = _relative_path_for_toml(default_phase2_output, start=output_dir)
    output_ref = _relative_path_for_toml(deterministic_trade_output, start=output_dir)
    scenario_output_path.write_text(
        "\n".join(
            [
                "# GENERATED BY scripts/assumptions_import.py",
                "# Use directly with scripts/run_phase3.py or let scripts/run_forecast_workflow.py consume it.",
                f'scenario_name = "{scenario_name}"',
                f'parameter_config = "{parameter_ref}"',
                "",
                "[inputs]",
                f'phase2_deterministic_cascade = "{input_ref}"',
                "",
                "[outputs]",
                f'deterministic_trade_layer = "{output_ref}"',
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_generated_phase4_parameters(path: Path, resolved_phase4: dict[str, object]) -> None:
    model = resolved_phase4["model"]
    modules = resolved_phase4["modules"]
    conversion = resolved_phase4["conversion"]
    review = resolved_phase4["review"]
    stepdown = resolved_phase4["stepdown"]
    fg = resolved_phase4["fg"]
    dp = resolved_phase4["dp"]
    ds = resolved_phase4["ds"]
    ss = resolved_phase4["ss"]
    validation = resolved_phase4["validation"]

    lines = [
        "# GENERATED BY scripts/assumptions_import.py",
        "# Business-facing assumptions workbook bridge into the current Phase 4 config path.",
        "",
        "[model]",
        f'phase = {model["phase"]}',
        f'build_scope = "{model["build_scope"]}"',
        f'upstream_demand_contract = "{model["upstream_demand_contract"]}"',
        "",
        "[modules]",
        f'enabled = {_toml_list(modules["enabled"])}',
        f'disabled = {_toml_list(modules["disabled"])}',
        "",
        "[conversion]",
        f'dp_to_fg_yield = {conversion["dp_to_fg_yield"]}',
        f'ds_to_dp_yield = {conversion["ds_to_dp_yield"]}',
        f'ds_qty_per_dp_unit_mg = {conversion["ds_qty_per_dp_unit_mg"]}',
        f'ds_overage_factor = {conversion["ds_overage_factor"]}',
        f'ss_ratio_to_fg = {conversion["ss_ratio_to_fg"]}',
        f'weeks_per_month = {conversion["weeks_per_month"]}',
        "",
        "[review]",
        f'bullwhip_amplification_threshold = {review["bullwhip_amplification_threshold"]}',
        f'bullwhip_review_window_months = {review["bullwhip_review_window_months"]}',
        f'excess_build_threshold_ratio = {review["excess_build_threshold_ratio"]}',
        f'supply_gap_tolerance_units = {review["supply_gap_tolerance_units"]}',
        f'capacity_clip_tolerance_units = {review["capacity_clip_tolerance_units"]}',
        "",
        "[stepdown]",
        f'cml_prevalent_forward_window_months = {stepdown["cml_prevalent_forward_window_months"]}',
        f'projected_cml_prevalent_bolus_exhaustion_month_index = {stepdown["projected_cml_prevalent_bolus_exhaustion_month_index"]}',
        "",
        "[fg]",
        f'lead_time_from_dp_release_weeks = {fg["lead_time_from_dp_release_weeks"]}',
        f'packaging_cycle_weeks = {fg["packaging_cycle_weeks"]}',
        f'release_qa_weeks = {fg["release_qa_weeks"]}',
        f'total_order_to_release_weeks = {fg["total_order_to_release_weeks"]}',
        f'packaging_campaign_size_units = {fg["packaging_campaign_size_units"]}',
        "",
        "[dp]",
        f'lead_time_from_ds_release_weeks = {dp["lead_time_from_ds_release_weeks"]}',
        f'manufacturing_cycle_weeks = {dp["manufacturing_cycle_weeks"]}',
        f'release_testing_weeks = {dp["release_testing_weeks"]}',
        f'total_order_to_release_weeks = {dp["total_order_to_release_weeks"]}',
        f'min_batch_size_units = {dp["min_batch_size_units"]}',
        f'max_batch_size_units = {dp["max_batch_size_units"]}',
        f'min_campaign_batches = {dp["min_campaign_batches"]}',
        f'annual_capacity_batches = {dp["annual_capacity_batches"]}',
        "",
        "[ds]",
        f'lead_time_to_batch_start_planning_horizon_weeks = {ds["lead_time_to_batch_start_planning_horizon_weeks"]}',
        f'manufacturing_cycle_weeks = {ds["manufacturing_cycle_weeks"]}',
        f'release_testing_weeks = {ds["release_testing_weeks"]}',
        f'total_order_to_release_weeks = {ds["total_order_to_release_weeks"]}',
        f'min_batch_size_kg = {ds["min_batch_size_kg"]}',
        f'max_batch_size_kg = {ds["max_batch_size_kg"]}',
        f'min_campaign_batches = {ds["min_campaign_batches"]}',
        f'annual_capacity_batches = {ds["annual_capacity_batches"]}',
        "",
        "[ss]",
        f'order_to_release_lead_time_weeks = {ss["order_to_release_lead_time_weeks"]}',
        f'batch_size_units = {ss["batch_size_units"]}',
        f'min_campaign_batches = {ss["min_campaign_batches"]}',
        f'annual_capacity_batches = {ss["annual_capacity_batches"]}',
        f'release_must_coincide_with_or_precede_fg = {_toml_bool(ss["release_must_coincide_with_or_precede_fg"])}',
        "",
        "[validation]",
        f'enforce_unique_output_keys = {_toml_bool(validation["enforce_unique_output_keys"])}',
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_generated_phase4_scenario(
    *,
    output_dir: Path,
    scenario_name: str,
    parameter_config_path: Path,
    scenario_output_path: Path,
) -> None:
    default_phase3_output = output_dir.parent / "phase3_trade_layer.csv"
    schedule_detail_output = output_dir.parent / "phase4_schedule_detail.csv"
    monthly_summary_output = output_dir.parent / "phase4_monthly_summary.csv"
    parameter_ref = _relative_path_for_toml(parameter_config_path, start=output_dir)
    input_ref = _relative_path_for_toml(default_phase3_output, start=output_dir)
    detail_ref = _relative_path_for_toml(schedule_detail_output, start=output_dir)
    summary_ref = _relative_path_for_toml(monthly_summary_output, start=output_dir)
    scenario_output_path.write_text(
        "\n".join(
            [
                "# GENERATED BY scripts/assumptions_import.py",
                "# Use directly with scripts/run_phase4.py or let scripts/run_forecast_workflow.py consume it.",
                f'scenario_name = "{scenario_name}"',
                f'parameter_config = "{parameter_ref}"',
                "",
                "[inputs]",
                f'phase3_trade_layer = "{input_ref}"',
                "",
                "[outputs]",
                f'schedule_detail = "{detail_ref}"',
                f'monthly_summary = "{summary_ref}"',
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_generated_phase5_parameters(path: Path, resolved_phase5: dict[str, object]) -> None:
    model = resolved_phase5["model"]
    modules = resolved_phase5["modules"]
    starting_inventory = resolved_phase5["starting_inventory"]
    shelf_life = resolved_phase5["shelf_life"]
    policy = resolved_phase5["policy"]
    conversion = resolved_phase5["conversion"]
    validation = resolved_phase5["validation"]

    lines = [
        "# GENERATED BY scripts/assumptions_import.py",
        "# Business-facing assumptions workbook bridge into the current Phase 5 config path.",
        "",
        "[model]",
        f'phase = {model["phase"]}',
        f'build_scope = "{model["build_scope"]}"',
        f'upstream_supply_contract = "{model["upstream_supply_contract"]}"',
        "",
        "[modules]",
        f'enabled = {_toml_list(modules["enabled"])}',
        f'disabled = {_toml_list(modules["disabled"])}',
        "",
        "[starting_inventory]",
        f'ds_mg = {starting_inventory["ds_mg"]}',
        f'dp_units = {starting_inventory["dp_units"]}',
        f'fg_units = {starting_inventory["fg_units"]}',
        f'ss_units = {starting_inventory["ss_units"]}',
        f'sublayer1_fg_units = {starting_inventory["sublayer1_fg_units"]}',
        f'sublayer2_fg_units = {starting_inventory["sublayer2_fg_units"]}',
        "",
        "[shelf_life]",
        f'ds_months = {shelf_life["ds_months"]}',
        f'dp_months = {shelf_life["dp_months"]}',
        f'fg_months = {shelf_life["fg_months"]}',
        f'ss_months = {shelf_life["ss_months"]}',
        "",
        "[policy]",
        f'excess_inventory_threshold_months_of_cover = {policy["excess_inventory_threshold_months_of_cover"]}',
        f'stockout_tolerance_units = {policy["stockout_tolerance_units"]}',
        f'fefo_enabled = {_toml_bool(policy["fefo_enabled"])}',
        f'ss_fg_match_required = {_toml_bool(policy["ss_fg_match_required"])}',
        f'allow_prelaunch_inventory_build = {_toml_bool(policy["allow_prelaunch_inventory_build"])}',
        "",
        "[conversion]",
        f'dp_to_fg_yield = {conversion["dp_to_fg_yield"]}',
        f'ds_to_dp_yield = {conversion["ds_to_dp_yield"]}',
        f'ds_qty_per_dp_unit_mg = {conversion["ds_qty_per_dp_unit_mg"]}',
        f'ds_overage_factor = {conversion["ds_overage_factor"]}',
        f'ss_ratio_to_fg = {conversion["ss_ratio_to_fg"]}',
        "",
        "[validation]",
        f'enforce_unique_output_keys = {_toml_bool(validation["enforce_unique_output_keys"])}',
        f'reconcile_phase4_receipts = {_toml_bool(validation["reconcile_phase4_receipts"])}',
        f'reconciliation_tolerance_units = {validation["reconciliation_tolerance_units"]}',
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_generated_phase5_scenario(
    *,
    output_dir: Path,
    scenario_name: str,
    parameter_config_path: Path,
    scenario_output_path: Path,
) -> None:
    default_phase3_output = output_dir.parent / "phase3_trade_layer.csv"
    default_phase4_detail = output_dir.parent / "phase4_schedule_detail.csv"
    default_phase4_summary = output_dir.parent / "phase4_monthly_summary.csv"
    inventory_detail_output = output_dir.parent / "phase5_inventory_detail.csv"
    inventory_summary_output = output_dir.parent / "phase5_monthly_inventory_summary.csv"
    cohort_audit_output = output_dir.parent / "phase5_inventory_cohort_audit.csv"
    parameter_ref = _relative_path_for_toml(parameter_config_path, start=output_dir)
    phase3_ref = _relative_path_for_toml(default_phase3_output, start=output_dir)
    phase4_detail_ref = _relative_path_for_toml(default_phase4_detail, start=output_dir)
    phase4_summary_ref = _relative_path_for_toml(default_phase4_summary, start=output_dir)
    detail_ref = _relative_path_for_toml(inventory_detail_output, start=output_dir)
    summary_ref = _relative_path_for_toml(inventory_summary_output, start=output_dir)
    cohort_ref = _relative_path_for_toml(cohort_audit_output, start=output_dir)
    scenario_output_path.write_text(
        "\n".join(
            [
                "# GENERATED BY scripts/assumptions_import.py",
                "# Use directly with scripts/run_phase5.py or let scripts/run_forecast_workflow.py consume it.",
                f'scenario_name = "{scenario_name}"',
                f'parameter_config = "{parameter_ref}"',
                "",
                "[inputs]",
                f'phase3_trade_layer = "{phase3_ref}"',
                f'phase4_schedule_detail = "{phase4_detail_ref}"',
                f'phase4_monthly_summary = "{phase4_summary_ref}"',
                "",
                "[outputs]",
                f'inventory_detail = "{detail_ref}"',
                f'monthly_inventory_summary = "{summary_ref}"',
                f'cohort_audit = "{cohort_ref}"',
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_generated_phase6_parameters(path: Path, resolved_phase6: dict[str, object]) -> None:
    model = resolved_phase6["model"]
    modules = resolved_phase6["modules"]
    cost_basis = resolved_phase6["cost_basis"]
    carrying_cost = resolved_phase6["carrying_cost"]
    expiry_writeoff = resolved_phase6["expiry_writeoff"]
    valuation_policy = resolved_phase6["valuation_policy"]
    shipping_cold_chain = resolved_phase6["shipping_cold_chain"]
    conversion = resolved_phase6["conversion"]
    validation = resolved_phase6["validation"]
    geography_overrides = cost_basis.get("geography_fg_packaging_labeling_cost_overrides", {})

    lines = [
        "# GENERATED BY scripts/assumptions_import.py",
        "# Business-facing assumptions workbook bridge into the current Phase 6 config path.",
        "",
        "[model]",
        f'phase = {model["phase"]}',
        f'build_scope = "{model["build_scope"]}"',
        f'upstream_value_contract = "{model["upstream_value_contract"]}"',
        "",
        "[modules]",
        f'enabled = {_toml_list(modules["enabled"])}',
        f'disabled = {_toml_list(modules["disabled"])}',
        "",
        "[cost_basis]",
        f'ds_standard_cost_basis_unit = "{cost_basis["ds_standard_cost_basis_unit"]}"',
        f'ds_standard_cost_per_mg = {cost_basis["ds_standard_cost_per_mg"]}',
        f'dp_conversion_cost_per_unit = {cost_basis["dp_conversion_cost_per_unit"]}',
        f'fg_packaging_labeling_cost_per_unit = {cost_basis["fg_packaging_labeling_cost_per_unit"]}',
        f'ss_standard_cost_per_unit = {cost_basis["ss_standard_cost_per_unit"]}',
        "",
    ]
    if geography_overrides:
        lines.append("[geography_fg_packaging_labeling_cost_overrides]")
        for geography_code, value in geography_overrides.items():
            lines.append(f'{geography_code} = {value}')
        lines.append("")
    lines.extend(
        [
            "[carrying_cost]",
            f'annual_inventory_carry_rate = {carrying_cost["annual_inventory_carry_rate"]}',
            f'monthly_inventory_carry_rate = {carrying_cost["monthly_inventory_carry_rate"]}',
            "",
            "[expiry_writeoff]",
            f'expired_inventory_writeoff_rate = {expiry_writeoff["expired_inventory_writeoff_rate"]}',
            f'expired_inventory_salvage_rate = {expiry_writeoff["expired_inventory_salvage_rate"]}',
            "",
            "[valuation_policy]",
            f'value_unmatched_fg_at_fg_standard_cost = {_toml_bool(valuation_policy["value_unmatched_fg_at_fg_standard_cost"])}',
            f'include_trade_node_fg_value = {_toml_bool(valuation_policy["include_trade_node_fg_value"])}',
            f'use_matched_administrable_fg_value = {_toml_bool(valuation_policy["use_matched_administrable_fg_value"])}',
            "",
            "[shipping_cold_chain]",
            f'us_fg_sub1_to_sub2_cost_per_unit = {shipping_cold_chain["us_fg_sub1_to_sub2_cost_per_unit"]}',
            f'eu_fg_sub1_to_sub2_cost_per_unit = {shipping_cold_chain["eu_fg_sub1_to_sub2_cost_per_unit"]}',
            f'us_ss_sub1_to_sub2_cost_per_unit = {shipping_cold_chain["us_ss_sub1_to_sub2_cost_per_unit"]}',
            f'eu_ss_sub1_to_sub2_cost_per_unit = {shipping_cold_chain["eu_ss_sub1_to_sub2_cost_per_unit"]}',
            "",
            "[conversion]",
            f'dp_to_fg_yield = {conversion["dp_to_fg_yield"]}',
            f'ds_to_dp_yield = {conversion["ds_to_dp_yield"]}',
            f'ds_qty_per_dp_unit_mg = {conversion["ds_qty_per_dp_unit_mg"]}',
            f'ds_overage_factor = {conversion["ds_overage_factor"]}',
            f'ss_ratio_to_fg = {conversion["ss_ratio_to_fg"]}',
            "",
            "[validation]",
            f'enforce_unique_output_keys = {_toml_bool(validation["enforce_unique_output_keys"])}',
            f'reconciliation_tolerance_value = {validation["reconciliation_tolerance_value"]}',
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_generated_phase6_scenario(
    *,
    output_dir: Path,
    scenario_name: str,
    parameter_config_path: Path,
    scenario_output_path: Path,
) -> None:
    default_phase4_summary = output_dir.parent / "phase4_monthly_summary.csv"
    default_phase5_detail = output_dir.parent / "phase5_inventory_detail.csv"
    default_phase5_summary = output_dir.parent / "phase5_monthly_summary.csv"
    financial_detail_output = output_dir.parent / "phase6_financial_detail.csv"
    monthly_summary_output = output_dir.parent / "phase6_monthly_financial_summary.csv"
    annual_summary_output = output_dir.parent / "phase6_annual_financial_summary.csv"
    parameter_ref = _relative_path_for_toml(parameter_config_path, start=output_dir)
    phase4_ref = _relative_path_for_toml(default_phase4_summary, start=output_dir)
    phase5_detail_ref = _relative_path_for_toml(default_phase5_detail, start=output_dir)
    phase5_summary_ref = _relative_path_for_toml(default_phase5_summary, start=output_dir)
    detail_ref = _relative_path_for_toml(financial_detail_output, start=output_dir)
    monthly_ref = _relative_path_for_toml(monthly_summary_output, start=output_dir)
    annual_ref = _relative_path_for_toml(annual_summary_output, start=output_dir)
    scenario_output_path.write_text(
        "\n".join(
            [
                "# GENERATED BY scripts/assumptions_import.py",
                "# Use directly with scripts/run_phase6.py or let scripts/run_forecast_workflow.py consume it.",
                f'scenario_name = "{scenario_name}"',
                f'parameter_config = "{parameter_ref}"',
                "",
                "[inputs]",
                f'phase4_monthly_summary = "{phase4_ref}"',
                f'phase5_inventory_detail = "{phase5_detail_ref}"',
                f'phase5_monthly_inventory_summary = "{phase5_summary_ref}"',
                "",
                "[outputs]",
                f'financial_detail = "{detail_ref}"',
                f'monthly_financial_summary = "{monthly_ref}"',
                f'annual_financial_summary = "{annual_ref}"',
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
