"""Generate the business-facing CBX250 model assumptions workbook template."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import zipfile

from ..constants import PHASE1_MODULES
from .excel_template import (
    CALCULATED_STYLE_ID,
    EDITABLE_DATE_STYLE_ID,
    EDITABLE_STYLE_ID,
    EDITABLE_WRAP_STYLE_ID,
    LABEL_STYLE_ID,
    WRAP_STYLE_ID,
    CellSpec,
    DataValidationSpec,
    SheetSpec,
    _blank_input_rows,
    _build_app_props_xml,
    _build_content_types_xml,
    _build_core_props_xml,
    _build_root_relationships_xml,
    _build_styles_xml,
    _build_workbook_relationships_xml,
    _build_workbook_xml,
    _build_worksheet_xml,
    _header_row,
)

SCENARIO_CONTROL_TEMPLATE_ROWS = 10
LAUNCH_TIMING_TEMPLATE_ROWS = 40
DOSING_TEMPLATE_ROWS = 80
PRODUCT_PARAMETER_TEMPLATE_ROWS = 40
YIELD_TEMPLATE_ROWS = 40
PACKAGING_TEMPLATE_ROWS = 40
SS_TEMPLATE_ROWS = 20
CML_PREVALENT_TEMPLATE_ROWS = 40
TRADE_FUTURE_TEMPLATE_ROWS = 20


def build_model_assumptions_template(output_path: Path) -> Path:
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sheets = _build_sheets()

    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", _build_content_types_xml(len(sheets)))
        workbook.writestr("_rels/.rels", _build_root_relationships_xml())
        workbook.writestr("docProps/core.xml", _build_core_props_xml())
        workbook.writestr("docProps/app.xml", _build_app_props_xml(sheets))
        workbook.writestr("xl/workbook.xml", _build_workbook_xml(sheets))
        workbook.writestr("xl/_rels/workbook.xml.rels", _build_workbook_relationships_xml(len(sheets)))
        workbook.writestr("xl/styles.xml", _build_styles_xml())
        for sheet_index, sheet in enumerate(sheets, start=1):
            workbook.writestr(f"xl/worksheets/sheet{sheet_index}.xml", _build_worksheet_xml(sheet))

    return output_path


def _build_sheets() -> tuple[SheetSpec, ...]:
    return (
        _build_instructions_sheet(),
        _build_scenario_controls_sheet(),
        _build_launch_timing_sheet(),
        _build_dosing_assumptions_sheet(),
        _build_product_parameters_sheet(),
        _build_yield_assumptions_sheet(),
        _build_packaging_and_vialing_sheet(),
        _build_ss_assumptions_sheet(),
        _build_cml_prevalent_assumptions_sheet(),
        _build_trade_inventory_future_hooks_sheet(),
        _build_lookup_lists_sheet(),
    )


def _build_instructions_sheet() -> SheetSpec:
    rows = [
        _header_row(("Section", "Guidance", "Details")),
        (
            CellSpec("Workbook purpose", LABEL_STYLE_ID),
            CellSpec(
                "Use this workbook as the business-facing assumptions entry point for the CBX-250 model.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "Users should edit Excel assumptions here rather than hand-editing TOML files. The importer converts these rows into normalized artifacts plus a generated Phase 2 parameter config.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Scope", LABEL_STYLE_ID),
            CellSpec(
                "Current scope covers Phase 1 and Phase 2 relevant assumptions only.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "Trade, inventory, production scheduling, financials, Monte Carlo, and UI remain later-phase work. FutureHooks rows are preserved but not wired into active logic.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Required entry pattern", LABEL_STYLE_ID),
            CellSpec(
                "Fill Scenario_Controls first, then the active assumption sheets that apply to your scenario.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "One active Scenario_Controls row is required. Dosing_Assumptions should provide one active module row for AML, MDS, CML_Incident, and CML_Prevalent.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Scope precedence", LABEL_STYLE_ID),
            CellSpec(
                "When a sheet supports both scenario-level defaults and module-specific overrides, module-specific override rows take precedence over scenario-default rows.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "Current generated Phase 2 config already applies this precedence for module FG mg per unit. For ds_qty_per_dp_unit_mg and ds_overage_factor, module overrides are preserved in normalized artifacts but the current engine still consumes the scenario default only.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Current engine wiring", LABEL_STYLE_ID),
            CellSpec(
                "The importer generates machine-readable CSV artifacts plus generated_phase2_parameters.toml and generated_phase2_scenario.toml.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "Current Phase 2 wiring consumes: dose_basis_default, module-specific dosing values, module FG mg per unit, module FG vialing rule, global yields, DS quantity per DP unit default, DS overage default, SS ratio, and co_pack_mode.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Future-ready fields", LABEL_STYLE_ID),
            CellSpec(
                "Launch_Timing, geography-specific overrides, segment-specific overrides, dp_concentration_mg_per_ml, dp_fill_volume_ml, and Trade_Inventory_FutureHooks are preserved in normalized outputs even if not yet wired into active model logic.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "CML_Prevalent_Assumptions remains a dedicated artifact sheet. Populate approved pool, timing, and exhaustion metadata here without treating CML like AML/MDS segment mix logic.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Workbook to model bridge", LABEL_STYLE_ID),
            CellSpec(
                "After import, use the generated Phase 2 scenario directly or pass it into the one-command forecast workflow as --phase2-scenario.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "This keeps the current architecture intact while removing the need to edit TOML by hand for the active deterministic assumptions.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Approved base-case defaults", LABEL_STYLE_ID),
            CellSpec(
                "Seeded defaults reflect the current approved Phase 2 base case: fixed dose 0.15 mg, weight-based 0.0023 mg/kg, deterministic average weight 80 kg, AML/MDS 4.33 doses per month, CML modules 1.00 dose per month, fg_mg_per_unit 1.0 mg, DS quantity per DP unit 1.0 mg, ds_to_dp_yield 0.90, dp_to_fg_yield 0.98, ds_overage_factor 0.05, ss_ratio_to_fg 1.0.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "Where the approved contract is still unresolved, rows are labeled PLACEHOLDER rather than silently inventing a final business assumption.",
                WRAP_STYLE_ID,
            ),
        ),
    ]
    return SheetSpec(
        name="Instructions",
        rows=tuple(rows),
        column_widths=(28, 52, 96),
        freeze_cell="A2",
        auto_filter_ref="A1:C1",
    )


def _build_scenario_controls_sheet() -> SheetSpec:
    headers = (
        "scenario_name",
        "scenario_description",
        "active_flag",
        "forecast_grain",
        "forecast_frequency",
        "dose_basis_default",
        "base_currency",
        "notes",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    rows.append(
        (
            CellSpec("BASE_2029", EDITABLE_STYLE_ID),
            CellSpec("Approved base-case assumptions bridge", EDITABLE_WRAP_STYLE_ID),
            CellSpec("yes", EDITABLE_STYLE_ID),
            CellSpec("module_level", EDITABLE_STYLE_ID),
            CellSpec("monthly", EDITABLE_STYLE_ID),
            CellSpec("fixed", EDITABLE_STYLE_ID),
            CellSpec("USD", EDITABLE_STYLE_ID),
            CellSpec("Edit this row instead of hand-editing Phase 2 TOML files.", EDITABLE_WRAP_STYLE_ID),
        )
    )
    rows.extend(
        _blank_input_rows(
            headers=len(headers),
            count=SCENARIO_CONTROL_TEMPLATE_ROWS - 1,
            style_map=(
                EDITABLE_STYLE_ID,
                EDITABLE_WRAP_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_WRAP_STYLE_ID,
            ),
        )
    )
    return SheetSpec(
        name="Scenario_Controls",
        rows=tuple(rows),
        column_widths=(20, 40, 12, 18, 18, 18, 14, 56),
        freeze_cell="A2",
        auto_filter_ref="A1:H1",
        data_validations=(
            DataValidationSpec("C2:C11", "list", "Lookup_Lists!$I$2:$I$3"),
            DataValidationSpec("D2:D11", "list", "Lookup_Lists!$J$2:$J$3"),
            DataValidationSpec("E2:E11", "list", "Lookup_Lists!$K$2:$K$3"),
            DataValidationSpec("F2:F11", "list", "Lookup_Lists!$C$2:$C$3"),
        ),
    )


def _build_launch_timing_sheet() -> SheetSpec:
    headers = (
        "scenario_name",
        "module",
        "geography_code",
        "initial_approval_date",
        "launch_offset_months",
        "active_flag",
        "notes",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_rows = (
        ("AML", "ALL", date(2029, 1, 1), 0, "yes", "PLACEHOLDER shared launch timing until module-specific approval dates are approved."),
        ("MDS", "ALL", date(2029, 1, 1), 0, "yes", "PLACEHOLDER shared launch timing until module-specific approval dates are approved."),
        ("CML_Incident", "ALL", date(2029, 1, 1), 0, "no", "PLACEHOLDER example row. Activate only when approved CML launch timing is available."),
        ("CML_Prevalent", "ALL", date(2029, 1, 1), 0, "no", "PLACEHOLDER example row. Activate only when approved CML launch timing is available."),
    )
    for module, geography_code, approval_date, offset_months, active_flag, notes in example_rows:
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula='IF(Scenario_Controls!$A$2="","",Scenario_Controls!$A$2)'),
                CellSpec(module, EDITABLE_STYLE_ID),
                CellSpec(geography_code, EDITABLE_STYLE_ID),
                CellSpec(approval_date, EDITABLE_DATE_STYLE_ID),
                CellSpec(offset_months, EDITABLE_STYLE_ID),
                CellSpec(active_flag, EDITABLE_STYLE_ID),
                CellSpec(notes, EDITABLE_WRAP_STYLE_ID),
            )
        )
    rows.extend(
        _blank_input_rows(
            headers=len(headers),
            count=LAUNCH_TIMING_TEMPLATE_ROWS - len(example_rows),
            style_map=(
                CALCULATED_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_DATE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_WRAP_STYLE_ID,
            ),
        )
    )
    return SheetSpec(
        name="Launch_Timing",
        rows=tuple(rows),
        column_widths=(18, 20, 18, 20, 20, 12, 72),
        freeze_cell="A2",
        auto_filter_ref="A1:G1",
        data_validations=(
            DataValidationSpec("B2:B41", "list", "Lookup_Lists!$A$2:$A$5"),
            DataValidationSpec("F2:F41", "list", "Lookup_Lists!$I$2:$I$3"),
        ),
    )


def _build_dosing_assumptions_sheet() -> SheetSpec:
    headers = (
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
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_rows = (
        ("AML", "ALL", "ALL", "fixed", 0.15, 0.0023, 80.0, 4.33, "false", "PLACEHOLDER_INACTIVE", "false", 0.0, 1.0, 0.0, "yes", "Approved AML base-case cadence QW -> 4.33."),
        ("MDS", "ALL", "ALL", "fixed", 0.15, 0.0023, 80.0, 4.33, "false", "PLACEHOLDER_INACTIVE", "false", 0.0, 1.0, 0.0, "yes", "Approved MDS base-case cadence QW -> 4.33."),
        ("CML_Incident", "ALL", "ALL", "fixed", 0.15, 0.0023, 80.0, 1.0, "false", "PLACEHOLDER_INACTIVE", "false", 0.0, 1.0, 0.0, "yes", "Approved CML_Incident cadence Q4W -> 1.00."),
        ("CML_Prevalent", "ALL", "ALL", "fixed", 0.15, 0.0023, 80.0, 1.0, "false", "PLACEHOLDER_INACTIVE", "false", 0.0, 1.0, 0.0, "yes", "Approved CML_Prevalent cadence Q4W -> 1.00."),
    )
    for values in example_rows:
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula='IF(Scenario_Controls!$A$2="","",Scenario_Controls!$A$2)'),
                CellSpec(values[0], EDITABLE_STYLE_ID),
                CellSpec(values[1], EDITABLE_STYLE_ID),
                CellSpec(values[2], EDITABLE_STYLE_ID),
                CellSpec(values[3], EDITABLE_STYLE_ID),
                CellSpec(values[4], EDITABLE_STYLE_ID),
                CellSpec(values[5], EDITABLE_STYLE_ID),
                CellSpec(values[6], EDITABLE_STYLE_ID),
                CellSpec(values[7], EDITABLE_STYLE_ID),
                CellSpec(values[8], EDITABLE_STYLE_ID),
                CellSpec(values[9], EDITABLE_STYLE_ID),
                CellSpec(values[10], EDITABLE_STYLE_ID),
                CellSpec(values[11], EDITABLE_STYLE_ID),
                CellSpec(values[12], EDITABLE_STYLE_ID),
                CellSpec(values[13], EDITABLE_STYLE_ID),
                CellSpec(values[14], EDITABLE_STYLE_ID),
                CellSpec(values[15], EDITABLE_WRAP_STYLE_ID),
            )
        )
    rows.extend(
        _blank_input_rows(
            headers=len(headers),
            count=DOSING_TEMPLATE_ROWS - len(example_rows),
            style_map=(
                CALCULATED_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_WRAP_STYLE_ID,
            ),
        )
    )
    return SheetSpec(
        name="Dosing_Assumptions",
        rows=tuple(rows),
        column_widths=(18, 20, 16, 16, 16, 14, 24, 22, 24, 16, 20, 20, 18, 16, 16, 12, 56),
        freeze_cell="A2",
        auto_filter_ref="A1:Q1",
        data_validations=(
            DataValidationSpec("B2:B81", "list", "Lookup_Lists!$A$2:$A$5"),
            DataValidationSpec("E2:E81", "list", "Lookup_Lists!$C$2:$C$3"),
            DataValidationSpec("J2:J81", "list", "Lookup_Lists!$E$2:$E$3"),
            DataValidationSpec("L2:L81", "list", "Lookup_Lists!$E$2:$E$3"),
            DataValidationSpec("P2:P81", "list", "Lookup_Lists!$I$2:$I$3"),
        ),
    )


def _build_product_parameters_sheet() -> SheetSpec:
    headers = (
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
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_rows = (
        ("scenario_default", "ALL", "ALL", 1.0, "", "", 1.0, "yes", "Approved scenario default row. Current engine consumes this ds_qty_per_dp_unit_mg default."),
        ("module_override", "AML", "ALL", 1.0, "", "", 1.0, "no", "Optional module-specific override example. Preserved in normalized artifacts for future use."),
    )
    for parameter_scope, module, geography_code, ds_qty, dp_conc, fill_volume, fg_mg, active_flag, notes in example_rows:
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula='IF(Scenario_Controls!$A$2="","",Scenario_Controls!$A$2)'),
                CellSpec(parameter_scope, EDITABLE_STYLE_ID),
                CellSpec(module, EDITABLE_STYLE_ID),
                CellSpec(geography_code, EDITABLE_STYLE_ID),
                CellSpec(ds_qty, EDITABLE_STYLE_ID),
                CellSpec(dp_conc, EDITABLE_STYLE_ID) if dp_conc != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(fill_volume, EDITABLE_STYLE_ID) if fill_volume != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(fg_mg, EDITABLE_STYLE_ID),
                CellSpec(active_flag, EDITABLE_STYLE_ID),
                CellSpec(notes, EDITABLE_WRAP_STYLE_ID),
            )
        )
    rows.extend(
        _blank_input_rows(
            headers=len(headers),
            count=PRODUCT_PARAMETER_TEMPLATE_ROWS - len(example_rows),
            style_map=(
                CALCULATED_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_WRAP_STYLE_ID,
            ),
        )
    )
    return SheetSpec(
        name="Product_Parameters",
        rows=tuple(rows),
        column_widths=(18, 18, 20, 16, 22, 24, 18, 16, 12, 64),
        freeze_cell="A2",
        auto_filter_ref="A1:J1",
        data_validations=(
            DataValidationSpec("B2:B41", "list", "Lookup_Lists!$F$2:$F$3"),
            DataValidationSpec("C2:C41", "list", "Lookup_Lists!$B$2:$B$6"),
            DataValidationSpec("I2:I41", "list", "Lookup_Lists!$I$2:$I$3"),
        ),
    )


def _build_yield_assumptions_sheet() -> SheetSpec:
    headers = (
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
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_rows = (
        ("scenario_default", "ALL", "ALL", 0.90, 0.98, 1.0, 1.0, 0.05, "yes", "Approved deterministic scenario default row."),
        ("module_override", "AML", "ALL", 0.90, 0.98, 1.0, 1.0, 0.05, "no", "Optional module-specific override example. Preserved for future use; current engine still consumes scenario-default ds_overage_factor."),
    )
    for parameter_scope, module, geography_code, ds_to_dp, dp_to_fg, fg_pack, ss_yield, ds_overage, active_flag, notes in example_rows:
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula='IF(Scenario_Controls!$A$2="","",Scenario_Controls!$A$2)'),
                CellSpec(parameter_scope, EDITABLE_STYLE_ID),
                CellSpec(module, EDITABLE_STYLE_ID),
                CellSpec(geography_code, EDITABLE_STYLE_ID),
                CellSpec(ds_to_dp, EDITABLE_STYLE_ID),
                CellSpec(dp_to_fg, EDITABLE_STYLE_ID),
                CellSpec(fg_pack, EDITABLE_STYLE_ID),
                CellSpec(ss_yield, EDITABLE_STYLE_ID),
                CellSpec(ds_overage, EDITABLE_STYLE_ID),
                CellSpec(active_flag, EDITABLE_STYLE_ID),
                CellSpec(notes, EDITABLE_WRAP_STYLE_ID),
            )
        )
    rows.extend(
        _blank_input_rows(
            headers=len(headers),
            count=YIELD_TEMPLATE_ROWS - len(example_rows),
            style_map=(
                CALCULATED_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_WRAP_STYLE_ID,
            ),
        )
    )
    return SheetSpec(
        name="Yield_Assumptions",
        rows=tuple(rows),
        column_widths=(18, 18, 20, 16, 14, 14, 14, 12, 18, 12, 72),
        freeze_cell="A2",
        auto_filter_ref="A1:K1",
        data_validations=(
            DataValidationSpec("B2:B41", "list", "Lookup_Lists!$F$2:$F$3"),
            DataValidationSpec("C2:C41", "list", "Lookup_Lists!$B$2:$B$6"),
            DataValidationSpec("J2:J41", "list", "Lookup_Lists!$I$2:$I$3"),
        ),
    )


def _build_packaging_and_vialing_sheet() -> SheetSpec:
    headers = (
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
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    notes = "Approved base case: patient-dose ceiling, no sharing, 1 vial per carton, full pack consumed."
    for module in PHASE1_MODULES:
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula='IF(Scenario_Controls!$A$2="","",Scenario_Controls!$A$2)'),
                CellSpec(module, EDITABLE_STYLE_ID),
                CellSpec("ALL", EDITABLE_STYLE_ID),
                CellSpec("patient_dose_ceiling", EDITABLE_STYLE_ID),
                CellSpec("true", EDITABLE_STYLE_ID),
                CellSpec("false", EDITABLE_STYLE_ID),
                CellSpec(1, EDITABLE_STYLE_ID),
                CellSpec("full_pack_consumed", EDITABLE_STYLE_ID),
                CellSpec("yes", EDITABLE_STYLE_ID),
                CellSpec(notes, EDITABLE_WRAP_STYLE_ID),
            )
        )
    rows.extend(
        _blank_input_rows(
            headers=len(headers),
            count=PACKAGING_TEMPLATE_ROWS - len(PHASE1_MODULES),
            style_map=(
                CALCULATED_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_WRAP_STYLE_ID,
            ),
        )
    )
    return SheetSpec(
        name="Packaging_and_Vialing",
        rows=tuple(rows),
        column_widths=(18, 20, 16, 22, 20, 20, 16, 22, 12, 72),
        freeze_cell="A2",
        auto_filter_ref="A1:J1",
        data_validations=(
            DataValidationSpec("B2:B41", "list", "Lookup_Lists!$A$2:$A$5"),
            DataValidationSpec("D2:D41", "list", "Lookup_Lists!$G$2:$G$2"),
            DataValidationSpec("E2:F41", "list", "Lookup_Lists!$E$2:$E$3"),
            DataValidationSpec("H2:H41", "list", "Lookup_Lists!$L$2:$L$2"),
            DataValidationSpec("I2:I41", "list", "Lookup_Lists!$I$2:$I$3"),
        ),
    )


def _build_ss_assumptions_sheet() -> SheetSpec:
    headers = (
        "scenario_name",
        "module",
        "geography_code",
        "ss_ratio_to_fg",
        "co_pack_mode",
        "active_flag",
        "notes",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    rows.append(
        (
            CellSpec(style_id=CALCULATED_STYLE_ID, formula='IF(Scenario_Controls!$A$2="","",Scenario_Controls!$A$2)'),
            CellSpec("ALL", EDITABLE_STYLE_ID),
            CellSpec("ALL", EDITABLE_STYLE_ID),
            CellSpec(1.0, EDITABLE_STYLE_ID),
            CellSpec("separate_sku_first", EDITABLE_STYLE_ID),
            CellSpec("yes", EDITABLE_STYLE_ID),
            CellSpec("Approved deterministic scenario default row. Current engine consumes a global SS ratio and co_pack_mode.", EDITABLE_WRAP_STYLE_ID),
        )
    )
    rows.extend(
        _blank_input_rows(
            headers=len(headers),
            count=SS_TEMPLATE_ROWS - 1,
            style_map=(
                CALCULATED_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_WRAP_STYLE_ID,
            ),
        )
    )
    return SheetSpec(
        name="SS_Assumptions",
        rows=tuple(rows),
        column_widths=(18, 20, 16, 16, 20, 12, 72),
        freeze_cell="A2",
        auto_filter_ref="A1:G1",
        data_validations=(
            DataValidationSpec("B2:B21", "list", "Lookup_Lists!$B$2:$B$6"),
            DataValidationSpec("E2:E21", "list", "Lookup_Lists!$H$2:$H$2"),
            DataValidationSpec("F2:F21", "list", "Lookup_Lists!$I$2:$I$3"),
        ),
    )


def _build_cml_prevalent_assumptions_sheet() -> SheetSpec:
    headers = (
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
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_rows = (
        ("US", "", 1, 1, 12, "PLACEHOLDER_PROFILE", 1, 3, 4, "track_vs_pool", "PLACEHOLDER", "no", "PLACEHOLDER populate approved US prevalent pool inputs before activating."),
        ("EU", "", 1, 1, 12, "PLACEHOLDER_PROFILE", 1, 3, 4, "track_vs_pool", "PLACEHOLDER", "no", "PLACEHOLDER populate approved EU prevalent pool inputs before activating."),
    )
    for geography_code, pool, launch_year_index, launch_month_index, duration_months, profile_id, bolus_start_year, bolus_end_year, exhaustion_year, exhaustion_rule, source, active_flag, notes in example_rows:
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula='IF(Scenario_Controls!$A$2="","",Scenario_Controls!$A$2)'),
                CellSpec(geography_code, EDITABLE_STYLE_ID),
                CellSpec(pool, EDITABLE_STYLE_ID) if pool != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(launch_year_index, EDITABLE_STYLE_ID),
                CellSpec(launch_month_index, EDITABLE_STYLE_ID),
                CellSpec(duration_months, EDITABLE_STYLE_ID),
                CellSpec(profile_id, EDITABLE_STYLE_ID),
                CellSpec(bolus_start_year, EDITABLE_STYLE_ID),
                CellSpec(bolus_end_year, EDITABLE_STYLE_ID),
                CellSpec(exhaustion_year, EDITABLE_STYLE_ID),
                CellSpec(exhaustion_rule, EDITABLE_STYLE_ID),
                CellSpec(source, EDITABLE_STYLE_ID),
                CellSpec(active_flag, EDITABLE_STYLE_ID),
                CellSpec(notes, EDITABLE_WRAP_STYLE_ID),
            )
        )
    rows.extend(
        _blank_input_rows(
            headers=len(headers),
            count=CML_PREVALENT_TEMPLATE_ROWS - len(example_rows),
            style_map=(
                CALCULATED_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_WRAP_STYLE_ID,
            ),
        )
    )
    return SheetSpec(
        name="CML_Prevalent_Assumptions",
        rows=tuple(rows),
        column_widths=(18, 16, 24, 16, 18, 16, 20, 16, 14, 16, 18, 18, 12, 72),
        freeze_cell="A2",
        auto_filter_ref="A1:N1",
        data_validations=(
            DataValidationSpec("K2:K41", "list", "Lookup_Lists!$M$2:$M$4"),
            DataValidationSpec("M2:M41", "list", "Lookup_Lists!$I$2:$I$3"),
        ),
    )


def _build_trade_inventory_future_hooks_sheet() -> SheetSpec:
    headers = (
        "scenario_name",
        "module",
        "geography_code",
        "trade_rule_placeholder",
        "inventory_rule_placeholder",
        "active_flag",
        "notes",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    rows.append(
        (
            CellSpec(style_id=CALCULATED_STYLE_ID, formula='IF(Scenario_Controls!$A$2="","",Scenario_Controls!$A$2)'),
            CellSpec("ALL", EDITABLE_STYLE_ID),
            CellSpec("ALL", EDITABLE_STYLE_ID),
            CellSpec("PLACEHOLDER_FUTURE_PHASE", EDITABLE_STYLE_ID),
            CellSpec("PLACEHOLDER_FUTURE_PHASE", EDITABLE_STYLE_ID),
            CellSpec("no", EDITABLE_STYLE_ID),
            CellSpec("Future-phase placeholders only. Not wired into active model logic.", EDITABLE_WRAP_STYLE_ID),
        )
    )
    rows.extend(
        _blank_input_rows(
            headers=len(headers),
            count=TRADE_FUTURE_TEMPLATE_ROWS - 1,
            style_map=(
                CALCULATED_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_STYLE_ID,
                EDITABLE_WRAP_STYLE_ID,
            ),
        )
    )
    return SheetSpec(
        name="Trade_Inventory_FutureHooks",
        rows=tuple(rows),
        column_widths=(18, 20, 16, 28, 32, 12, 68),
        freeze_cell="A2",
        auto_filter_ref="A1:G1",
        data_validations=(DataValidationSpec("F2:F21", "list", "Lookup_Lists!$I$2:$I$3"),),
    )


def _build_lookup_lists_sheet() -> SheetSpec:
    headers = (
        "modules",
        "modules_with_all",
        "dose_basis",
        "yes_no",
        "true_false",
        "parameter_scope",
        "vialing_rule",
        "co_pack_mode",
        "active_flag",
        "forecast_grain",
        "forecast_frequency",
        "partial_pack_handling",
        "exhaustion_rule",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    lookup_rows = (
        ("AML", "ALL", "fixed", "yes", "true", "scenario_default", "patient_dose_ceiling", "separate_sku_first", "yes", "module_level", "monthly", "full_pack_consumed", "track_vs_pool"),
        ("MDS", "AML", "weight_based", "no", "false", "module_override", "", "", "no", "segment_level", "annual", "", "placeholder_metadata_only"),
        ("CML_Incident", "MDS", "", "", "", "", "", "", "", "", "", "", "validate_only"),
        ("CML_Prevalent", "CML_Incident", "", "", "", "", "", "", "", "", "", "", ""),
        ("", "CML_Prevalent", "", "", "", "", "", "", "", "", "", "", ""),
        ("", "", "", "", "", "", "", "", "", "", "", "", ""),
    )
    for row in lookup_rows:
        rows.append(tuple(CellSpec(value) for value in row))
    return SheetSpec(
        name="Lookup_Lists",
        rows=tuple(rows),
        column_widths=(18, 18, 18, 12, 12, 18, 24, 20, 12, 18, 18, 22, 24),
        freeze_cell="A2",
        auto_filter_ref="A1:M1",
    )
