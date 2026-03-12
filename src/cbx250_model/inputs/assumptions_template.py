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
TREATMENT_DURATION_TEMPLATE_ROWS = 40
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
        _build_treatment_duration_assumptions_sheet(),
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
                "Users should edit Excel assumptions here rather than hand-editing TOML files. The importer converts these rows into normalized artifacts plus generated Phase 2 and Phase 3 parameter configs.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Scope", LABEL_STYLE_ID),
            CellSpec(
                "Current scope covers Phase 1, Phase 2, and the active deterministic Phase 3, Phase 4, and Phase 5 assumptions.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "Revenue, Monte Carlo, and UI remain later-phase work. Trade_Inventory_FutureHooks now wires the active deterministic Phase 3 trade config plus the active deterministic Phase 4 scheduling, Phase 5 inventory, and Phase 6 financial configs.",
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
                "One active Scenario_Controls row is required. Dosing_Assumptions should provide one active module row for AML, MDS, CML_Incident, and CML_Prevalent. The seeded base case uses demand_basis = patient_starts, so populate active Treatment_Duration_Assumptions rows before running the forecast workflow.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Phase 1 demand basis", LABEL_STYLE_ID),
            CellSpec(
                "patient_starts is the preferred default operating mode and represents the approved base-case commercial input path.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "treated_census remains supported for backward compatibility and special cases only. Do not apply duration logic on top of already treated-census inputs.",
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
                "The importer generates machine-readable CSV artifacts plus generated_phase2_parameters.toml / generated_phase2_scenario.toml, generated_phase3_parameters.toml / generated_phase3_scenario.toml, generated_phase4_parameters.toml / generated_phase4_scenario.toml, generated_phase5_parameters.toml / generated_phase5_scenario.toml, and generated_phase6_parameters.toml / generated_phase6_scenario.toml.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "Current wiring consumes: Scenario_Controls.demand_basis plus Treatment_Duration_Assumptions for Phase 1 starts-based mode; dose_basis_default, module-specific dosing values, module FG mg per unit, module FG vialing rule, global yields, DS quantity per DP unit default, DS overage default, SS ratio, and co_pack_mode for Phase 2; active deterministic trade parameters from Trade_Inventory_FutureHooks for Phase 3; active deterministic Phase 4 scheduling plus Phase 5 inventory controls from Trade_Inventory_FutureHooks together with the scenario-default Product_Parameters, Yield_Assumptions, and SS_Assumptions rows; and active deterministic Phase 6 financial assumptions plus editable geography-bucketed Sub-Layer 1 -> Sub-Layer 2 shipping/cold-chain cost parameters from Trade_Inventory_FutureHooks together with the scenario-default Product_Parameters, Yield_Assumptions, and SS_Assumptions rows.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Future-ready fields", LABEL_STYLE_ID),
            CellSpec(
                "Launch_Timing, geography-specific overrides, segment-specific overrides, dp_concentration_mg_per_ml, and dp_fill_volume_ml are preserved in normalized outputs even if not yet wired into active model logic.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "CML_Prevalent_Assumptions remains a dedicated artifact sheet. Populate approved pool, timing, and exhaustion metadata here without treating CML like AML/MDS segment mix logic. Trade_Inventory_FutureHooks still carries a historical name, but it now also feeds the active deterministic Phase 3 trade config plus the active Phase 4 scheduling, Phase 5 inventory, and Phase 6 financial config bridges.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Workbook to model bridge", LABEL_STYLE_ID),
            CellSpec(
                "After import, use the generated Phase 2 or Phase 3 scenario directly, or pass the assumptions workbook into the one-command forecast workflow.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "This keeps the current architecture intact while removing the need to edit TOML by hand for the active deterministic assumptions. The workflow also consumes the generated treatment duration artifact when demand_basis = patient_starts and can optionally run through Phase 5.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Approved base-case defaults", LABEL_STYLE_ID),
            CellSpec(
                "Seeded defaults reflect the current approved base case: annual patient_starts for Phase 1, fixed dose 0.15 mg, weight-based 0.0023 mg/kg, deterministic average weight 80 kg, AML/MDS 4.33 doses per month, CML modules 1.00 dose per month, fg_mg_per_unit 1.0 mg, DS quantity per DP unit 1.0 mg, ds_to_dp_yield 0.90, dp_to_fg_yield 0.98, ds_overage_factor 0.05, ss_ratio_to_fg 1.0.",
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
        "demand_basis",
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
            CellSpec("annual", EDITABLE_STYLE_ID),
            CellSpec("patient_starts", EDITABLE_STYLE_ID),
            CellSpec("fixed", EDITABLE_STYLE_ID),
            CellSpec("USD", EDITABLE_STYLE_ID),
            CellSpec("Edit this row instead of hand-editing config files. Seeded for the annual patient_starts base case.", EDITABLE_WRAP_STYLE_ID),
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
                EDITABLE_STYLE_ID,
                EDITABLE_WRAP_STYLE_ID,
            ),
        )
    )
    return SheetSpec(
        name="Scenario_Controls",
        rows=tuple(rows),
        column_widths=(20, 40, 12, 18, 18, 18, 18, 14, 56),
        freeze_cell="A2",
        auto_filter_ref="A1:I1",
        data_validations=(
            DataValidationSpec("C2:C11", "list", "Lookup_Lists!$J$2:$J$3"),
            DataValidationSpec("D2:D11", "list", "Lookup_Lists!$K$2:$K$3"),
            DataValidationSpec("E2:E11", "list", "Lookup_Lists!$L$2:$L$3"),
            DataValidationSpec("F2:F11", "list", "Lookup_Lists!$D$2:$D$3"),
            DataValidationSpec("G2:G11", "list", "Lookup_Lists!$C$2:$C$3"),
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
            DataValidationSpec("F2:F41", "list", "Lookup_Lists!$J$2:$J$3"),
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
            DataValidationSpec("J2:J81", "list", "Lookup_Lists!$F$2:$F$3"),
            DataValidationSpec("L2:L81", "list", "Lookup_Lists!$F$2:$F$3"),
            DataValidationSpec("P2:P81", "list", "Lookup_Lists!$J$2:$J$3"),
        ),
    )


def _build_treatment_duration_assumptions_sheet() -> SheetSpec:
    headers = (
        "scenario_name",
        "module",
        "segment_code",
        "geography_code",
        "treatment_duration_months",
        "active_flag",
        "notes",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_rows = (
        ("AML", "1L_fit", "ALL", 12, "yes", "Approved base-case duration default."),
        ("AML", "1L_unfit", "ALL", 10, "yes", "Approved base-case duration default."),
        ("AML", "RR", "ALL", 6, "yes", "Approved base-case duration default."),
        ("MDS", "HR_MDS", "ALL", 12, "yes", "Approved base-case duration default."),
        ("MDS", "LR_MDS", "ALL", 12, "yes", "Approved base-case duration default."),
        ("CML_Incident", "CML_Incident", "ALL", 24, "yes", "Approved base-case duration default."),
        ("CML_Prevalent", "CML_Prevalent", "ALL", 24, "yes", "Approved base-case duration default."),
    )
    for module, segment_code, geography_code, treatment_duration_months, active_flag, notes in example_rows:
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula='IF(Scenario_Controls!$A$2="","",Scenario_Controls!$A$2)'),
                CellSpec(module, EDITABLE_STYLE_ID),
                CellSpec(segment_code, EDITABLE_STYLE_ID),
                CellSpec(geography_code, EDITABLE_STYLE_ID),
                CellSpec(treatment_duration_months, EDITABLE_STYLE_ID),
                CellSpec(active_flag, EDITABLE_STYLE_ID),
                CellSpec(notes, EDITABLE_WRAP_STYLE_ID),
            )
        )
    rows.extend(
        _blank_input_rows(
            headers=len(headers),
            count=TREATMENT_DURATION_TEMPLATE_ROWS - len(example_rows),
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
        name="Treatment_Duration_Assumptions",
        rows=tuple(rows),
        column_widths=(18, 20, 18, 16, 24, 12, 72),
        freeze_cell="A2",
        auto_filter_ref="A1:G1",
        data_validations=(
            DataValidationSpec("B2:B41", "list", "Lookup_Lists!$A$2:$A$5"),
            DataValidationSpec("F2:F41", "list", "Lookup_Lists!$J$2:$J$3"),
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
            DataValidationSpec("B2:B41", "list", "Lookup_Lists!$G$2:$G$3"),
            DataValidationSpec("C2:C41", "list", "Lookup_Lists!$B$2:$B$6"),
            DataValidationSpec("I2:I41", "list", "Lookup_Lists!$J$2:$J$3"),
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
            DataValidationSpec("B2:B41", "list", "Lookup_Lists!$G$2:$G$3"),
            DataValidationSpec("C2:C41", "list", "Lookup_Lists!$B$2:$B$6"),
            DataValidationSpec("J2:J41", "list", "Lookup_Lists!$J$2:$J$3"),
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
            DataValidationSpec("D2:D41", "list", "Lookup_Lists!$H$2:$H$2"),
            DataValidationSpec("E2:F41", "list", "Lookup_Lists!$F$2:$F$3"),
            DataValidationSpec("H2:H41", "list", "Lookup_Lists!$M$2:$M$2"),
            DataValidationSpec("I2:I41", "list", "Lookup_Lists!$J$2:$J$3"),
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
            DataValidationSpec("E2:E21", "list", "Lookup_Lists!$I$2:$I$2"),
            DataValidationSpec("F2:F21", "list", "Lookup_Lists!$J$2:$J$3"),
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
            DataValidationSpec("K2:K41", "list", "Lookup_Lists!$N$2:$N$4"),
            DataValidationSpec("M2:M41", "list", "Lookup_Lists!$J$2:$J$3"),
        ),
    )


def _build_trade_inventory_future_hooks_sheet() -> SheetSpec:
    headers = (
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
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    scenario_default_row = {
        "trade_row_type": "scenario_default",
        "module": "ALL",
        "geography_code": "ALL",
        "sublayer1_target_weeks_on_hand": 2.5,
        "sublayer2_target_weeks_on_hand": 1.5,
        "sublayer2_wastage_rate": 0.0,
        "initial_stocking_units_per_new_site": 6.0,
        "ss_units_per_new_site": 6.0,
        "sublayer1_launch_fill_months_of_demand": 1.0,
        "rems_certification_lag_weeks": 0.0,
        "january_softening_enabled": "false",
        "january_softening_factor": 1.0,
        "bullwhip_flag_threshold": 0.25,
        "channel_fill_start_prelaunch_weeks": 4.0,
        "sublayer2_fill_distribution_weeks": 8.0,
        "weeks_per_month": 4.33,
        "bullwhip_amplification_threshold": 1.25,
        "bullwhip_review_window_months": 2,
        "excess_build_threshold_ratio": 0.25,
        "supply_gap_tolerance_units": 0.000001,
        "capacity_clip_tolerance_units": 0.000001,
        "cml_prevalent_forward_window_months": 6,
        "projected_cml_prevalent_bolus_exhaustion_month_index": 0,
        "fg_lead_time_from_dp_release_weeks": 4.0,
        "fg_packaging_cycle_weeks": 2.0,
        "fg_release_qa_weeks": 2.0,
        "fg_total_order_to_release_weeks": 8.0,
        "fg_packaging_campaign_size_units": 50000.0,
        "dp_lead_time_from_ds_release_weeks": 4.0,
        "dp_manufacturing_cycle_weeks": 2.0,
        "dp_release_testing_weeks": 12.0,
        "dp_total_order_to_release_weeks": 18.0,
        "dp_min_batch_size_units": 100000.0,
        "dp_max_batch_size_units": 500000.0,
        "dp_min_campaign_batches": 3,
        "dp_annual_capacity_batches": 10,
        "ds_lead_time_to_batch_start_planning_horizon_weeks": 24.0,
        "ds_manufacturing_cycle_weeks": 8.0,
        "ds_release_testing_weeks": 12.0,
        "ds_total_order_to_release_weeks": 44.0,
        "ds_min_batch_size_kg": 2.0,
        "ds_max_batch_size_kg": 4.0,
        "ds_min_campaign_batches": 3,
        "ds_annual_capacity_batches": 5,
        "ss_order_to_release_lead_time_weeks": 24.0,
        "ss_batch_size_units": 100000.0,
        "ss_min_campaign_batches": 3,
        "ss_annual_capacity_batches": 10,
        "ss_release_must_coincide_with_or_precede_fg": "true",
        "starting_inventory_ds_mg": 0.0,
        "starting_inventory_dp_units": 0.0,
        "starting_inventory_fg_units": 0.0,
        "starting_inventory_ss_units": 0.0,
        "starting_inventory_sublayer1_fg_units": 0.0,
        "starting_inventory_sublayer2_fg_units": 0.0,
        "shelf_life_ds_months": 48,
        "shelf_life_dp_months": 36,
        "shelf_life_fg_months": 36,
        "shelf_life_ss_months": 48,
        "excess_inventory_threshold_months_of_cover": 18.0,
        "stockout_tolerance_units": 0.000001,
        "fefo_enabled": "true",
        "ss_fg_match_required": "true",
        "allow_prelaunch_inventory_build": "true",
        "phase5_enforce_unique_output_keys": "true",
        "phase5_reconcile_phase4_receipts": "true",
        "phase5_reconciliation_tolerance_units": 0.000001,
        "active_flag": "yes",
        "notes": "Active deterministic defaults for Phase 3, Phase 4, Phase 5, and Phase 6. Phase 5 shelf life/excess-cover and Phase 6 standard-cost placeholders are revised baseline assumptions; edit here instead of hand-editing phase3_trade_layer.toml, phase4_production_schedule.toml, phase5_inventory_layer.toml, or phase6_financial_layer.toml.",
        "ds_standard_cost_basis_unit": "mg",
        "ds_standard_cost_per_mg": 0.002,
        "dp_conversion_cost_per_unit": 0.5,
        "fg_packaging_labeling_cost_per_unit": 0.25,
        "ss_standard_cost_per_unit": 0.1,
        "annual_inventory_carry_rate": 0.2,
        "monthly_inventory_carry_rate": 0.0166666666666667,
        "expired_inventory_writeoff_rate": 1.0,
        "expired_inventory_salvage_rate": 0.0,
        "value_unmatched_fg_at_fg_standard_cost": "true",
        "include_trade_node_fg_value": "true",
        "use_matched_administrable_fg_value": "true",
        "phase6_enforce_unique_output_keys": "true",
        "phase6_reconciliation_tolerance_value": 0.000001,
        "us_fg_sub1_to_sub2_cost_per_unit": 25.0,
        "eu_fg_sub1_to_sub2_cost_per_unit": 57.5,
        "us_ss_sub1_to_sub2_cost_per_unit": 25.0,
        "eu_ss_sub1_to_sub2_cost_per_unit": 57.5,
    }
    example_rows = [
        scenario_default_row,
        {
            "trade_row_type": "geography_default",
            "module": "ALL",
            "geography_code": "US",
            "site_activation_rate": 5.0,
            "certified_sites_at_launch": 5.0,
            "certified_sites_at_peak": 25.0,
            "active_flag": "yes",
            "notes": "Approved/sample US site activation defaults for the deterministic Phase 3 trade layer.",
        },
        {
            "trade_row_type": "geography_default",
            "module": "ALL",
            "geography_code": "EU",
            "site_activation_rate": 3.0,
            "certified_sites_at_launch": 3.0,
            "certified_sites_at_peak": 18.0,
            "active_flag": "yes",
            "notes": "Approved/sample EU site activation defaults for the deterministic Phase 3 trade layer.",
        },
    ]
    for module in PHASE1_MODULES:
        example_rows.append(
            {
                "trade_row_type": "launch_event",
                "module": module,
                "geography_code": "US",
                "launch_month_index": 1,
                "active_flag": "yes",
                "notes": "Approved/sample launch event.",
            }
        )
    for module in PHASE1_MODULES:
        example_rows.append(
            {
                "trade_row_type": "launch_event",
                "module": module,
                "geography_code": "EU",
                "launch_month_index": 1,
                "active_flag": "yes",
                "notes": "Approved/sample launch event.",
            }
        )

    for row in example_rows:
        row_values = [row.get(header, "") for header in headers[1:]]
        rendered_cells: list[CellSpec] = [
            CellSpec(style_id=CALCULATED_STYLE_ID, formula='IF(Scenario_Controls!$A$2="","",Scenario_Controls!$A$2)')
        ]
        for header, value in zip(headers[1:], row_values, strict=True):
            style_id = EDITABLE_WRAP_STYLE_ID if header == "notes" else EDITABLE_STYLE_ID
            if value == "":
                rendered_cells.append(CellSpec(style_id=style_id))
            else:
                rendered_cells.append(CellSpec(value, style_id))
        rows.append(
            tuple(rendered_cells)
        )
    rows.extend(
        _blank_input_rows(
            headers=len(headers),
            count=TRADE_FUTURE_TEMPLATE_ROWS - len(example_rows),
            style_map=(
                (CALCULATED_STYLE_ID,)
                + (EDITABLE_STYLE_ID,) * (headers.index("notes") - 1)
                + (EDITABLE_WRAP_STYLE_ID,)
                + (EDITABLE_STYLE_ID,) * (len(headers) - headers.index("notes") - 1)
            ),
        )
    )
    return SheetSpec(
        name="Trade_Inventory_FutureHooks",
        rows=tuple(rows),
        column_widths=(
            18, 18, 18, 16, 18, 18, 18, 20, 18, 24, 20, 18, 18, 18, 22, 22, 16, 18, 20, 20, 18,
            18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 20, 18, 18, 18, 18, 20, 20, 18, 18, 18,
            22, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18,
            18, 18, 18, 18, 18, 18, 18, 18, 12, 72, 18, 18, 18, 20, 18, 18, 18, 18, 18, 18, 18,
            20, 18, 18,
        ),
        freeze_cell="A2",
        auto_filter_ref="A1:CJ1",
        data_validations=(
            DataValidationSpec("B2:B21", "list", "Lookup_Lists!$O$2:$O$4"),
            DataValidationSpec("C2:C21", "list", "Lookup_Lists!$B$2:$B$6"),
            DataValidationSpec("L2:L21", "list", "Lookup_Lists!$F$2:$F$3"),
            DataValidationSpec("BB2:BB21", "list", "Lookup_Lists!$F$2:$F$3"),
            DataValidationSpec("BO2:BS21", "list", "Lookup_Lists!$F$2:$F$3"),
            DataValidationSpec("BU2:BU21", "list", "Lookup_Lists!$J$2:$J$3"),
            DataValidationSpec("CF2:CI21", "list", "Lookup_Lists!$F$2:$F$3"),
        ),
    )


def _build_lookup_lists_sheet() -> SheetSpec:
    headers = (
        "modules",
        "modules_with_all",
        "dose_basis",
        "demand_basis",
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
        "trade_row_type",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    lookup_rows = (
        ("AML", "ALL", "fixed", "patient_starts", "yes", "true", "scenario_default", "patient_dose_ceiling", "separate_sku_first", "yes", "module_level", "annual", "full_pack_consumed", "track_vs_pool", "scenario_default"),
        ("MDS", "AML", "weight_based", "treated_census", "no", "false", "module_override", "", "", "no", "segment_level", "monthly", "", "placeholder_metadata_only", "geography_default"),
        ("CML_Incident", "MDS", "", "", "", "", "", "", "", "", "", "", "", "validate_only", "launch_event"),
        ("CML_Prevalent", "CML_Incident", "", "", "", "", "", "", "", "", "", "", "", "", ""),
        ("", "CML_Prevalent", "", "", "", "", "", "", "", "", "", "", "", "", ""),
        ("", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
    )
    for row in lookup_rows:
        rows.append(tuple(CellSpec(value) for value in row))
    return SheetSpec(
        name="Lookup_Lists",
        rows=tuple(rows),
        column_widths=(18, 18, 18, 18, 12, 12, 18, 24, 20, 12, 18, 18, 22, 24, 20),
        freeze_cell="A2",
        auto_filter_ref="A1:O1",
    )
