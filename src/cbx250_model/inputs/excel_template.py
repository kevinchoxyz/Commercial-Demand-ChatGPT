"""Generate the business-facing Commercial forecast workbook template."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable
from xml.sax.saxutils import escape
import zipfile

XML_DECLARATION = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
SHEET_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
DOC_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
CORE_PROPS_NS = "http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
DC_NS = "http://purl.org/dc/elements/1.1/"
DCTERMS_NS = "http://purl.org/dc/terms/"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
EXT_PROPS_NS = "http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"
VTYPES_NS = "http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes"

DEFAULT_STYLE_ID = 0
HEADER_STYLE_ID = 1
EDITABLE_STYLE_ID = 2
CALCULATED_STYLE_ID = 3
LABEL_STYLE_ID = 4
EDITABLE_DATE_STYLE_ID = 5
CALCULATED_DATE_STYLE_ID = 6
CALCULATED_DECIMAL_STYLE_ID = 7
WRAP_STYLE_ID = 8
EDITABLE_WRAP_STYLE_ID = 9

GEOGRAPHY_TEMPLATE_ROWS = 100
MODULE_LEVEL_TEMPLATE_ROWS = 2500
SEGMENT_LEVEL_TEMPLATE_ROWS = 4000
MIX_TEMPLATE_ROWS = 1000
ANNUAL_MODULE_LEVEL_TEMPLATE_ROWS = 400
ANNUAL_SEGMENT_LEVEL_TEMPLATE_ROWS = 800
PROFILE_TEMPLATE_ROWS = 50
CML_PREVALENT_ASSUMPTION_ROWS = 100
MONTHLYIZED_OUTPUT_ROWS = 40000
STARTER_MONTHLYIZATION_PROFILES = (
    ("FLAT_12", "", "", "", "FLAT_12", (8.3333, 8.3333, 8.3333, 8.3333, 8.3333, 8.3333, 8.3333, 8.3333, 8.3333, 8.3333, 8.3333, 8.3337), "Starter default profile. Use only when a flat split is appropriate."),
    ("LAUNCH_RAMP", "", "", "", "LAUNCH_RAMP", (2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 10.0, 12.0, 13.0, 14.0, 16.0), "Starter default profile for launch years."),
    ("STEADY_STATE", "", "", "", "STEADY_STATE", (7.0, 7.0, 8.0, 8.0, 8.0, 8.0, 9.0, 9.0, 9.0, 9.0, 9.0, 9.0), "Starter default profile for steady-state years."),
    ("CML_PREVALENT_LAUNCH", "CML_Prevalent", "", "ALL", "CML_PREVALENT_BOLUS", (1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 15.0, 13.0, 12.0), "Starter default profile for the first CML prevalent bolus year."),
    ("CML_PREVALENT_PEAK", "CML_Prevalent", "", "ALL", "CML_PREVALENT_BOLUS", (6.0, 7.0, 8.0, 9.0, 9.0, 10.0, 10.0, 10.0, 9.0, 8.0, 7.0, 7.0), "Starter default profile for middle or peak CML prevalent bolus years."),
    ("CML_PREVALENT_TAIL", "CML_Prevalent", "", "ALL", "CML_PREVALENT_BOLUS", (12.0, 11.0, 10.0, 10.0, 9.0, 9.0, 8.0, 8.0, 7.0, 6.0, 5.0, 5.0), "Starter default profile for the final CML prevalent tail year."),
)


@dataclass(frozen=True)
class CellSpec:
    value: str | int | float | date | None = None
    style_id: int = DEFAULT_STYLE_ID
    formula: str | None = None


@dataclass(frozen=True)
class DataValidationSpec:
    sqref: str
    validation_type: str
    formula1: str
    formula2: str | None = None
    operator: str | None = None
    allow_blank: bool = True


@dataclass(frozen=True)
class SheetSpec:
    name: str
    rows: tuple[tuple[CellSpec, ...], ...]
    column_widths: tuple[float, ...]
    freeze_cell: str | None
    auto_filter_ref: str | None = None
    data_validations: tuple[DataValidationSpec, ...] = ()


def build_commercial_forecast_template(output_path: Path) -> Path:
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
            workbook.writestr(
                f"xl/worksheets/sheet{sheet_index}.xml",
                _build_worksheet_xml(sheet),
            )

    return output_path


def _build_sheets() -> tuple[SheetSpec, ...]:
    return (
        _build_instructions_sheet(),
        _build_inputs_sheet(),
        _build_geography_master_sheet(),
        _build_module_level_forecast_sheet(),
        _build_segment_level_forecast_sheet(),
        _build_annual_module_level_forecast_sheet(),
        _build_annual_segment_level_forecast_sheet(),
        _build_aml_mix_sheet(),
        _build_mds_mix_sheet(),
        _build_annual_to_monthly_profiles_sheet(),
        _build_cml_prevalent_assumptions_sheet(),
        _build_monthlyized_output_sheet(),
        _build_lookup_lists_sheet(),
    )


def _build_instructions_sheet() -> SheetSpec:
    rows = [
        _header_row(("Section", "Guidance", "Details")),
        (
            CellSpec("Workbook purpose", LABEL_STYLE_ID),
            CellSpec(
                "Use this workbook for Phase 1 Commercial patient forecast submissions and normalized monthly demand staging.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "CML must stay split into CML_Incident and CML_Prevalent. AML and MDS mix tabs support allocation in module_level mode and QA/reference in segment_level mode.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Frequency choice", LABEL_STYLE_ID),
            CellSpec(
                "Set forecast_frequency to monthly when Commercial has month-level data. Set it to annual when Commercial only has year buckets.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "Annual rows are monthlyized through Annual_to_Monthly_Profiles. Do not divide by 12 unless profile_id = FLAT_12.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("If forecast_grain = module_level", LABEL_STYLE_ID),
            CellSpec(
                "Monthly mode: fill ModuleLevel_Forecast plus AML_Mix and MDS_Mix. Annual mode: fill Annual_ModuleLevel_Forecast plus AML_Mix and MDS_Mix.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "AML and MDS are allocated to segments after monthlyization. CML modules stay separate and do not use AML/MDS mix logic.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("If forecast_grain = segment_level", LABEL_STYLE_ID),
            CellSpec(
                "Monthly mode: fill SegmentLevel_Forecast. Annual mode: fill Annual_SegmentLevel_Forecast.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "AML_Mix and MDS_Mix can still be used for reasonableness checks, but they are not used to allocate demand in segment_level mode.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("CML_Prevalent precedence", LABEL_STYLE_ID),
            CellSpec(
                "If explicit CML_Prevalent forecast rows exist in the active forecast tab, they are the primary demand input.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "Always complete CML_Prevalent_Assumptions for validation pool generation. If explicit CML_Prevalent rows are missing, the assumptions sheet can generate a fallback monthly series.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("CML_Prevalent fallback", LABEL_STYLE_ID),
            CellSpec(
                "If you need fallback generation, populate fallback_patients_treated_annual on CML_Prevalent_Assumptions.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "This is a clearly labeled workbook input used only when explicit CML_Prevalent forecast rows are absent from the active forecast sheet.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Geography setup", LABEL_STYLE_ID),
            CellSpec("Maintain one row per geography in Geography_Master.", WRAP_STYLE_ID),
            CellSpec(
                "Seed rows for US and EU are included. Add additional geographies in the blank rows beneath them.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Month index", LABEL_STYLE_ID),
            CellSpec("Use month_index values 1 through 240.", WRAP_STYLE_ID),
            CellSpec(
                "calendar_month is formula-driven from us_aml_mds_initial_approval_date on the Inputs sheet. For annual rows, year_index is converted into the matching calendar year and monthly bucket range.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Profile guidance", LABEL_STYLE_ID),
            CellSpec(
                "Starter profiles are editable defaults, not fixed business assumptions.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "Recommended mapping: AML/MDS/CML_Incident launch year -> LAUNCH_RAMP, later years -> STEADY_STATE or FLAT_12; CML_Prevalent first bolus -> CML_PREVALENT_LAUNCH, middle years -> CML_PREVALENT_PEAK, final tail -> CML_PREVALENT_TAIL.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Segment rules", LABEL_STYLE_ID),
            CellSpec(
                "AML segments: 1L_fit, 1L_unfit, RR. MDS segments: HR_MDS, LR_MDS.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "For CML rows on SegmentLevel_Forecast, use segment_code = ALL because the business input is already separated by module.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Generated output", LABEL_STYLE_ID),
            CellSpec(
                "Monthlyized_Output is a generated or reference tab for normalized monthly patient-treated rows.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "In Phase 1, the authoritative normalized workbook export is monthlyized_output.csv. The workbook tab is reference-only unless a future exporter explicitly writes rows back into it.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("Workbook roles", LABEL_STYLE_ID),
            CellSpec(
                "User-entry sheets: Inputs, Geography_Master, ModuleLevel_Forecast, SegmentLevel_Forecast, Annual_ModuleLevel_Forecast, Annual_SegmentLevel_Forecast, AML_Mix, MDS_Mix, Annual_to_Monthly_Profiles, CML_Prevalent_Assumptions.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "Generated/reference sheet: Monthlyized_Output. Authoritative CSV exports: monthlyized_output.csv plus the normalized Phase 1 contract CSVs written by the importer.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("CML Phase 1 limitation", LABEL_STYLE_ID),
            CellSpec(
                "exhaustion_rule is captured and audited for CML_Prevalent, but Phase 1 does not implement a full dynamic depletion or remainder engine.",
                WRAP_STYLE_ID,
            ),
            CellSpec(
                "Current CML_Prevalent behavior uses the supplied annual totals, launch_month_index, duration_months, and profile logic only. Fuller depletion mechanics are deferred to a later phase.",
                WRAP_STYLE_ID,
            ),
        ),
    ]
    return SheetSpec(
        name="Instructions",
        rows=tuple(rows),
        column_widths=(28, 46, 82),
        freeze_cell="A2",
        auto_filter_ref="A1:C1",
    )


def _build_inputs_sheet() -> SheetSpec:
    rows = [
        _header_row(("Input", "Value", "Required", "Guidance")),
        (
            CellSpec("scenario_name", LABEL_STYLE_ID),
            CellSpec("BASE_2029", EDITABLE_STYLE_ID),
            CellSpec("yes"),
            CellSpec("Single scenario label carried through every submission row.", WRAP_STYLE_ID),
        ),
        (
            CellSpec("forecast_grain", LABEL_STYLE_ID),
            CellSpec("module_level", EDITABLE_STYLE_ID),
            CellSpec("yes"),
            CellSpec(
                "Choose module_level when forecast is provided at geography x module x month. Choose segment_level when AML/MDS arrive pre-segmented.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("forecast_frequency", LABEL_STYLE_ID),
            CellSpec("monthly", EDITABLE_STYLE_ID),
            CellSpec("yes"),
            CellSpec(
                "Choose monthly for direct monthly entry or annual for year-bucket entry that must be monthlyized through profiles.",
                WRAP_STYLE_ID,
            ),
        ),
        (
            CellSpec("us_aml_mds_initial_approval_date", LABEL_STYLE_ID),
            CellSpec(date(2029, 1, 1), EDITABLE_DATE_STYLE_ID),
            CellSpec("yes"),
            CellSpec("Year 1 anchor used for calendar_month formulas across the workbook.", WRAP_STYLE_ID),
        ),
        (
            CellSpec("real_geography_list_confirmed", LABEL_STYLE_ID),
            CellSpec("false", EDITABLE_STYLE_ID),
            CellSpec("yes"),
            CellSpec("Set to true once the Geography_Master list is reviewed and finalized.", WRAP_STYLE_ID),
        ),
    ]
    return SheetSpec(
        name="Inputs",
        rows=tuple(rows),
        column_widths=(34, 24, 12, 84),
        freeze_cell="A2",
        auto_filter_ref="A1:D1",
        data_validations=(
            DataValidationSpec("B3", "list", "Lookup_Lists!$A$2:$A$3"),
            DataValidationSpec("B4", "list", "Lookup_Lists!$B$2:$B$3"),
            DataValidationSpec("B6", "list", "Lookup_Lists!$J$2:$J$3"),
        ),
    )


def _build_geography_master_sheet() -> SheetSpec:
    headers = ("geography_code", "market_group", "currency_code", "launch_sequence_rank", "active_flag")
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_rows = (
        ("US", "North America", "USD", 1, "yes"),
        ("EU", "Europe", "EUR", 2, "yes"),
    )
    for geography_code, market_group, currency_code, launch_rank, active_flag in example_rows:
        rows.append(
            (
                CellSpec(geography_code, EDITABLE_STYLE_ID),
                CellSpec(market_group, EDITABLE_STYLE_ID),
                CellSpec(currency_code, EDITABLE_STYLE_ID),
                CellSpec(launch_rank, EDITABLE_STYLE_ID),
                CellSpec(active_flag, EDITABLE_STYLE_ID),
            )
        )
    rows.extend(
        _blank_input_rows(
            headers=len(headers),
            count=GEOGRAPHY_TEMPLATE_ROWS - len(example_rows),
            style_map=(EDITABLE_STYLE_ID, EDITABLE_STYLE_ID, EDITABLE_STYLE_ID, EDITABLE_STYLE_ID, EDITABLE_STYLE_ID),
        )
    )
    return SheetSpec(
        name="Geography_Master",
        rows=tuple(rows),
        column_widths=(18, 24, 16, 22, 12),
        freeze_cell="A2",
        auto_filter_ref="A1:E1",
        data_validations=(
            DataValidationSpec("D2:D101", "whole", "1", "9999", operator="between"),
            DataValidationSpec("E2:E101", "list", "Lookup_Lists!$I$2:$I$3"),
        ),
    )


def _build_module_level_forecast_sheet() -> SheetSpec:
    headers = (
        "scenario_name",
        "geography_code",
        "module",
        "month_index",
        "calendar_month",
        "patients_treated",
        "notes",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_values = {
        2: ("US", "AML", 1, 25.0, "Replace example row"),
        3: ("US", "CML_Incident", 1, 5.0, "CML must remain a separate module"),
        4: ("EU", "MDS", 1, 12.0, "Example only"),
        5: ("EU", "CML_Prevalent", 1, 18.0, "Example only"),
    }
    for row_number in range(2, MODULE_LEVEL_TEMPLATE_ROWS + 2):
        geography_code, module, month_index, patients_treated, notes = example_values.get(
            row_number,
            ("", "", "", "", ""),
        )
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula="Inputs!$B$2"),
                CellSpec(geography_code, EDITABLE_STYLE_ID),
                CellSpec(module, EDITABLE_STYLE_ID),
                CellSpec(month_index, EDITABLE_STYLE_ID) if month_index != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(
                    style_id=CALCULATED_DATE_STYLE_ID,
                    formula=f'IF(OR($D{row_number}="",Inputs!$B$5=""),"",EDATE(Inputs!$B$5,$D{row_number}-1))',
                ),
                CellSpec(patients_treated, EDITABLE_STYLE_ID)
                if patients_treated != ""
                else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(notes, EDITABLE_WRAP_STYLE_ID) if notes else CellSpec(style_id=EDITABLE_WRAP_STYLE_ID),
            )
        )
    return SheetSpec(
        name="ModuleLevel_Forecast",
        rows=tuple(rows),
        column_widths=(18, 18, 20, 14, 18, 18, 40),
        freeze_cell="A2",
        auto_filter_ref="A1:G1",
        data_validations=(
            DataValidationSpec(f"B2:B{MODULE_LEVEL_TEMPLATE_ROWS + 1}", "list", "Geography_Master!$A$2:$A$101"),
            DataValidationSpec(f"C2:C{MODULE_LEVEL_TEMPLATE_ROWS + 1}", "list", "Lookup_Lists!$C$2:$C$5"),
            DataValidationSpec(
                f"D2:D{MODULE_LEVEL_TEMPLATE_ROWS + 1}",
                "whole",
                "1",
                "240",
                operator="between",
            ),
            DataValidationSpec(
                f"F2:F{MODULE_LEVEL_TEMPLATE_ROWS + 1}",
                "decimal",
                "0",
                operator="greaterThanOrEqual",
            ),
        ),
    )


def _build_segment_level_forecast_sheet() -> SheetSpec:
    headers = (
        "scenario_name",
        "geography_code",
        "module",
        "segment_code",
        "month_index",
        "calendar_month",
        "patients_treated",
        "notes",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_values = {
        2: ("US", "AML", "1L_fit", 1, 11.0, "AML segmented example"),
        3: ("US", "MDS", "HR_MDS", 1, 6.0, "MDS segmented example"),
        4: ("EU", "CML_Incident", "ALL", 1, 3.0, "Use ALL for CML business submissions"),
        5: ("EU", "CML_Prevalent", "ALL", 1, 8.0, "Use ALL for CML business submissions"),
    }
    for row_number in range(2, SEGMENT_LEVEL_TEMPLATE_ROWS + 2):
        geography_code, module, segment_code, month_index, patients_treated, notes = example_values.get(
            row_number,
            ("", "", "", "", "", ""),
        )
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula="Inputs!$B$2"),
                CellSpec(geography_code, EDITABLE_STYLE_ID),
                CellSpec(module, EDITABLE_STYLE_ID),
                CellSpec(segment_code, EDITABLE_STYLE_ID),
                CellSpec(month_index, EDITABLE_STYLE_ID) if month_index != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(
                    style_id=CALCULATED_DATE_STYLE_ID,
                    formula=f'IF(OR($E{row_number}="",Inputs!$B$5=""),"",EDATE(Inputs!$B$5,$E{row_number}-1))',
                ),
                CellSpec(patients_treated, EDITABLE_STYLE_ID)
                if patients_treated != ""
                else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(notes, EDITABLE_WRAP_STYLE_ID) if notes else CellSpec(style_id=EDITABLE_WRAP_STYLE_ID),
            )
        )
    return SheetSpec(
        name="SegmentLevel_Forecast",
        rows=tuple(rows),
        column_widths=(18, 18, 20, 18, 14, 18, 18, 42),
        freeze_cell="A2",
        auto_filter_ref="A1:H1",
        data_validations=(
            DataValidationSpec(f"B2:B{SEGMENT_LEVEL_TEMPLATE_ROWS + 1}", "list", "Geography_Master!$A$2:$A$101"),
            DataValidationSpec(f"C2:C{SEGMENT_LEVEL_TEMPLATE_ROWS + 1}", "list", "Lookup_Lists!$C$2:$C$5"),
            DataValidationSpec(f"D2:D{SEGMENT_LEVEL_TEMPLATE_ROWS + 1}", "list", "Lookup_Lists!$F$2:$F$7"),
            DataValidationSpec(
                f"E2:E{SEGMENT_LEVEL_TEMPLATE_ROWS + 1}",
                "whole",
                "1",
                "240",
                operator="between",
            ),
            DataValidationSpec(
                f"G2:G{SEGMENT_LEVEL_TEMPLATE_ROWS + 1}",
                "decimal",
                "0",
                operator="greaterThanOrEqual",
            ),
        ),
    )


def _build_annual_module_level_forecast_sheet() -> SheetSpec:
    headers = (
        "scenario_name",
        "geography_code",
        "module",
        "year_index",
        "calendar_year",
        "patients_treated_annual",
        "monthlyization_profile_id",
        "notes",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_values = {
        2: ("US", "AML", 1, 120.0, "LAUNCH_RAMP", "Launch year example"),
        3: ("US", "CML_Incident", 1, 36.0, "LAUNCH_RAMP", "Example only"),
        4: ("EU", "MDS", 2, 180.0, "STEADY_STATE", "Later-year example"),
        5: ("EU", "CML_Prevalent", 1, 90.0, "CML_PREVALENT_LAUNCH", "Explicit forecast example"),
    }
    for row_number in range(2, ANNUAL_MODULE_LEVEL_TEMPLATE_ROWS + 2):
        geography_code, module, year_index, annual_patients, profile_id, notes = example_values.get(
            row_number,
            ("", "", "", "", "", ""),
        )
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula="Inputs!$B$2"),
                CellSpec(geography_code, EDITABLE_STYLE_ID),
                CellSpec(module, EDITABLE_STYLE_ID),
                CellSpec(year_index, EDITABLE_STYLE_ID) if year_index != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(
                    style_id=CALCULATED_STYLE_ID,
                    formula=f'IF($D{row_number}="","",YEAR(Inputs!$B$5)+$D{row_number}-1)',
                ),
                CellSpec(annual_patients, EDITABLE_STYLE_ID)
                if annual_patients != ""
                else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(profile_id, EDITABLE_STYLE_ID) if profile_id else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(notes, EDITABLE_WRAP_STYLE_ID) if notes else CellSpec(style_id=EDITABLE_WRAP_STYLE_ID),
            )
        )
    return SheetSpec(
        name="Annual_ModuleLevel_Forecast",
        rows=tuple(rows),
        column_widths=(18, 18, 20, 14, 16, 22, 24, 42),
        freeze_cell="A2",
        auto_filter_ref="A1:H1",
        data_validations=(
            DataValidationSpec(
                f"B2:B{ANNUAL_MODULE_LEVEL_TEMPLATE_ROWS + 1}",
                "list",
                "Geography_Master!$A$2:$A$101",
            ),
            DataValidationSpec(
                f"C2:C{ANNUAL_MODULE_LEVEL_TEMPLATE_ROWS + 1}",
                "list",
                "Lookup_Lists!$C$2:$C$5",
            ),
            DataValidationSpec(
                f"D2:D{ANNUAL_MODULE_LEVEL_TEMPLATE_ROWS + 1}",
                "whole",
                "1",
                "25",
                operator="between",
            ),
            DataValidationSpec(
                f"F2:F{ANNUAL_MODULE_LEVEL_TEMPLATE_ROWS + 1}",
                "decimal",
                "0",
                operator="greaterThanOrEqual",
            ),
            DataValidationSpec(
                f"G2:G{ANNUAL_MODULE_LEVEL_TEMPLATE_ROWS + 1}",
                "list",
                "Lookup_Lists!$D$2:$D$7",
            ),
        ),
    )


def _build_annual_segment_level_forecast_sheet() -> SheetSpec:
    headers = (
        "scenario_name",
        "geography_code",
        "module",
        "segment_code",
        "year_index",
        "calendar_year",
        "patients_treated_annual",
        "monthlyization_profile_id",
        "notes",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_values = {
        2: ("US", "AML", "1L_fit", 1, 48.0, "LAUNCH_RAMP", "Launch year segment example"),
        3: ("US", "MDS", "HR_MDS", 2, 30.0, "STEADY_STATE", "Later-year example"),
        4: ("EU", "CML_Incident", "ALL", 1, 24.0, "LAUNCH_RAMP", "CML annual example"),
        5: ("EU", "CML_Prevalent", "ALL", 1, 60.0, "CML_PREVALENT_LAUNCH", "Explicit annual example"),
    }
    for row_number in range(2, ANNUAL_SEGMENT_LEVEL_TEMPLATE_ROWS + 2):
        geography_code, module, segment_code, year_index, annual_patients, profile_id, notes = example_values.get(
            row_number,
            ("", "", "", "", "", "", ""),
        )
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula="Inputs!$B$2"),
                CellSpec(geography_code, EDITABLE_STYLE_ID),
                CellSpec(module, EDITABLE_STYLE_ID),
                CellSpec(segment_code, EDITABLE_STYLE_ID),
                CellSpec(year_index, EDITABLE_STYLE_ID) if year_index != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(
                    style_id=CALCULATED_STYLE_ID,
                    formula=f'IF($E{row_number}="","",YEAR(Inputs!$B$5)+$E{row_number}-1)',
                ),
                CellSpec(annual_patients, EDITABLE_STYLE_ID)
                if annual_patients != ""
                else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(profile_id, EDITABLE_STYLE_ID) if profile_id else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(notes, EDITABLE_WRAP_STYLE_ID) if notes else CellSpec(style_id=EDITABLE_WRAP_STYLE_ID),
            )
        )
    return SheetSpec(
        name="Annual_SegmentLevel_Forecast",
        rows=tuple(rows),
        column_widths=(18, 18, 20, 18, 14, 16, 22, 24, 42),
        freeze_cell="A2",
        auto_filter_ref="A1:I1",
        data_validations=(
            DataValidationSpec(
                f"B2:B{ANNUAL_SEGMENT_LEVEL_TEMPLATE_ROWS + 1}",
                "list",
                "Geography_Master!$A$2:$A$101",
            ),
            DataValidationSpec(
                f"C2:C{ANNUAL_SEGMENT_LEVEL_TEMPLATE_ROWS + 1}",
                "list",
                "Lookup_Lists!$C$2:$C$5",
            ),
            DataValidationSpec(
                f"D2:D{ANNUAL_SEGMENT_LEVEL_TEMPLATE_ROWS + 1}",
                "list",
                "Lookup_Lists!$F$2:$F$7",
            ),
            DataValidationSpec(
                f"E2:E{ANNUAL_SEGMENT_LEVEL_TEMPLATE_ROWS + 1}",
                "whole",
                "1",
                "25",
                operator="between",
            ),
            DataValidationSpec(
                f"G2:G{ANNUAL_SEGMENT_LEVEL_TEMPLATE_ROWS + 1}",
                "decimal",
                "0",
                operator="greaterThanOrEqual",
            ),
            DataValidationSpec(
                f"H2:H{ANNUAL_SEGMENT_LEVEL_TEMPLATE_ROWS + 1}",
                "list",
                "Lookup_Lists!$D$2:$D$7",
            ),
        ),
    )


def _build_aml_mix_sheet() -> SheetSpec:
    headers = (
        "scenario_name",
        "geography_code",
        "month_index",
        "1L_fit_share",
        "1L_unfit_share",
        "RR_share",
        "sum_check",
        "status",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_values: dict[int, tuple[str, int, float, float, float]] = {}
    for offset, month_index in enumerate(range(1, 13), start=2):
        example_values[offset] = ("US", month_index, 0.40, 0.35, 0.25)
    example_values[14] = ("EU", 1, 0.45, 0.30, 0.25)
    for row_number in range(2, MIX_TEMPLATE_ROWS + 2):
        geography_code, month_index, fit_share, unfit_share, rr_share = example_values.get(
            row_number,
            ("", "", "", "", ""),
        )
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula="Inputs!$B$2"),
                CellSpec(geography_code, EDITABLE_STYLE_ID),
                CellSpec(month_index, EDITABLE_STYLE_ID) if month_index != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(fit_share, EDITABLE_STYLE_ID) if fit_share != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(unfit_share, EDITABLE_STYLE_ID) if unfit_share != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(rr_share, EDITABLE_STYLE_ID) if rr_share != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(
                    style_id=CALCULATED_DECIMAL_STYLE_ID,
                    formula=f'IF(COUNTA(A{row_number}:F{row_number})=0,"",ROUND(D{row_number}+E{row_number}+F{row_number},6))',
                ),
                CellSpec(
                    style_id=CALCULATED_STYLE_ID,
                    formula=f'IF(G{row_number}="","",IF(ABS(G{row_number}-1)<=0.000001,"OK","CHECK"))',
                ),
            )
        )
    return SheetSpec(
        name="AML_Mix",
        rows=tuple(rows),
        column_widths=(18, 18, 14, 14, 16, 14, 12, 12),
        freeze_cell="A2",
        auto_filter_ref="A1:H1",
        data_validations=(
            DataValidationSpec(f"B2:B{MIX_TEMPLATE_ROWS + 1}", "list", "Geography_Master!$A$2:$A$101"),
            DataValidationSpec(f"C2:C{MIX_TEMPLATE_ROWS + 1}", "whole", "1", "240", operator="between"),
            DataValidationSpec(f"D2:F{MIX_TEMPLATE_ROWS + 1}", "decimal", "0", "1", operator="between"),
        ),
    )


def _build_mds_mix_sheet() -> SheetSpec:
    headers = (
        "scenario_name",
        "geography_code",
        "month_index",
        "HR_MDS_share",
        "LR_MDS_share",
        "sum_check",
        "status",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_values: dict[int, tuple[str, int, float, float]] = {
        2: ("EU", 1, 0.60, 0.40),
    }
    for offset, month_index in enumerate(range(13, 25), start=3):
        example_values[offset] = ("EU", month_index, 0.60, 0.40)
    for row_number in range(2, MIX_TEMPLATE_ROWS + 2):
        geography_code, month_index, hr_share, lr_share = example_values.get(
            row_number,
            ("", "", "", ""),
        )
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula="Inputs!$B$2"),
                CellSpec(geography_code, EDITABLE_STYLE_ID),
                CellSpec(month_index, EDITABLE_STYLE_ID) if month_index != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(hr_share, EDITABLE_STYLE_ID) if hr_share != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(lr_share, EDITABLE_STYLE_ID) if lr_share != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(
                    style_id=CALCULATED_DECIMAL_STYLE_ID,
                    formula=f'IF(COUNTA(A{row_number}:E{row_number})=0,"",ROUND(D{row_number}+E{row_number},6))',
                ),
                CellSpec(
                    style_id=CALCULATED_STYLE_ID,
                    formula=f'IF(F{row_number}="","",IF(ABS(F{row_number}-1)<=0.000001,"OK","CHECK"))',
                ),
            )
        )
    return SheetSpec(
        name="MDS_Mix",
        rows=tuple(rows),
        column_widths=(18, 18, 14, 16, 16, 12, 12),
        freeze_cell="A2",
        auto_filter_ref="A1:G1",
        data_validations=(
            DataValidationSpec(f"B2:B{MIX_TEMPLATE_ROWS + 1}", "list", "Geography_Master!$A$2:$A$101"),
            DataValidationSpec(f"C2:C{MIX_TEMPLATE_ROWS + 1}", "whole", "1", "240", operator="between"),
            DataValidationSpec(f"D2:E{MIX_TEMPLATE_ROWS + 1}", "decimal", "0", "1", operator="between"),
        ),
    )


def _build_annual_to_monthly_profiles_sheet() -> SheetSpec:
    headers = (
        "profile_id",
        "module",
        "geography_code",
        "segment_code",
        "profile_type",
        "month_1_weight",
        "month_2_weight",
        "month_3_weight",
        "month_4_weight",
        "month_5_weight",
        "month_6_weight",
        "month_7_weight",
        "month_8_weight",
        "month_9_weight",
        "month_10_weight",
        "month_11_weight",
        "month_12_weight",
        "sum_check",
        "status",
        "notes",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    for row_number in range(2, PROFILE_TEMPLATE_ROWS + 2):
        profile = STARTER_MONTHLYIZATION_PROFILES[row_number - 2] if row_number - 2 < len(STARTER_MONTHLYIZATION_PROFILES) else None
        if profile is None:
            profile_id, module, geography_code, segment_code, profile_type, weights, notes = (
                "",
                "",
                "",
                "",
                "",
                tuple("" for _ in range(12)),
                "",
            )
        else:
            profile_id, module, geography_code, segment_code, profile_type, weights, notes = profile
        rows.append(
            (
                CellSpec(profile_id, EDITABLE_STYLE_ID) if profile_id else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(module, EDITABLE_STYLE_ID) if module else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(geography_code, EDITABLE_STYLE_ID)
                if geography_code
                else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(segment_code, EDITABLE_STYLE_ID) if segment_code else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(profile_type, EDITABLE_STYLE_ID) if profile_type else CellSpec(style_id=EDITABLE_STYLE_ID),
                *(
                    CellSpec(weight, EDITABLE_STYLE_ID) if weight != "" else CellSpec(style_id=EDITABLE_STYLE_ID)
                    for weight in weights
                ),
                CellSpec(
                    style_id=CALCULATED_DECIMAL_STYLE_ID,
                    formula=f'IF(COUNTA(A{row_number}:Q{row_number})=0,"",ROUND(SUM(F{row_number}:Q{row_number}),6))',
                ),
                CellSpec(
                    style_id=CALCULATED_STYLE_ID,
                    formula=f'IF(R{row_number}="","",IF(ABS(R{row_number}-100)<=0.000001,"OK","CHECK"))',
                ),
                CellSpec(notes, EDITABLE_WRAP_STYLE_ID) if notes else CellSpec(style_id=EDITABLE_WRAP_STYLE_ID),
            )
        )
    return SheetSpec(
        name="Annual_to_Monthly_Profiles",
        rows=tuple(rows),
        column_widths=(24, 18, 18, 18, 22, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 54),
        freeze_cell="A2",
        auto_filter_ref="A1:T1",
        data_validations=(
            DataValidationSpec(f"B2:B{PROFILE_TEMPLATE_ROWS + 1}", "list", "Lookup_Lists!$C$2:$C$5"),
            DataValidationSpec(f"C2:C{PROFILE_TEMPLATE_ROWS + 1}", "list", "Geography_Master!$A$2:$A$101"),
            DataValidationSpec(f"D2:D{PROFILE_TEMPLATE_ROWS + 1}", "list", "Lookup_Lists!$F$2:$F$7"),
            DataValidationSpec(f"E2:E{PROFILE_TEMPLATE_ROWS + 1}", "list", "Lookup_Lists!$E$2:$E$5"),
            DataValidationSpec(f"F2:Q{PROFILE_TEMPLATE_ROWS + 1}", "decimal", "0", "100", operator="between"),
        ),
    )


def _build_cml_prevalent_assumptions_sheet() -> SheetSpec:
    headers = (
        "scenario_name",
        "geography_code",
        "year_index",
        "calendar_year",
        "addressable_prevalent_pool_annual",
        "fallback_patients_treated_annual",
        "monthlyization_profile_id",
        "launch_month_index",
        "duration_months",
        "exhaustion_rule",
        "notes",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    example_values = {
        2: ("US", 1, 120.0, 60.0, "CML_PREVALENT_LAUNCH", 1, 24, "track_vs_pool", "Fallback example row for the first bolus year."),
        3: ("US", 2, 180.0, 80.0, "CML_PREVALENT_PEAK", 1, 24, "track_vs_pool", "Example peak year."),
        4: ("EU", 1, 90.0, 45.0, "CML_PREVALENT_TAIL", 4, 12, "track_vs_pool", "Example with a delayed launch month."),
    }
    for row_number in range(2, CML_PREVALENT_ASSUMPTION_ROWS + 2):
        geography_code, year_index, annual_pool, fallback_patients, profile_id, launch_month_index, duration_months, exhaustion_rule, notes = example_values.get(
            row_number,
            ("", "", "", "", "", "", "", "", ""),
        )
        rows.append(
            (
                CellSpec(style_id=CALCULATED_STYLE_ID, formula="Inputs!$B$2"),
                CellSpec(geography_code, EDITABLE_STYLE_ID) if geography_code else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(year_index, EDITABLE_STYLE_ID) if year_index != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(
                    style_id=CALCULATED_STYLE_ID,
                    formula=f'IF($C{row_number}="","",YEAR(Inputs!$B$5)+$C{row_number}-1)',
                ),
                CellSpec(annual_pool, EDITABLE_STYLE_ID) if annual_pool != "" else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(fallback_patients, EDITABLE_STYLE_ID)
                if fallback_patients != ""
                else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(profile_id, EDITABLE_STYLE_ID) if profile_id else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(launch_month_index, EDITABLE_STYLE_ID)
                if launch_month_index != ""
                else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(duration_months, EDITABLE_STYLE_ID)
                if duration_months != ""
                else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(exhaustion_rule, EDITABLE_STYLE_ID) if exhaustion_rule else CellSpec(style_id=EDITABLE_STYLE_ID),
                CellSpec(notes, EDITABLE_WRAP_STYLE_ID) if notes else CellSpec(style_id=EDITABLE_WRAP_STYLE_ID),
            )
        )
    return SheetSpec(
        name="CML_Prevalent_Assumptions",
        rows=tuple(rows),
        column_widths=(18, 18, 12, 16, 28, 28, 24, 18, 18, 20, 48),
        freeze_cell="A2",
        auto_filter_ref="A1:K1",
        data_validations=(
            DataValidationSpec(f"B2:B{CML_PREVALENT_ASSUMPTION_ROWS + 1}", "list", "Geography_Master!$A$2:$A$101"),
            DataValidationSpec(
                f"C2:C{CML_PREVALENT_ASSUMPTION_ROWS + 1}",
                "whole",
                "1",
                "25",
                operator="between",
            ),
            DataValidationSpec(
                f"G2:G{CML_PREVALENT_ASSUMPTION_ROWS + 1}",
                "list",
                "Lookup_Lists!$D$2:$D$7",
            ),
            DataValidationSpec(
                f"H2:H{CML_PREVALENT_ASSUMPTION_ROWS + 1}",
                "whole",
                "1",
                "240",
                operator="between",
            ),
            DataValidationSpec(
                f"I2:I{CML_PREVALENT_ASSUMPTION_ROWS + 1}",
                "whole",
                "1",
                "240",
                operator="between",
            ),
            DataValidationSpec(
                f"J2:J{CML_PREVALENT_ASSUMPTION_ROWS + 1}",
                "list",
                "Lookup_Lists!$K$2:$K$4",
            ),
        ),
    )


def _build_monthlyized_output_sheet() -> SheetSpec:
    headers = (
        "scenario_name",
        "geography_code",
        "module",
        "segment_code",
        "month_index",
        "calendar_month",
        "patients_treated_monthly",
        "source_frequency",
        "source_grain",
        "source_sheet",
        "profile_id_used",
        "notes",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    rows.append(
        (
            CellSpec(style_id=CALCULATED_STYLE_ID, formula="Inputs!$B$2"),
            CellSpec("REFERENCE_ONLY", CALCULATED_STYLE_ID),
            CellSpec("REFERENCE_ONLY", CALCULATED_STYLE_ID),
            CellSpec("REFERENCE_ONLY", CALCULATED_STYLE_ID),
            CellSpec(style_id=CALCULATED_STYLE_ID),
            CellSpec(style_id=CALCULATED_DATE_STYLE_ID),
            CellSpec(style_id=CALCULATED_DECIMAL_STYLE_ID),
            CellSpec(style_id=CALCULATED_STYLE_ID, formula="Inputs!$B$4"),
            CellSpec(style_id=CALCULATED_STYLE_ID, formula="Inputs!$B$3"),
            CellSpec("CSV export is authoritative", CALCULATED_STYLE_ID),
            CellSpec(style_id=CALCULATED_STYLE_ID),
            CellSpec(
                "Reference tab only in Phase 1 unless a future exporter populates it. The authoritative normalized workbook export is monthlyized_output.csv written by the importer.",
                WRAP_STYLE_ID,
            ),
        )
    )
    rows.extend(
        _blank_input_rows(
            headers=len(headers),
            count=24,
            style_map=(
                CALCULATED_STYLE_ID,
                CALCULATED_STYLE_ID,
                CALCULATED_STYLE_ID,
                CALCULATED_STYLE_ID,
                CALCULATED_STYLE_ID,
                CALCULATED_DATE_STYLE_ID,
                CALCULATED_DECIMAL_STYLE_ID,
                CALCULATED_STYLE_ID,
                CALCULATED_STYLE_ID,
                CALCULATED_STYLE_ID,
                CALCULATED_STYLE_ID,
                CALCULATED_STYLE_ID,
            ),
        )
    )
    return SheetSpec(
        name="Monthlyized_Output",
        rows=tuple(rows),
        column_widths=(18, 18, 18, 18, 14, 18, 22, 18, 18, 24, 24, 56),
        freeze_cell="A2",
        auto_filter_ref="A1:L1",
    )


def _build_lookup_lists_sheet() -> SheetSpec:
    headers = (
        "forecast_grain",
        "forecast_frequency",
        "module",
        "profile_id",
        "profile_type",
        "segment_code_all",
        "aml_segments",
        "mds_segments",
        "yes_no",
        "true_false",
        "exhaustion_rule",
    )
    rows: list[tuple[CellSpec, ...]] = [_header_row(headers)]
    lookup_values = (
        ("module_level", "monthly", "AML", "FLAT_12", "FLAT_12", "1L_fit", "1L_fit", "HR_MDS", "yes", "true", "track_vs_pool"),
        ("segment_level", "annual", "MDS", "LAUNCH_RAMP", "LAUNCH_RAMP", "1L_unfit", "1L_unfit", "LR_MDS", "no", "false", "placeholder_metadata_only"),
        ("", "", "CML_Incident", "STEADY_STATE", "STEADY_STATE", "RR", "RR", "", "", "", "validate_only"),
        ("", "", "CML_Prevalent", "CML_PREVALENT_LAUNCH", "CML_PREVALENT_BOLUS", "HR_MDS", "", "", "", "", ""),
        ("", "", "", "CML_PREVALENT_PEAK", "", "LR_MDS", "", "", "", "", ""),
        ("", "", "", "CML_PREVALENT_TAIL", "", "ALL", "", "", "", "", ""),
        ("", "", "", "", "", "", "", "", "", "", ""),
    )
    for row in lookup_values:
        rows.append(tuple(CellSpec(value) for value in row))
    return SheetSpec(
        name="Lookup_Lists",
        rows=tuple(rows),
        column_widths=(18, 18, 20, 24, 24, 18, 16, 16, 12, 12, 24),
        freeze_cell="A2",
        auto_filter_ref="A1:K1",
    )


def _header_row(values: tuple[str, ...]) -> tuple[CellSpec, ...]:
    return tuple(CellSpec(value, HEADER_STYLE_ID) for value in values)


def _blank_input_rows(
    *,
    headers: int,
    count: int,
    style_map: tuple[int, ...],
) -> Iterable[tuple[CellSpec, ...]]:
    if len(style_map) != headers:
        raise ValueError("style_map length must match header count.")
    for _ in range(count):
        yield tuple(CellSpec(style_id=style_id) for style_id in style_map)


def _build_content_types_xml(sheet_count: int) -> str:
    overrides = [
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>',
        '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>',
        '<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>',
    ]
    for sheet_index in range(1, sheet_count + 1):
        overrides.append(
            f'<Override PartName="/xl/worksheets/sheet{sheet_index}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        )
    return (
        f"{XML_DECLARATION}"
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        f'{"".join(overrides)}'
        "</Types>"
    )


def _build_root_relationships_xml() -> str:
    return (
        f"{XML_DECLARATION}"
        f'<Relationships xmlns="{PKG_REL_NS}">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>'
        '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>'
        "</Relationships>"
    )


def _build_core_props_xml() -> str:
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return (
        f"{XML_DECLARATION}"
        f'<cp:coreProperties xmlns:cp="{CORE_PROPS_NS}" xmlns:dc="{DC_NS}" xmlns:dcterms="{DCTERMS_NS}" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="{XSI_NS}">'
        "<dc:title>CBX250 Commercial Forecast Template</dc:title>"
        "<dc:creator>Codex</dc:creator>"
        "<cp:lastModifiedBy>Codex</cp:lastModifiedBy>"
        f'<dcterms:created xsi:type="dcterms:W3CDTF">{timestamp}</dcterms:created>'
        f'<dcterms:modified xsi:type="dcterms:W3CDTF">{timestamp}</dcterms:modified>'
        "</cp:coreProperties>"
    )


def _build_app_props_xml(sheets: tuple[SheetSpec, ...]) -> str:
    part_names = "".join(f"<vt:lpstr>{escape(sheet.name)}</vt:lpstr>" for sheet in sheets)
    return (
        f"{XML_DECLARATION}"
        f'<Properties xmlns="{EXT_PROPS_NS}" xmlns:vt="{VTYPES_NS}">'
        "<Application>Codex</Application>"
        f"<TitlesOfParts><vt:vector size=\"{len(sheets)}\" baseType=\"lpstr\">{part_names}</vt:vector></TitlesOfParts>"
        "<Company>OpenAI</Company>"
        "<AppVersion>1.0</AppVersion>"
        "</Properties>"
    )


def _build_workbook_xml(sheets: tuple[SheetSpec, ...]) -> str:
    sheet_nodes = []
    for sheet_index, sheet in enumerate(sheets, start=1):
        sheet_nodes.append(
            f'<sheet name="{escape(sheet.name)}" sheetId="{sheet_index}" r:id="rId{sheet_index}"/>'
        )
    return (
        f"{XML_DECLARATION}"
        f'<workbook xmlns="{SHEET_MAIN_NS}" xmlns:r="{DOC_REL_NS}">'
        "<bookViews><workbookView xWindow=\"0\" yWindow=\"0\" windowWidth=\"24000\" windowHeight=\"12000\"/></bookViews>"
        f"<sheets>{''.join(sheet_nodes)}</sheets>"
        '<calcPr calcId="171027" fullCalcOnLoad="1"/>'
        "</workbook>"
    )


def _build_workbook_relationships_xml(sheet_count: int) -> str:
    relationships = []
    for sheet_index in range(1, sheet_count + 1):
        relationships.append(
            f'<Relationship Id="rId{sheet_index}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{sheet_index}.xml"/>'
        )
    relationships.append(
        f'<Relationship Id="rId{sheet_count + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
    )
    return (
        f"{XML_DECLARATION}"
        f'<Relationships xmlns="{PKG_REL_NS}">{"".join(relationships)}</Relationships>'
    )


def _build_styles_xml() -> str:
    return f"""{XML_DECLARATION}
<styleSheet xmlns="{SHEET_MAIN_NS}">
  <numFmts count="2">
    <numFmt numFmtId="164" formatCode="yyyy-mm-dd"/>
    <numFmt numFmtId="165" formatCode="0.000000"/>
  </numFmts>
  <fonts count="3">
    <font>
      <sz val="11"/>
      <color rgb="FF000000"/>
      <name val="Calibri"/>
      <family val="2"/>
    </font>
    <font>
      <b/>
      <sz val="11"/>
      <color rgb="FFFFFFFF"/>
      <name val="Calibri"/>
      <family val="2"/>
    </font>
    <font>
      <b/>
      <sz val="11"/>
      <color rgb="FF1F2937"/>
      <name val="Calibri"/>
      <family val="2"/>
    </font>
  </fonts>
  <fills count="6">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF0F4C81"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFF2CC"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFF3F4F6"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFD9EAF7"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="1">
    <border>
      <left/>
      <right/>
      <top/>
      <bottom/>
      <diagonal/>
    </border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="10">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFill="1" applyFont="1">
      <alignment horizontal="center" vertical="center"/>
    </xf>
    <xf numFmtId="0" fontId="0" fillId="3" borderId="0" xfId="0" applyFill="1">
      <alignment vertical="top"/>
    </xf>
    <xf numFmtId="0" fontId="0" fillId="4" borderId="0" xfId="0" applyFill="1">
      <alignment vertical="top"/>
    </xf>
    <xf numFmtId="0" fontId="2" fillId="5" borderId="0" xfId="0" applyFill="1" applyFont="1">
      <alignment vertical="top"/>
    </xf>
    <xf numFmtId="164" fontId="0" fillId="3" borderId="0" xfId="0" applyFill="1" applyNumberFormat="1">
      <alignment vertical="top"/>
    </xf>
    <xf numFmtId="164" fontId="0" fillId="4" borderId="0" xfId="0" applyFill="1" applyNumberFormat="1">
      <alignment vertical="top"/>
    </xf>
    <xf numFmtId="165" fontId="0" fillId="4" borderId="0" xfId="0" applyFill="1" applyNumberFormat="1">
      <alignment vertical="top"/>
    </xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1">
      <alignment wrapText="1" vertical="top"/>
    </xf>
    <xf numFmtId="0" fontId="0" fillId="3" borderId="0" xfId="0" applyFill="1" applyAlignment="1">
      <alignment wrapText="1" vertical="top"/>
    </xf>
  </cellXfs>
  <cellStyles count="1">
    <cellStyle name="Normal" xfId="0" builtinId="0"/>
  </cellStyles>
</styleSheet>
"""


def _build_worksheet_xml(sheet: SheetSpec) -> str:
    row_xml = []
    max_columns = max(len(row) for row in sheet.rows)
    for row_index, row in enumerate(sheet.rows, start=1):
        cells = []
        for column_index, cell in enumerate(row, start=1):
            cell_xml = _build_cell_xml(row_index=row_index, column_index=column_index, cell=cell)
            if cell_xml:
                cells.append(cell_xml)
        row_xml.append(f'<row r="{row_index}">{"".join(cells)}</row>')

    sheet_views = '<sheetViews><sheetView workbookViewId="0">'
    if sheet.freeze_cell is not None:
        split_row = _row_number(sheet.freeze_cell) - 1
        sheet_views += (
            f'<pane ySplit="{split_row}" topLeftCell="{sheet.freeze_cell}" activePane="bottomLeft" state="frozen"/>'
            f'<selection pane="bottomLeft" activeCell="{sheet.freeze_cell}" sqref="{sheet.freeze_cell}"/>'
        )
    sheet_views += "</sheetView></sheetViews>"

    cols = "".join(
        f'<col min="{index}" max="{index}" width="{width}" customWidth="1"/>'
        for index, width in enumerate(sheet.column_widths, start=1)
    )
    data_validations_xml = _build_data_validations_xml(sheet.data_validations)
    auto_filter_xml = f'<autoFilter ref="{sheet.auto_filter_ref}"/>' if sheet.auto_filter_ref else ""
    max_cell = f"{_column_letter(max_columns)}{len(sheet.rows)}"

    return (
        f"{XML_DECLARATION}"
        f'<worksheet xmlns="{SHEET_MAIN_NS}">'
        f'<dimension ref="A1:{max_cell}"/>'
        f"{sheet_views}"
        '<sheetFormatPr defaultRowHeight="18"/>'
        f"<cols>{cols}</cols>"
        f"<sheetData>{''.join(row_xml)}</sheetData>"
        f"{auto_filter_xml}"
        f"{data_validations_xml}"
        '<pageMargins left="0.7" right="0.7" top="0.75" bottom="0.75" header="0.3" footer="0.3"/>'
        "</worksheet>"
    )


def _build_data_validations_xml(validations: tuple[DataValidationSpec, ...]) -> str:
    if not validations:
        return ""
    nodes = []
    for validation in validations:
        attributes = [
            f'type="{validation.validation_type}"',
            f'sqref="{validation.sqref}"',
            'showErrorMessage="1"',
            f'allowBlank="{1 if validation.allow_blank else 0}"',
        ]
        if validation.operator is not None:
            attributes.append(f'operator="{validation.operator}"')
        formula2 = (
            f"<formula2>{escape(validation.formula2)}</formula2>" if validation.formula2 is not None else ""
        )
        nodes.append(
            f'<dataValidation {" ".join(attributes)}><formula1>{escape(validation.formula1)}</formula1>{formula2}</dataValidation>'
        )
    return f'<dataValidations count="{len(nodes)}">{"".join(nodes)}</dataValidations>'


def _build_cell_xml(*, row_index: int, column_index: int, cell: CellSpec) -> str:
    ref = f"{_column_letter(column_index)}{row_index}"
    style_attr = f' s="{cell.style_id}"' if cell.style_id else ""

    if cell.formula is not None:
        value_xml = ""
        if cell.value is not None:
            value_xml = f"<v>{_serialize_value(cell.value)}</v>"
        return f'<c r="{ref}"{style_attr}><f>{escape(cell.formula)}</f>{value_xml}</c>'

    if cell.value is None:
        return f'<c r="{ref}"{style_attr}/>'

    if isinstance(cell.value, date):
        return f'<c r="{ref}"{style_attr}><v>{_excel_date_serial(cell.value)}</v></c>'

    if isinstance(cell.value, (int, float)) and not isinstance(cell.value, bool):
        return f'<c r="{ref}"{style_attr}><v>{_serialize_value(cell.value)}</v></c>'

    text = escape(str(cell.value))
    return f'<c r="{ref}" t="inlineStr"{style_attr}><is><t xml:space="preserve">{text}</t></is></c>'


def _serialize_value(value: str | int | float | date) -> str:
    if isinstance(value, date):
        return str(_excel_date_serial(value))
    if isinstance(value, float):
        return format(value, ".15g")
    return str(value)


def _excel_date_serial(value: date) -> int:
    epoch = date(1899, 12, 30)
    return (value - epoch).days


def _row_number(cell_ref: str) -> int:
    return int("".join(character for character in cell_ref if character.isdigit()))


def _column_letter(column_index: int) -> str:
    result = ""
    current = column_index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result = chr(65 + remainder) + result
    return result
