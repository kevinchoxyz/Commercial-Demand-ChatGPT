from __future__ import annotations

from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile

from cbx250_model.inputs.excel_template import build_commercial_forecast_template

MAIN_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


def _sheet_path_map(workbook_path: Path) -> dict[str, str]:
    with zipfile.ZipFile(workbook_path) as zf:
        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        relationships = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))

    rel_lookup = {
        relationship.attrib["Id"]: relationship.attrib["Target"]
        for relationship in relationships.findall(f"{{{PKG_REL_NS}}}Relationship")
    }
    sheet_map: dict[str, str] = {}
    for sheet in workbook.findall("a:sheets/a:sheet", MAIN_NS):
        rel_id = sheet.attrib[f"{{{REL_NS}}}id"]
        sheet_map[sheet.attrib["name"]] = f"xl/{rel_lookup[rel_id]}"
    return sheet_map


def _row_values(workbook_path: Path, worksheet_path: str, row_number: int) -> list[str]:
    with zipfile.ZipFile(workbook_path) as zf:
        worksheet = ET.fromstring(zf.read(worksheet_path))

    for row in worksheet.findall(".//a:sheetData/a:row", MAIN_NS):
        if int(row.attrib["r"]) != row_number:
            continue
        values: list[str] = []
        for cell in row.findall("a:c", MAIN_NS):
            if cell.attrib.get("t") == "inlineStr":
                values.append("".join(node.text or "" for node in cell.findall(".//a:t", MAIN_NS)))
            else:
                values.append(cell.findtext("a:v", default="", namespaces=MAIN_NS))
        return values
    raise AssertionError(f"Row {row_number} not found in {worksheet_path}.")


def _cell_formula(workbook_path: Path, worksheet_path: str, cell_ref: str) -> str:
    with zipfile.ZipFile(workbook_path) as zf:
        worksheet = ET.fromstring(zf.read(worksheet_path))

    for cell in worksheet.findall(".//a:sheetData/a:row/a:c", MAIN_NS):
        if cell.attrib["r"] == cell_ref:
            return cell.findtext("a:f", default="", namespaces=MAIN_NS)
    raise AssertionError(f"Cell {cell_ref} not found in {worksheet_path}.")


def test_build_commercial_forecast_template_creates_expected_workbook(tmp_path: Path) -> None:
    output_path = tmp_path / "CBX250_Commercial_Forecast_Template.xlsx"

    build_commercial_forecast_template(output_path)

    assert output_path.exists()

    sheet_map = _sheet_path_map(output_path)
    assert list(sheet_map) == [
        "Instructions",
        "Inputs",
        "Geography_Master",
        "ModuleLevel_Forecast",
        "SegmentLevel_Forecast",
        "Annual_ModuleLevel_Forecast",
        "Annual_SegmentLevel_Forecast",
        "AML_Mix",
        "MDS_Mix",
        "Annual_to_Monthly_Profiles",
        "CML_Prevalent_Assumptions",
        "Monthlyized_Output",
        "Lookup_Lists",
    ]

    assert _row_values(output_path, sheet_map["Inputs"], 4)[:2] == ["forecast_frequency", "monthly"]
    assert _row_values(output_path, sheet_map["Instructions"], 5) == [
        "Mix override rule",
        "Use month_index mix rows only when the within-year segment mix differs from the standard annual mix.",
        "If both are present, month_index rows override year_index rows for the same geography and month. CML modules stay separate and do not use AML/MDS mix logic.",
    ]
    assert _row_values(output_path, sheet_map["Instructions"], 7) == [
        "CML_Prevalent precedence",
        "If explicit CML_Prevalent forecast rows exist in the active forecast tab, they are the primary demand input.",
        "CML_Prevalent_Assumptions is optional for explicit demand import. If provided, it generates the validation pool; if explicit CML_Prevalent rows are missing, the assumptions sheet can generate a fallback monthly series.",
    ]
    assert _row_values(output_path, sheet_map["AML_Mix"], 1) == [
        "scenario_name",
        "geography_code",
        "year_index",
        "month_index",
        "1L_fit_share",
        "1L_unfit_share",
        "RR_share",
        "sum_check",
        "status",
    ]
    assert _row_values(output_path, sheet_map["AML_Mix"], 2)[1:7] == ["US", "1", "", "0.4", "0.35", "0.25"]
    assert _row_values(output_path, sheet_map["MDS_Mix"], 1) == [
        "scenario_name",
        "geography_code",
        "year_index",
        "month_index",
        "HR_MDS_share",
        "LR_MDS_share",
        "sum_check",
        "status",
    ]
    assert _row_values(output_path, sheet_map["MDS_Mix"], 2)[1:6] == ["EU", "1", "", "0.6", "0.4"]
    assert _row_values(output_path, sheet_map["Annual_to_Monthly_Profiles"], 2)[:5] == [
        "FLAT_12",
        "",
        "",
        "",
        "FLAT_12",
    ]
    assert _row_values(output_path, sheet_map["Annual_to_Monthly_Profiles"], 5)[:5] == [
        "CML_PREVALENT_LAUNCH",
        "CML_Prevalent",
        "",
        "ALL",
        "CML_PREVALENT_BOLUS",
    ]
    assert _row_values(output_path, sheet_map["CML_Prevalent_Assumptions"], 1) == [
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
    ]
    assert _row_values(output_path, sheet_map["Monthlyized_Output"], 1) == [
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
    ]
    assert _row_values(output_path, sheet_map["Monthlyized_Output"], 2)[9:] == [
        "CSV export is authoritative",
        "",
        "Reference tab only in Phase 1 unless a future exporter populates it. The authoritative normalized workbook export is monthlyized_output.csv written by the importer.",
    ]
    assert _row_values(output_path, sheet_map["Lookup_Lists"], 2) == [
        "module_level",
        "monthly",
        "AML",
        "FLAT_12",
        "FLAT_12",
        "1L_fit",
        "1L_fit",
        "HR_MDS",
        "yes",
        "true",
        "track_vs_pool",
    ]

    assert _cell_formula(output_path, sheet_map["ModuleLevel_Forecast"], "E2") == (
        'IF(OR($D2="",Inputs!$B$5=""),"",EDATE(Inputs!$B$5,$D2-1))'
    )
    assert _cell_formula(output_path, sheet_map["Annual_to_Monthly_Profiles"], "R2") == (
        'IF(COUNTA(A2:Q2)=0,"",ROUND(SUM(F2:Q2),6))'
    )
    assert _cell_formula(output_path, sheet_map["Annual_to_Monthly_Profiles"], "S2") == (
        'IF(R2="","",IF(ABS(R2-100)<=0.000001,"OK","CHECK"))'
    )
