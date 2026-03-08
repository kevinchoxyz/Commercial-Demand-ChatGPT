from __future__ import annotations

from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile

from cbx250_model.inputs.assumptions_template import build_model_assumptions_template

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


def test_build_model_assumptions_template_creates_expected_workbook(tmp_path: Path) -> None:
    output_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"

    build_model_assumptions_template(output_path)

    assert output_path.exists()

    sheet_map = _sheet_path_map(output_path)
    assert list(sheet_map) == [
        "Instructions",
        "Scenario_Controls",
        "Launch_Timing",
        "Dosing_Assumptions",
        "Product_Parameters",
        "Yield_Assumptions",
        "Packaging_and_Vialing",
        "SS_Assumptions",
        "CML_Prevalent_Assumptions",
        "Trade_Inventory_FutureHooks",
        "Lookup_Lists",
    ]

    assert _row_values(output_path, sheet_map["Instructions"], 5) == [
        "Scope precedence",
        "When a sheet supports both scenario-level defaults and module-specific overrides, module-specific override rows take precedence over scenario-default rows.",
        "Current generated Phase 2 config already applies this precedence for module FG mg per unit. For ds_qty_per_dp_unit_mg and ds_overage_factor, module overrides are preserved in normalized artifacts but the current engine still consumes the scenario default only.",
    ]
    assert _row_values(output_path, sheet_map["Scenario_Controls"], 2) == [
        "BASE_2029",
        "Approved base-case assumptions bridge",
        "yes",
        "module_level",
        "monthly",
        "fixed",
        "USD",
        "Edit this row instead of hand-editing Phase 2 TOML files.",
    ]
    assert _row_values(output_path, sheet_map["Dosing_Assumptions"], 1) == [
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
    ]
    assert _row_values(output_path, sheet_map["Dosing_Assumptions"], 2)[1:] == [
        "AML",
        "ALL",
        "ALL",
        "fixed",
        "0.15",
        "0.0023",
        "80",
        "4.33",
        "false",
        "PLACEHOLDER_INACTIVE",
        "false",
        "0",
        "1",
        "0",
        "yes",
        "Approved AML base-case cadence QW -> 4.33.",
    ]
    assert _row_values(output_path, sheet_map["Product_Parameters"], 2)[1:] == [
        "scenario_default",
        "ALL",
        "ALL",
        "1",
        "",
        "",
        "1",
        "yes",
        "Approved scenario default row. Current engine consumes this ds_qty_per_dp_unit_mg default.",
    ]
    assert _row_values(output_path, sheet_map["Yield_Assumptions"], 2)[1:] == [
        "scenario_default",
        "ALL",
        "ALL",
        "0.9",
        "0.98",
        "1",
        "1",
        "0.05",
        "yes",
        "Approved deterministic scenario default row.",
    ]
    assert _row_values(output_path, sheet_map["Packaging_and_Vialing"], 2)[1:9] == [
        "AML",
        "ALL",
        "patient_dose_ceiling",
        "true",
        "false",
        "1",
        "full_pack_consumed",
        "yes",
    ]
    assert _row_values(output_path, sheet_map["SS_Assumptions"], 2)[1:6] == [
        "ALL",
        "ALL",
        "1",
        "separate_sku_first",
        "yes",
    ]
    assert _row_values(output_path, sheet_map["CML_Prevalent_Assumptions"], 2)[1:] == [
        "US",
        "",
        "1",
        "1",
        "12",
        "PLACEHOLDER_PROFILE",
        "1",
        "3",
        "4",
        "track_vs_pool",
        "PLACEHOLDER",
        "no",
        "PLACEHOLDER populate approved US prevalent pool inputs before activating.",
    ]
    assert _cell_formula(output_path, sheet_map["Dosing_Assumptions"], "A2") == (
        'IF(Scenario_Controls!$A$2="","",Scenario_Controls!$A$2)'
    )
