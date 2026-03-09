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
        "Treatment_Duration_Assumptions",
        "Product_Parameters",
        "Yield_Assumptions",
        "Packaging_and_Vialing",
        "SS_Assumptions",
        "CML_Prevalent_Assumptions",
        "Trade_Inventory_FutureHooks",
        "Lookup_Lists",
    ]

    assert _row_values(output_path, sheet_map["Instructions"], 5) == [
        "Phase 1 demand basis",
        "patient_starts is the preferred default operating mode and represents the approved base-case commercial input path.",
        "treated_census remains supported for backward compatibility and special cases only. Do not apply duration logic on top of already treated-census inputs.",
    ]
    assert _row_values(output_path, sheet_map["Instructions"], 7) == [
        "Current engine wiring",
        "The importer generates machine-readable CSV artifacts plus generated_phase2_parameters.toml / generated_phase2_scenario.toml and generated_phase3_parameters.toml / generated_phase3_scenario.toml.",
        "Current wiring consumes: Scenario_Controls.demand_basis plus Treatment_Duration_Assumptions for Phase 1 starts-based mode; dose_basis_default, module-specific dosing values, module FG mg per unit, module FG vialing rule, global yields, DS quantity per DP unit default, DS overage default, SS ratio, and co_pack_mode for Phase 2; and active deterministic trade parameters from Trade_Inventory_FutureHooks for Phase 3.",
    ]
    assert _row_values(output_path, sheet_map["Scenario_Controls"], 2) == [
        "BASE_2029",
        "Approved base-case assumptions bridge",
        "yes",
        "module_level",
        "annual",
        "patient_starts",
        "fixed",
        "USD",
        "Edit this row instead of hand-editing config files. Seeded for the annual patient_starts base case.",
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
    assert _row_values(output_path, sheet_map["Treatment_Duration_Assumptions"], 1) == [
        "scenario_name",
        "module",
        "segment_code",
        "geography_code",
        "treatment_duration_months",
        "active_flag",
        "notes",
    ]
    assert _row_values(output_path, sheet_map["Treatment_Duration_Assumptions"], 2)[1:] == [
        "AML",
        "1L_fit",
        "ALL",
        "12",
        "yes",
        "Approved base-case duration default.",
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
    assert _row_values(output_path, sheet_map["Trade_Inventory_FutureHooks"], 1) == [
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
        "active_flag",
        "notes",
    ]
    assert _row_values(output_path, sheet_map["Trade_Inventory_FutureHooks"], 2)[1:] == [
        "scenario_default",
        "ALL",
        "ALL",
        "2.5",
        "1.5",
        "0",
        "6",
        "6",
        "1",
        "0",
        "false",
        "1",
        "0.25",
        "4",
        "8",
        "4.33",
        "",
        "",
        "",
        "",
        "yes",
        "Approved deterministic Phase 3 scenario defaults. Edit here instead of phase3_trade_layer.toml.",
    ]
    assert _cell_formula(output_path, sheet_map["Dosing_Assumptions"], "A2") == (
        'IF(Scenario_Controls!$A$2="","",Scenario_Controls!$A$2)'
    )
