from __future__ import annotations

from pathlib import Path
import csv
import json
import xml.etree.ElementTree as ET
import zipfile

import pytest

from cbx250_model.inputs.assumptions_import import import_model_assumptions_workbook
from cbx250_model.inputs.assumptions_template import build_model_assumptions_template
from cbx250_model.phase2.config_schema import load_phase2_config
from cbx250_model.phase3.config_schema import load_phase3_config
from cbx250_model.phase4.config_schema import load_phase4_config
from cbx250_model.phase5.config_schema import load_phase5_config

MAIN_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
MAIN_NS_URI = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
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


def _replace_zip_entry(workbook_path: Path, entry_path: str, payload: bytes) -> None:
    temp_path = workbook_path.with_suffix(".tmp.xlsx")
    with zipfile.ZipFile(workbook_path) as source_zip:
        with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED) as target_zip:
            for member in source_zip.infolist():
                content = payload if member.filename == entry_path else source_zip.read(member.filename)
                target_zip.writestr(member, content)
    temp_path.replace(workbook_path)


def _set_cell(workbook_path: Path, sheet_name: str, cell_ref: str, value: str | float | int | None) -> None:
    worksheet_path = _sheet_path_map(workbook_path)[sheet_name]
    with zipfile.ZipFile(workbook_path) as zf:
        worksheet = ET.fromstring(zf.read(worksheet_path))

    target_cell: ET.Element | None = None
    for cell in worksheet.findall(".//a:sheetData/a:row/a:c", MAIN_NS):
        if cell.attrib["r"] == cell_ref:
            target_cell = cell
            break
    if target_cell is None:
        raise AssertionError(f"Cell {cell_ref} not found in {sheet_name}.")

    for child in list(target_cell):
        target_cell.remove(child)

    if value is None or value == "":
        target_cell.attrib.pop("t", None)
    elif isinstance(value, (int, float)):
        target_cell.attrib.pop("t", None)
        value_node = ET.SubElement(target_cell, f"{{{MAIN_NS_URI}}}v")
        value_node.text = str(value)
    else:
        target_cell.attrib["t"] = "inlineStr"
        inline_node = ET.SubElement(target_cell, f"{{{MAIN_NS_URI}}}is")
        text_node = ET.SubElement(inline_node, f"{{{MAIN_NS_URI}}}t")
        text_node.text = value

    _replace_zip_entry(
        workbook_path,
        worksheet_path,
        ET.tostring(worksheet, encoding="utf-8", xml_declaration=True),
    )


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _find_row(rows: list[dict[str, str]], **criteria: str) -> dict[str, str]:
    for row in rows:
        if all(row[key] == value for key, value in criteria.items()):
            return row
    raise AssertionError(f"No row found matching {criteria!r}.")


def test_import_model_assumptions_workbook_happy_path_generates_artifacts_and_phase2_bridge(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"
    output_dir = tmp_path / "assumptions"

    build_model_assumptions_template(workbook_path)
    result = import_model_assumptions_workbook(workbook_path, output_dir=output_dir)

    assert result.context.scenario_name == "BASE_2029"
    assert result.context.demand_basis == "patient_starts"
    assert result.context.dose_basis_default == "fixed"
    assert result.row_counts["dosing_assumptions"] == 4
    assert result.row_counts["treatment_duration_assumptions"] == 7
    assert result.file_paths["generated_phase2_parameters"].exists()
    assert result.file_paths["generated_phase2_scenario"].exists()
    assert result.file_paths["generated_phase3_parameters"].exists()
    assert result.file_paths["generated_phase3_scenario"].exists()
    assert result.file_paths["generated_phase4_parameters"].exists()
    assert result.file_paths["generated_phase4_scenario"].exists()
    assert result.file_paths["generated_phase5_parameters"].exists()
    assert result.file_paths["generated_phase5_scenario"].exists()
    assert result.file_paths["treatment_duration_assumptions"].exists()

    config = load_phase2_config(result.file_paths["generated_phase2_scenario"])
    assert config.model.dose_basis == "fixed"
    assert config.get_module_settings("AML").fixed_dose_mg == 0.15
    assert config.get_module_settings("AML").doses_per_patient_per_month == 4.33
    assert config.get_module_settings("CML_Prevalent").doses_per_patient_per_month == 1.0
    assert config.ds.qty_per_dp_unit_mg == 1.0
    assert config.ds.overage_factor == 0.05
    assert config.ss.ratio_to_fg == 1.0
    phase3_config = load_phase3_config(result.file_paths["generated_phase3_scenario"])
    assert phase3_config.trade.sublayer1_target_weeks_on_hand == 2.5
    assert phase3_config.trade.initial_stocking_units_per_new_site == 6.0
    assert phase3_config.get_geography_defaults("US").site_activation_rate == 5.0
    assert phase3_config.get_launch_event("AML", "US").launch_month_index == 1
    phase4_config = load_phase4_config(result.file_paths["generated_phase4_scenario"])
    assert phase4_config.conversion.dp_to_fg_yield == 0.98
    assert phase4_config.review.bullwhip_amplification_threshold == 1.25
    assert phase4_config.fg.packaging_campaign_size_units == 50000.0
    assert phase4_config.ds.max_batch_size_kg == 4.0
    phase5_config = load_phase5_config(result.file_paths["generated_phase5_scenario"])
    assert phase5_config.starting_inventory.fg_units == 0.0
    assert phase5_config.shelf_life.ds_months == 48
    assert phase5_config.shelf_life.dp_months == 36
    assert phase5_config.shelf_life.fg_months == 36
    assert phase5_config.shelf_life.ss_months == 48
    assert phase5_config.policy.excess_inventory_threshold_months_of_cover == 18.0
    assert phase5_config.policy.fefo_enabled is True
    assert phase5_config.validation.reconcile_phase4_receipts is True

    summary = json.loads(result.file_paths["import_summary"].read_text(encoding="utf-8"))
    assert "Scenario_Controls.demand_basis and Treatment_Duration_Assumptions -> Phase 1 starts-based treated census build when demand_basis=patient_starts." in summary["wired_into_current_engine"]
    assert "Product_Parameters scenario_default + module_override -> fg_mg_per_unit resolution" in summary["wired_into_current_engine"]
    assert "Trade_Inventory_FutureHooks scenario_default / geography_default / launch_event rows -> active deterministic Phase 3 config generation" in summary["wired_into_current_engine"]
    assert "Trade_Inventory_FutureHooks scenario_default row plus Product_Parameters / Yield_Assumptions / SS_Assumptions scenario defaults -> active deterministic Phase 4 config generation" in summary["wired_into_current_engine"]
    assert "Trade_Inventory_FutureHooks scenario_default row plus Product_Parameters / Yield_Assumptions / SS_Assumptions scenario defaults -> active deterministic Phase 5 config generation" in summary["wired_into_current_engine"]
    assert "Broader future-phase execution, financial, and Monte Carlo logic remains deferred even though the assumptions workbook now feeds the active deterministic Phase 3, Phase 4, and Phase 5 configs." in summary["future_ready_only"]


def test_import_model_assumptions_workbook_missing_required_field_fails_with_context(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"

    build_model_assumptions_template(workbook_path)
    _set_cell(workbook_path, "Scenario_Controls", "G2", "")

    with pytest.raises(ValueError, match="Scenario_Controls row 2 is missing required value for 'dose_basis_default'"):
        import_model_assumptions_workbook(workbook_path)


def test_import_model_assumptions_workbook_invalid_lookup_value_fails(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"

    build_model_assumptions_template(workbook_path)
    _set_cell(workbook_path, "Packaging_and_Vialing", "D2", "bad_rule")

    with pytest.raises(ValueError, match="Packaging_and_Vialing row 2 has unsupported fg_vialing_rule"):
        import_model_assumptions_workbook(workbook_path)


def test_import_model_assumptions_workbook_duplicate_scoped_row_fails(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"

    build_model_assumptions_template(workbook_path)
    _set_cell(workbook_path, "Product_Parameters", "B3", "scenario_default")
    _set_cell(workbook_path, "Product_Parameters", "C3", "ALL")
    _set_cell(workbook_path, "Product_Parameters", "D3", "ALL")
    _set_cell(workbook_path, "Product_Parameters", "I3", "yes")

    with pytest.raises(ValueError, match="Product_Parameters row 3 duplicates active scope"):
        import_model_assumptions_workbook(workbook_path)


def test_import_model_assumptions_workbook_preserves_ds_qty_module_override_but_uses_scenario_default(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"
    output_dir = tmp_path / "assumptions"

    build_model_assumptions_template(workbook_path)
    _set_cell(workbook_path, "Product_Parameters", "E3", 1.2)
    _set_cell(workbook_path, "Product_Parameters", "I3", "yes")

    result = import_model_assumptions_workbook(workbook_path, output_dir=output_dir)

    product_rows = _read_csv(result.file_paths["product_parameters"])
    assert _find_row(
        product_rows,
        parameter_scope="scenario_default",
        module="ALL",
        geography_code="ALL",
    )["ds_qty_per_dp_unit_mg"] == "1"
    assert _find_row(
        product_rows,
        parameter_scope="module_override",
        module="AML",
        geography_code="ALL",
    )["ds_qty_per_dp_unit_mg"] == "1.2"

    snapshot = json.loads(result.file_paths["resolved_phase2_snapshot"].read_text(encoding="utf-8"))
    assert snapshot["resolved_phase2"]["ds"]["qty_per_dp_unit_mg"] == 1.0
    assert any("ds_qty_per_dp_unit_mg still uses the scenario_default row only" in warning for warning in snapshot["warnings"])


def test_import_model_assumptions_workbook_preserves_ds_overage_module_override_but_uses_scenario_default(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"
    output_dir = tmp_path / "assumptions"

    build_model_assumptions_template(workbook_path)
    _set_cell(workbook_path, "Yield_Assumptions", "I3", 0.12)
    _set_cell(workbook_path, "Yield_Assumptions", "J3", "yes")

    result = import_model_assumptions_workbook(workbook_path, output_dir=output_dir)

    yield_rows = _read_csv(result.file_paths["yield_assumptions"])
    assert _find_row(
        yield_rows,
        parameter_scope="module_override",
        module="AML",
        geography_code="ALL",
    )["ds_overage_factor"] == "0.12"

    snapshot = json.loads(result.file_paths["resolved_phase2_snapshot"].read_text(encoding="utf-8"))
    assert snapshot["resolved_phase2"]["ds"]["overage_factor"] == 0.05
    assert any("ds_overage_factor rows are preserved in normalized artifacts" in warning for warning in snapshot["warnings"])


def test_import_model_assumptions_workbook_writes_treatment_duration_artifact_for_patient_starts(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"
    output_dir = tmp_path / "assumptions"

    build_model_assumptions_template(workbook_path)
    _set_cell(workbook_path, "Scenario_Controls", "F2", "patient_starts")

    result = import_model_assumptions_workbook(workbook_path, output_dir=output_dir)

    duration_rows = _read_csv(result.file_paths["treatment_duration_assumptions"])
    assert result.context.demand_basis == "patient_starts"
    assert _find_row(
        duration_rows,
        module="AML",
        segment_code="1L_fit",
        geography_code="ALL",
    )["treatment_duration_months"] == "12"


def test_import_model_assumptions_workbook_duplicate_treatment_duration_scope_fails(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"

    build_model_assumptions_template(workbook_path)
    _set_cell(workbook_path, "Treatment_Duration_Assumptions", "B9", "AML")
    _set_cell(workbook_path, "Treatment_Duration_Assumptions", "C9", "1L_fit")
    _set_cell(workbook_path, "Treatment_Duration_Assumptions", "D9", "ALL")
    _set_cell(workbook_path, "Treatment_Duration_Assumptions", "E9", 12)
    _set_cell(workbook_path, "Treatment_Duration_Assumptions", "F9", "yes")

    with pytest.raises(ValueError, match="duplicates active scope"):
        import_model_assumptions_workbook(workbook_path)


def test_import_model_assumptions_workbook_missing_required_active_phase3_trade_row_fails(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"

    build_model_assumptions_template(workbook_path)
    _set_cell(workbook_path, "Trade_Inventory_FutureHooks", "BU2", "no")

    with pytest.raises(ValueError, match="Trade_Inventory_FutureHooks is missing a required active row"):
        import_model_assumptions_workbook(workbook_path)


def test_import_model_assumptions_workbook_missing_required_active_phase4_phase5_trade_value_fails(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"

    build_model_assumptions_template(workbook_path)
    _set_cell(workbook_path, "Trade_Inventory_FutureHooks", "V2", "")

    with pytest.raises(ValueError, match="bullwhip_amplification_threshold"):
        import_model_assumptions_workbook(workbook_path)
