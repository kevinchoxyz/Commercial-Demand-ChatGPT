from __future__ import annotations

from pathlib import Path
import csv
import json

import pytest

from cbx250_model.inputs.assumptions_import import import_model_assumptions_workbook
from cbx250_model.inputs.assumptions_template import build_model_assumptions_template
from cbx250_model.phase2.config_schema import load_phase2_config

from ._workbook_test_support import set_cell


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
    assert result.context.dose_basis_default == "fixed"
    assert result.row_counts["dosing_assumptions"] == 4
    assert result.file_paths["generated_phase2_parameters"].exists()
    assert result.file_paths["generated_phase2_scenario"].exists()

    config = load_phase2_config(result.file_paths["generated_phase2_scenario"])
    assert config.model.dose_basis == "fixed"
    assert config.get_module_settings("AML").fixed_dose_mg == 0.15
    assert config.get_module_settings("AML").doses_per_patient_per_month == 4.33
    assert config.get_module_settings("CML_Prevalent").doses_per_patient_per_month == 1.0
    assert config.ds.qty_per_dp_unit_mg == 1.0
    assert config.ds.overage_factor == 0.05
    assert config.ss.ratio_to_fg == 1.0

    summary = json.loads(result.file_paths["import_summary"].read_text(encoding="utf-8"))
    assert "Product_Parameters scenario_default + module_override -> fg_mg_per_unit resolution" in summary["wired_into_current_engine"]
    assert "Trade_Inventory_FutureHooks normalized only; future-phase placeholder." in summary["future_ready_only"]


def test_import_model_assumptions_workbook_missing_required_field_fails_with_context(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"

    build_model_assumptions_template(workbook_path)
    set_cell(workbook_path, "Scenario_Controls", "F2", "")

    with pytest.raises(ValueError, match="Scenario_Controls row 2 is missing required value for 'dose_basis_default'"):
        import_model_assumptions_workbook(workbook_path)


def test_import_model_assumptions_workbook_invalid_lookup_value_fails(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"

    build_model_assumptions_template(workbook_path)
    set_cell(workbook_path, "Packaging_and_Vialing", "D2", "bad_rule")

    with pytest.raises(ValueError, match="Packaging_and_Vialing row 2 has unsupported fg_vialing_rule"):
        import_model_assumptions_workbook(workbook_path)


def test_import_model_assumptions_workbook_duplicate_scoped_row_fails(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"

    build_model_assumptions_template(workbook_path)
    set_cell(workbook_path, "Product_Parameters", "B3", "scenario_default")
    set_cell(workbook_path, "Product_Parameters", "C3", "ALL")
    set_cell(workbook_path, "Product_Parameters", "D3", "ALL")
    set_cell(workbook_path, "Product_Parameters", "I3", "yes")

    with pytest.raises(ValueError, match="Product_Parameters row 3 duplicates active scope"):
        import_model_assumptions_workbook(workbook_path)


def test_import_model_assumptions_workbook_preserves_ds_qty_module_override_but_uses_scenario_default(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Model_Assumptions_Template.xlsx"
    output_dir = tmp_path / "assumptions"

    build_model_assumptions_template(workbook_path)
    set_cell(workbook_path, "Product_Parameters", "E3", 1.2)
    set_cell(workbook_path, "Product_Parameters", "I3", "yes")

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
    set_cell(workbook_path, "Yield_Assumptions", "I3", 0.12)
    set_cell(workbook_path, "Yield_Assumptions", "J3", "yes")

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
