from __future__ import annotations

from pathlib import Path

import pytest

from cbx250_model.inputs.excel_import import import_commercial_forecast_workbook
from cbx250_model.inputs.excel_template import build_commercial_forecast_template
from cbx250_model.phase2.config_schema import load_phase2_config
from cbx250_model.phase2.runner import run_phase2_scenario
from cbx250_model.phase2.writer import write_phase2_outputs

from _phase1_acceptance_support import configure_template_for_mode
from _phase2_support import read_csv_rows, write_phase2_scenario


def test_phase2_acceptance_runs_from_authoritative_phase1_monthlyized_output(tmp_path: Path) -> None:
    workbook_path = tmp_path / "phase2_acceptance.xlsx"
    curated_dir = tmp_path / "curated_phase1"

    build_commercial_forecast_template(workbook_path)
    configure_template_for_mode(
        workbook_path,
        forecast_grain="module_level",
        forecast_frequency="monthly",
    )
    import_result = import_commercial_forecast_workbook(workbook_path, output_dir=curated_dir)

    phase2_scenario = write_phase2_scenario(
        tmp_path / "phase2",
        scenario_name=import_result.context.scenario_name,
        phase1_monthlyized_output_path=import_result.file_paths["monthlyized_output"],
        dose_basis="fixed",
        ds_to_dp_yield=0.90,
        dp_to_fg_yield=0.98,
        fg_pack_yield=1.0,
        ss_yield=1.0,
        ss_ratio_to_fg=1.0,
    )

    result = run_phase2_scenario(phase2_scenario)
    output_path = write_phase2_outputs(tmp_path / "phase2_deterministic_cascade.csv", result.outputs)
    phase1_rows = read_csv_rows(import_result.file_paths["monthlyized_output"])
    phase2_rows = read_csv_rows(output_path)

    assert import_result.file_paths["monthlyized_output"].name == "monthlyized_output.csv"
    assert not result.validation.has_errors
    assert len(phase2_rows) == len(phase1_rows)
    assert sum(float(row["patients_treated"]) for row in phase2_rows) == sum(
        float(row["patients_treated_monthly"]) for row in phase1_rows
    )
    assert {"CML_Incident", "CML_Prevalent"} <= {row["module"] for row in phase2_rows}
    assert {row["dose_basis_used"] for row in phase2_rows} == {"fixed"}
    assert {row["fg_vialing_rule_used"] for row in phase2_rows} == {"ceil_mg_per_unit_no_sharing"}
    assert all(float(row["fg_mg_per_unit_used"]) == pytest.approx(1.0) for row in phase2_rows)
    assert all(float(row["ds_required_mg"]) == pytest.approx(float(row["ds_required"])) for row in phase2_rows)
    assert all(float(row["ds_required_g"]) == pytest.approx(float(row["ds_required_mg"]) / 1000.0) for row in phase2_rows)
    assert all(float(row["ds_required_kg"]) == pytest.approx(float(row["ds_required_mg"]) / 1_000_000.0) for row in phase2_rows)
    assert output_path.name == "phase2_deterministic_cascade.csv"


def test_phase2_acceptance_checked_in_base_config_uses_approved_module_defaults() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config = load_phase2_config(repo_root / "config" / "scenarios" / "base_phase2.toml")

    assert config.model.dose_basis == "fixed"
    assert config.get_module_settings("AML").fixed_dose_mg == pytest.approx(0.15)
    assert config.get_module_settings("MDS").fixed_dose_mg == pytest.approx(0.15)
    assert config.get_module_settings("CML_Incident").weight_based_dose_mg_per_kg == pytest.approx(
        0.0023
    )
    assert config.get_module_settings("CML_Prevalent").average_patient_weight_kg == pytest.approx(
        80.0
    )
    assert config.get_module_settings("AML").doses_per_patient_per_month == pytest.approx(4.33)
    assert config.get_module_settings("MDS").doses_per_patient_per_month == pytest.approx(4.33)
    assert config.get_module_settings("CML_Incident").doses_per_patient_per_month == pytest.approx(
        1.0
    )
    assert config.get_module_settings("CML_Prevalent").doses_per_patient_per_month == pytest.approx(
        1.0
    )
    assert config.get_module_settings("AML").fg_mg_per_unit == pytest.approx(1.0)
    assert config.get_module_settings("AML").fg_vialing_rule == "ceil_mg_per_unit_no_sharing"
    assert config.plan_yield.ds_to_dp == pytest.approx(0.90)
    assert config.ds.qty_per_dp_unit_mg == pytest.approx(1.0)
    assert config.ds.overage_factor == pytest.approx(0.05)
