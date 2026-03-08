from __future__ import annotations

from pathlib import Path

import pytest

from cbx250_model.phase2.config_schema import load_phase2_config
from cbx250_model.phase2.runner import run_phase2_scenario
from cbx250_model.phase2.writer import write_phase2_outputs

from _phase2_support import read_csv_rows, write_phase2_scenario


def test_phase2_checked_in_base_config_uses_module_specific_approved_defaults() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config = load_phase2_config(repo_root / "config" / "scenarios" / "base_phase2.toml")

    assert config.model.dose_basis == "fixed"
    assert config.get_module_settings("AML").fixed_dose_mg == pytest.approx(0.15)
    assert config.get_module_settings("AML").weight_based_dose_mg_per_kg == pytest.approx(0.0023)
    assert config.get_module_settings("AML").average_patient_weight_kg == pytest.approx(80.0)
    assert config.get_module_settings("AML").doses_per_patient_per_month == pytest.approx(4.33)
    assert config.get_module_settings("MDS").doses_per_patient_per_month == pytest.approx(4.33)
    assert config.get_module_settings("CML_Incident").doses_per_patient_per_month == pytest.approx(1.0)
    assert config.get_module_settings("CML_Prevalent").doses_per_patient_per_month == pytest.approx(1.0)
    assert config.get_module_settings("AML").fg_mg_per_unit == pytest.approx(1.0)
    assert config.get_module_settings("AML").fg_vialing_rule == "ceil_mg_per_unit_no_sharing"
    assert config.plan_yield.ds_to_dp == pytest.approx(0.90)
    assert config.ds.qty_per_dp_unit_mg == pytest.approx(1.0)
    assert config.ds.overage_factor == pytest.approx(0.05)


def test_phase2_fixed_dose_base_case_uses_approved_aml_defaults(tmp_path: Path) -> None:
    scenario_path = write_phase2_scenario(
        tmp_path,
        scenario_name="PHASE2_BASE",
        monthlyized_rows=[
            "PHASE2_BASE,US,AML,1L_fit,1,2029-01-01,10,monthly,segment_level,test,,fixture",
        ],
        dose_basis="fixed",
    )

    result = run_phase2_scenario(scenario_path)
    row = result.outputs[0]

    assert not result.validation.has_errors
    assert row.dose_basis_used == "fixed"
    assert row.doses_required == pytest.approx(43.3)
    assert row.mg_per_dose_before_reduction == pytest.approx(0.15)
    assert row.mg_per_dose_after_reduction == pytest.approx(0.15)
    assert row.mg_required == pytest.approx(6.495)
    assert row.fg_mg_per_unit_used == pytest.approx(1.0)
    assert row.fg_units_before_pack_yield == pytest.approx(43.3)
    assert row.fg_units_required == pytest.approx(43.3)
    assert row.ss_units_required == pytest.approx(43.3)
    assert row.dp_units_required == pytest.approx(44.183673469387756)
    assert row.ds_required == pytest.approx(51.54761904761905)


def test_phase2_weight_based_base_case_uses_approved_defaults(tmp_path: Path) -> None:
    scenario_path = write_phase2_scenario(
        tmp_path,
        scenario_name="WEIGHT_BASED",
        monthlyized_rows=[
            "WEIGHT_BASED,US,MDS,HR_MDS,1,2029-01-01,4,monthly,segment_level,test,,fixture",
        ],
        dose_basis="weight_based",
    )

    result = run_phase2_scenario(scenario_path)
    row = result.outputs[0]

    assert not result.validation.has_errors
    assert row.dose_basis_used == "weight_based"
    assert row.mg_per_dose_before_reduction == pytest.approx(0.184)
    assert row.mg_per_dose_after_reduction == pytest.approx(0.184)
    assert row.doses_required == pytest.approx(17.32)
    assert row.mg_required == pytest.approx(3.18688)
    assert row.fg_units_required == pytest.approx(17.32)
    assert row.ds_required == pytest.approx(20.619047619047617)


def test_phase2_no_sharing_cml_prevalent_vialing_uses_patient_dose_level_rounding(tmp_path: Path) -> None:
    scenario_path = write_phase2_scenario(
        tmp_path,
        scenario_name="CML_PREV_NO_SHARING",
        monthlyized_rows=[
            "CML_PREV_NO_SHARING,US,CML_Prevalent,CML_Prevalent,1,2029-01-01,18,monthly,segment_level,test,,fixture",
        ],
        dose_basis="fixed",
    )

    result = run_phase2_scenario(scenario_path)
    row = result.outputs[0]

    assert not result.validation.has_errors
    assert row.doses_required == pytest.approx(18.0)
    assert row.mg_per_dose_after_reduction == pytest.approx(0.15)
    assert row.mg_required == pytest.approx(2.7)
    assert row.fg_units_before_pack_yield == pytest.approx(18.0)
    assert row.fg_units_required == pytest.approx(18.0)
    assert row.ss_units_required == pytest.approx(18.0)
    assert row.dp_units_required == pytest.approx(18.367346938775512)
    assert row.ds_required == pytest.approx(21.428571428571427)


def test_phase2_module_specific_doses_per_patient_per_month_is_respected(tmp_path: Path) -> None:
    scenario_path = write_phase2_scenario(
        tmp_path,
        scenario_name="FREQUENCY",
        monthlyized_rows=[
            "FREQUENCY,US,AML,RR,1,2029-01-01,1,monthly,segment_level,test,,fixture",
            "FREQUENCY,US,CML_Incident,CML_Incident,1,2029-01-01,1,monthly,segment_level,test,,fixture",
            "FREQUENCY,US,CML_Prevalent,CML_Prevalent,1,2029-01-01,1,monthly,segment_level,test,,fixture",
        ],
    )

    result = run_phase2_scenario(scenario_path)
    output_map = {(row.module, row.segment_code): row.doses_required for row in result.outputs}

    assert not result.validation.has_errors
    assert output_map[("AML", "RR")] == pytest.approx(4.33)
    assert output_map[("CML_Incident", "CML_Incident")] == pytest.approx(1.0)
    assert output_map[("CML_Prevalent", "CML_Prevalent")] == pytest.approx(1.0)


def test_phase2_dose_reduction_recalculates_patient_dose_vialing_after_mg_reduction(tmp_path: Path) -> None:
    base_scenario = write_phase2_scenario(
        tmp_path / "base",
        scenario_name="REDUCTION_TEST",
        monthlyized_rows=[
            "REDUCTION_TEST,US,AML,RR,1,2029-01-01,10,monthly,segment_level,test,,fixture",
        ],
        dose_basis="fixed",
        module_settings_overrides={
            "AML": {
                "fixed_dose_mg": 1.1,
            }
        },
    )
    reduced_scenario = write_phase2_scenario(
        tmp_path / "reduced",
        scenario_name="REDUCTION_TEST",
        monthlyized_rows=[
            "REDUCTION_TEST,US,AML,RR,1,2029-01-01,10,monthly,segment_level,test,,fixture",
        ],
        dose_basis="fixed",
        dose_reduction_enabled=True,
        dose_reduction_pct=0.25,
        module_settings_overrides={
            "AML": {
                "fixed_dose_mg": 1.1,
            }
        },
    )

    base_row = run_phase2_scenario(base_scenario).outputs[0]
    reduced_row = run_phase2_scenario(reduced_scenario).outputs[0]

    assert reduced_row.dose_reduction_applied is True
    assert reduced_row.mg_required == pytest.approx(base_row.mg_required * 0.75)
    assert base_row.fg_units_required == pytest.approx(86.6)
    assert reduced_row.fg_units_required == pytest.approx(43.3)
    assert reduced_row.ss_units_required == pytest.approx(43.3)
    assert reduced_row.dp_units_required == pytest.approx(44.183673469387756)
    assert base_row.ds_required == pytest.approx(103.0952380952381)
    assert reduced_row.ds_required == pytest.approx(51.54761904761905)


def test_phase2_cml_modules_remain_separate_through_outputs(tmp_path: Path) -> None:
    scenario_path = write_phase2_scenario(
        tmp_path,
        scenario_name="CML_FLOW",
        monthlyized_rows=[
            "CML_FLOW,US,CML_Incident,CML_Incident,1,2029-01-01,2,monthly,segment_level,test,,fixture",
            "CML_FLOW,US,CML_Prevalent,CML_Prevalent,1,2029-01-01,3,monthly,segment_level,test,,fixture",
        ],
    )

    result = run_phase2_scenario(scenario_path)
    output_keys = {(row.module, row.segment_code) for row in result.outputs}

    assert not result.validation.has_errors
    assert output_keys == {
        ("CML_Incident", "CML_Incident"),
        ("CML_Prevalent", "CML_Prevalent"),
    }


def test_phase2_ss_ratio_and_planning_yields_are_applied_after_patient_dose_vialing(tmp_path: Path) -> None:
    scenario_path = write_phase2_scenario(
        tmp_path,
        scenario_name="YIELDS",
        monthlyized_rows=[
            "YIELDS,EU,CML_Incident,CML_Incident,2,2029-02-01,10,monthly,segment_level,test,,fixture",
        ],
        ds_to_dp_yield=0.25,
        dp_to_fg_yield=0.5,
        ss_ratio_to_fg=1.5,
        ss_yield=0.75,
    )

    result = run_phase2_scenario(scenario_path)
    row = result.outputs[0]

    assert row.fg_units_before_pack_yield == pytest.approx(10.0)
    assert row.fg_units_required == pytest.approx(10.0)
    assert row.ss_units_required == pytest.approx(15.0)
    assert row.dp_units_required == pytest.approx(20.0)
    assert row.ds_required == pytest.approx(84.0)


def test_phase2_ds_conversion_uses_approved_yield_and_overage_not_legacy_divide_by_point95(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase2_scenario(
        tmp_path,
        scenario_name="DS_FORMULA",
        monthlyized_rows=[
            "DS_FORMULA,US,CML_Incident,CML_Incident,1,2029-01-01,10,monthly,segment_level,test,,fixture",
        ],
        ds_to_dp_yield=0.90,
        dp_to_fg_yield=0.5,
        ds_qty_per_dp_unit_mg=1.0,
        ds_overage_factor=0.05,
    )

    result = run_phase2_scenario(scenario_path)
    row = result.outputs[0]

    assert row.dp_units_required == pytest.approx(20.0)
    assert row.ds_required == pytest.approx(23.333333333333336)
    assert row.ds_required != pytest.approx(20.0 / 0.95)


def test_phase2_output_keys_are_unique_and_writer_emits_machine_readable_csv(tmp_path: Path) -> None:
    scenario_path = write_phase2_scenario(
        tmp_path,
        scenario_name="UNIQUE_KEYS",
        monthlyized_rows=[
            "UNIQUE_KEYS,US,AML,1L_fit,1,2029-01-01,1,monthly,segment_level,test,,fixture",
            "UNIQUE_KEYS,US,AML,RR,1,2029-01-01,2,monthly,segment_level,test,,fixture",
        ],
    )

    result = run_phase2_scenario(scenario_path)
    output_path = write_phase2_outputs(tmp_path / "phase2_output.csv", result.outputs)
    rows = read_csv_rows(output_path)

    assert not result.validation.has_errors
    assert len({row.key for row in result.outputs}) == len(result.outputs)
    assert len(rows) == 2
    assert rows[0]["planning_yields_used"].startswith("{")
    assert float(rows[0]["ds_required"]) == pytest.approx(result.outputs[0].ds_required)
    assert float(rows[0]["ds_required_mg"]) == pytest.approx(result.outputs[0].ds_required)
    assert float(rows[0]["ds_required_g"]) == pytest.approx(result.outputs[0].ds_required / 1000.0)
    assert float(rows[0]["ds_required_kg"]) == pytest.approx(result.outputs[0].ds_required / 1_000_000.0)


def test_phase2_duplicate_input_keys_fail_validation(tmp_path: Path) -> None:
    scenario_path = write_phase2_scenario(
        tmp_path,
        scenario_name="DUPLICATES",
        monthlyized_rows=[
            "DUPLICATES,US,AML,1L_fit,1,2029-01-01,1,monthly,segment_level,test,,fixture",
            "DUPLICATES,US,AML,1L_fit,1,2029-01-01,2,monthly,segment_level,test,,fixture",
        ],
    )

    result = run_phase2_scenario(scenario_path)

    assert result.validation.has_errors
    assert "phase2_input.duplicate_key" in {issue.code for issue in result.validation.issues}
