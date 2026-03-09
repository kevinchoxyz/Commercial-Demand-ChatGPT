from __future__ import annotations

from pathlib import Path

from cbx250_model.demand.phase1_runner import run_phase1_scenario
from cbx250_model.outputs.summary import build_run_summary
import pytest


def _write_lines(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_phase1_scenario(
    tmp_path: Path,
    *,
    forecast_grain: str,
    demand_basis: str = "treated_census",
    module_level_rows: list[str] | None = None,
    segment_level_rows: list[str] | None = None,
    aml_mix_rows: list[str] | None = None,
    mds_mix_rows: list[str] | None = None,
    cml_prevalent_rows: list[str] | None = None,
    treatment_duration_rows: list[str] | None = None,
) -> Path:
    config_dir = tmp_path / "config"
    parameters_dir = config_dir / "parameters"
    scenarios_dir = config_dir / "scenarios"
    data_dir = tmp_path / "data"
    parameters_dir.mkdir(parents=True)
    scenarios_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)

    _write_lines(
        parameters_dir / "phase1.toml",
        [
            "[model]",
            "phase = 1",
            'build_scope = "deterministic_demand_foundation"',
            'primary_demand_input = "Commercial Patients Treated"',
            f'forecast_grain = "{forecast_grain}"',
            f'demand_basis = "{demand_basis}"',
            "",
            "[horizon]",
            'us_aml_mds_initial_approval_date = "2029-01-01"',
            "forecast_horizon_months = 240",
            'time_grain = "monthly"',
            "",
            "[modules]",
            'enabled = ["AML", "MDS", "CML_Incident", "CML_Prevalent"]',
            'disabled = ["trade", "production", "inventory", "financials", "monte_carlo"]',
            "",
            "[validation]",
            "enforce_segment_share_rules = true",
            "enforce_cml_prevalent_pool_constraints = true",
            "enforce_epi_crosscheck_warning = false",
        ],
    )

    _write_lines(
        scenarios_dir / "scenario.toml",
        [
            'scenario_name = "BASE"',
            'parameter_config = "../parameters/phase1.toml"',
            "",
            "[inputs]",
            'commercial_forecast_module_level = "../../data/commercial_forecast_module_level.csv"',
            'commercial_forecast_segment_level = "../../data/commercial_forecast_segment_level.csv"',
            'epi_crosscheck = "../../data/inp_epi_crosscheck.csv"',
            'aml_segment_mix = "../../data/aml_segment_mix.csv"',
            'mds_segment_mix = "../../data/mds_segment_mix.csv"',
            'cml_prevalent = "../../data/inp_cml_prevalent.csv"',
            'treatment_duration_assumptions = "../../data/treatment_duration_assumptions.csv"',
        ],
    )

    _write_lines(
        data_dir / "commercial_forecast_module_level.csv",
        [
            "geography_code,module,month_index,patients_treated",
            *(module_level_rows or []),
        ],
    )
    _write_lines(
        data_dir / "commercial_forecast_segment_level.csv",
        [
            "geography_code,module,segment_code,month_index,patients_treated",
            *(segment_level_rows or []),
        ],
    )
    _write_lines(
        data_dir / "inp_epi_crosscheck.csv",
        ["geography_code,module,month_index,treatable_patients"],
    )
    _write_lines(
        data_dir / "aml_segment_mix.csv",
        [
            "geography_code,month_index,segment_code,segment_share",
            *(aml_mix_rows or []),
        ],
    )
    _write_lines(
        data_dir / "mds_segment_mix.csv",
        [
            "geography_code,month_index,segment_code,segment_share",
            *(mds_mix_rows or []),
        ],
    )
    _write_lines(
        data_dir / "inp_cml_prevalent.csv",
        [
            "geography_code,month_index,addressable_prevalent_pool",
            *(cml_prevalent_rows or []),
        ],
    )
    _write_lines(
        data_dir / "treatment_duration_assumptions.csv",
        [
            "scenario_name,geography_code,module,segment_code,treatment_duration_months,active_flag,notes",
            *(treatment_duration_rows or []),
        ],
    )

    return scenarios_dir / "scenario.toml"


def test_base_phase1_template_config_runs_without_input_rows() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    result = run_phase1_scenario(repo_root / "config" / "scenarios" / "base_phase1.toml")

    assert len(result.calendar.months) == 240
    assert len(result.outputs) == 0
    assert not result.validation.has_errors


def test_module_level_allocates_aml_mds_and_keeps_cml_separate(tmp_path: Path) -> None:
    scenario_path = _write_phase1_scenario(
        tmp_path,
        forecast_grain="module_level",
        module_level_rows=[
            "US,AML,1,100",
            "US,MDS,1,50",
            "US,CML_Incident,1,20",
            "US,CML_Prevalent,1,30",
        ],
        aml_mix_rows=[
            "US,1,1L_fit,0.5",
            "US,1,1L_unfit,0.3",
            "US,1,RR,0.2",
        ],
        mds_mix_rows=[
            "US,1,HR_MDS,0.6",
            "US,1,LR_MDS,0.4",
        ],
        cml_prevalent_rows=["US,1,40"],
    )

    result = run_phase1_scenario(scenario_path)
    summary = build_run_summary(result)
    output_map = {(row.module, row.segment_code): row.patients_treated for row in result.outputs}

    assert not result.validation.has_errors
    assert len(result.outputs) == 7
    assert output_map[("AML", "1L_fit")] == 50.0
    assert output_map[("AML", "1L_unfit")] == 30.0
    assert output_map[("AML", "RR")] == 20.0
    assert output_map[("MDS", "HR_MDS")] == 30.0
    assert output_map[("MDS", "LR_MDS")] == 20.0
    assert output_map[("CML_Incident", "CML_Incident")] == 20.0
    assert output_map[("CML_Prevalent", "CML_Prevalent")] == 30.0
    assert summary["forecast_grain"] == "module_level"
    assert summary["demand_basis"] == "treated_census"
    assert summary["geography_count"] == 1
    assert summary["output_rows_by_module"] == {
        "AML": 3,
        "MDS": 2,
        "CML_Incident": 1,
        "CML_Prevalent": 1,
    }


def test_segment_level_passes_aml_mds_through_directly(tmp_path: Path) -> None:
    scenario_path = _write_phase1_scenario(
        tmp_path,
        forecast_grain="segment_level",
        segment_level_rows=[
            "US,AML,1L_fit,1,40",
            "US,AML,RR,1,10",
            "US,MDS,HR_MDS,1,25",
            "US,MDS,LR_MDS,1,15",
            "US,CML_Incident,CML_Incident,1,8",
            "US,CML_Prevalent,CML_Prevalent,1,9",
        ],
        aml_mix_rows=[
            "US,1,1L_fit,0.5",
            "US,1,1L_unfit,0.3",
            "US,1,RR,0.2",
        ],
        mds_mix_rows=[
            "US,1,HR_MDS,0.6",
            "US,1,LR_MDS,0.4",
        ],
        cml_prevalent_rows=["US,1,10"],
    )

    result = run_phase1_scenario(scenario_path)
    output_map = {(row.module, row.segment_code): row.patients_treated for row in result.outputs}

    assert not result.validation.has_errors
    assert output_map[("AML", "1L_fit")] == 40.0
    assert output_map[("AML", "RR")] == 10.0
    assert output_map[("MDS", "HR_MDS")] == 25.0
    assert output_map[("MDS", "LR_MDS")] == 15.0
    assert output_map[("CML_Incident", "CML_Incident")] == 8.0
    assert output_map[("CML_Prevalent", "CML_Prevalent")] == 9.0


def test_missing_mix_rows_fail_in_module_level_mode(tmp_path: Path) -> None:
    scenario_path = _write_phase1_scenario(
        tmp_path,
        forecast_grain="module_level",
        module_level_rows=["US,AML,1,100"],
        aml_mix_rows=[
            "US,1,1L_fit,0.5",
            "US,1,1L_unfit,0.5",
        ],
        cml_prevalent_rows=[],
    )

    result = run_phase1_scenario(scenario_path)
    issue_codes = {issue.code for issue in result.validation.issues}

    assert result.validation.has_errors
    assert "segment_mix.missing_required_rows" in issue_codes


def test_bad_segment_codes_fail_in_segment_level_mode(tmp_path: Path) -> None:
    scenario_path = _write_phase1_scenario(
        tmp_path,
        forecast_grain="segment_level",
        segment_level_rows=["US,AML,NOT_A_REAL_SEGMENT,1,10"],
        cml_prevalent_rows=[],
    )

    with pytest.raises(ValueError, match="segment_code"):
        run_phase1_scenario(scenario_path)


def test_patient_starts_mode_builds_continuing_census_and_rolloff(tmp_path: Path) -> None:
    scenario_path = _write_phase1_scenario(
        tmp_path,
        forecast_grain="segment_level",
        demand_basis="patient_starts",
        segment_level_rows=[
            "US,AML,1L_fit,1,10",
            "US,AML,1L_fit,2,10",
        ],
        treatment_duration_rows=[
            "BASE,ALL,AML,1L_fit,12,true,Approved base-case duration default.",
        ],
    )

    result = run_phase1_scenario(scenario_path)
    output_map = {
        (row.module, row.segment_code, row.month_index): row
        for row in result.outputs
    }

    assert not result.validation.has_errors
    assert output_map[("AML", "1L_fit", 1)].patients_treated == 10.0
    assert output_map[("AML", "1L_fit", 1)].patients_active == 10.0
    assert output_map[("AML", "1L_fit", 1)].patient_starts == 10.0
    assert output_map[("AML", "1L_fit", 2)].patients_treated == 20.0
    assert output_map[("AML", "1L_fit", 2)].patients_active == 20.0
    assert output_map[("AML", "1L_fit", 2)].patient_starts == 10.0
    assert output_map[("AML", "1L_fit", 2)].patients_continuing == 10.0
    assert output_map[("AML", "1L_fit", 2)].patients_treated == output_map[("AML", "1L_fit", 2)].patients_active
    assert output_map[("AML", "1L_fit", 13)].patients_treated == 10.0
    assert output_map[("AML", "1L_fit", 13)].patients_active == 10.0
    assert output_map[("AML", "1L_fit", 13)].patient_starts == 0.0
    assert output_map[("AML", "1L_fit", 13)].patients_continuing == 10.0
    assert output_map[("AML", "1L_fit", 13)].patients_rolloff == 10.0
    assert ("AML", "1L_fit", 14) not in output_map


def test_patient_starts_mode_respects_segment_specific_durations(tmp_path: Path) -> None:
    scenario_path = _write_phase1_scenario(
        tmp_path,
        forecast_grain="segment_level",
        demand_basis="patient_starts",
        segment_level_rows=[
            "US,AML,1L_fit,1,10",
            "US,AML,RR,1,10",
        ],
        treatment_duration_rows=[
            "BASE,ALL,AML,1L_fit,12,true,Approved base-case duration default.",
            "BASE,ALL,AML,RR,6,true,Approved base-case duration default.",
        ],
    )

    result = run_phase1_scenario(scenario_path)
    output_map = {
        (row.module, row.segment_code, row.month_index): row.patients_treated
        for row in result.outputs
    }

    assert not result.validation.has_errors
    assert output_map[("AML", "1L_fit", 7)] == 10.0
    assert ("AML", "RR", 7) not in output_map


def test_patient_starts_mode_supports_cml_incident_and_prevalent_24_month_duration(tmp_path: Path) -> None:
    cml_pool_rows = [f"US,{month_index},100" for month_index in range(1, 25)]
    scenario_path = _write_phase1_scenario(
        tmp_path,
        forecast_grain="segment_level",
        demand_basis="patient_starts",
        segment_level_rows=[
            "US,CML_Incident,CML_Incident,1,5",
            "US,CML_Prevalent,CML_Prevalent,1,6",
        ],
        cml_prevalent_rows=cml_pool_rows,
        treatment_duration_rows=[
            "BASE,ALL,CML_Incident,CML_Incident,24,true,Approved base-case duration default.",
            "BASE,ALL,CML_Prevalent,CML_Prevalent,24,true,Approved base-case duration default.",
        ],
    )

    result = run_phase1_scenario(scenario_path)
    output_map = {
        (row.module, row.segment_code, row.month_index): row.patients_treated
        for row in result.outputs
    }

    assert not result.validation.has_errors
    assert output_map[("CML_Incident", "CML_Incident", 24)] == 5.0
    assert output_map[("CML_Prevalent", "CML_Prevalent", 24)] == 6.0
    assert ("CML_Incident", "CML_Incident", 25) not in output_map
    assert ("CML_Prevalent", "CML_Prevalent", 25) not in output_map
