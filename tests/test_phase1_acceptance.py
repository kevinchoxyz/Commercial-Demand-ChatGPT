from __future__ import annotations

from pathlib import Path

import pytest

from cbx250_model.demand.phase1_runner import run_phase1_scenario
from cbx250_model.inputs.excel_import import import_commercial_forecast_workbook
from cbx250_model.inputs.excel_template import build_commercial_forecast_template

from _phase1_acceptance_support import (
    configure_template_for_mode,
    read_csv_rows,
    sum_csv_numeric,
    write_curated_phase1_scenario,
    write_import_backed_phase1_scenario,
)


def _monthlyized_output_map(rows: list[dict[str, str]]) -> dict[tuple[str, str, str, str, int], float]:
    return {
        (
            row["scenario_name"],
            row["geography_code"],
            row["module"],
            row["segment_code"],
            int(row["month_index"]),
        ): float(row["patients_treated_monthly"])
        for row in rows
    }


def _runner_output_map(result: object) -> dict[tuple[str, str, str, str, int], float]:
    return {
        (
            row.scenario_name,
            row.geography_code,
            row.module,
            row.segment_code,
            row.month_index,
        ): row.patients_treated
        for row in result.outputs
    }


@pytest.mark.parametrize(
    ("forecast_grain", "forecast_frequency", "expected_source_total", "expected_key", "expected_value"),
    (
        (
            "module_level",
            "monthly",
            50.0,
            ("BASE_2029", "US", "AML", "1L_fit", 1),
            10.0,
        ),
        (
            "segment_level",
            "monthly",
            28.0,
            ("BASE_2029", "US", "AML", "1L_fit", 1),
            11.0,
        ),
        (
            "module_level",
            "annual",
            426.0,
            ("BASE_2029", "US", "AML", "1L_fit", 1),
            0.96,
        ),
        (
            "segment_level",
            "annual",
            162.0,
            ("BASE_2029", "US", "AML", "1L_fit", 1),
            0.96,
        ),
    ),
)
def test_phase1_acceptance_reconciles_authoritative_monthlyized_output_across_forecast_modes(
    tmp_path: Path,
    forecast_grain: str,
    forecast_frequency: str,
    expected_source_total: float,
    expected_key: tuple[str, str, str, str, int],
    expected_value: float,
) -> None:
    workbook_path = tmp_path / f"{forecast_grain}_{forecast_frequency}.xlsx"
    output_dir = tmp_path / f"curated_{forecast_grain}_{forecast_frequency}"

    build_commercial_forecast_template(workbook_path)
    configure_template_for_mode(
        workbook_path,
        forecast_grain=forecast_grain,
        forecast_frequency=forecast_frequency,
    )
    import_result = import_commercial_forecast_workbook(workbook_path, output_dir=output_dir)

    assert import_result.file_paths["monthlyized_output"].name == "monthlyized_output.csv"
    assert import_result.file_paths["monthlyized_output"].exists()
    assert import_result.context.demand_basis == "treated_census"

    active_contract_key = (
        "commercial_forecast_module_level"
        if forecast_grain == "module_level"
        else "commercial_forecast_segment_level"
    )
    active_contract_rows = read_csv_rows(import_result.file_paths[active_contract_key])
    monthlyized_rows = read_csv_rows(import_result.file_paths["monthlyized_output"])
    monthlyized_map = _monthlyized_output_map(monthlyized_rows)

    assert sum_csv_numeric(active_contract_rows, "patients_treated") == pytest.approx(expected_source_total)
    assert sum_csv_numeric(monthlyized_rows, "patients_treated_monthly") == pytest.approx(expected_source_total)
    assert all(
        float(row["patients_treated_monthly"]) == pytest.approx(float(row["patients_active"]))
        for row in monthlyized_rows
    )
    assert monthlyized_map[expected_key] == pytest.approx(expected_value)
    assert {"CML_Incident", "CML_Prevalent"} <= {row["module"] for row in monthlyized_rows}

    csv_keys = list(monthlyized_map)
    assert len(csv_keys) == len(set(csv_keys))

    scenario_path = write_import_backed_phase1_scenario(
        tmp_path / f"runner_{forecast_grain}_{forecast_frequency}",
        scenario_name=import_result.context.scenario_name,
        forecast_grain=forecast_grain,
        input_dir=output_dir,
        demand_basis="treated_census",
    )
    run_result = run_phase1_scenario(scenario_path)
    runner_map = _runner_output_map(run_result)

    assert len(run_result.calendar.months) == 240
    assert not run_result.validation.has_errors
    assert set(runner_map) == set(monthlyized_map)
    for key, patients_treated in monthlyized_map.items():
        assert runner_map[key] == pytest.approx(patients_treated)


def test_phase1_acceptance_mix_sum_errors_are_actionable_for_aml_and_mds(tmp_path: Path) -> None:
    scenario_path = write_curated_phase1_scenario(
        tmp_path,
        forecast_grain="module_level",
        module_level_rows=[
            "US,AML,1,100",
            "US,MDS,1,50",
        ],
        aml_mix_rows=[
            "US,1,1L_fit,0.5",
            "US,1,1L_unfit,0.2",
            "US,1,RR,0.2",
        ],
        mds_mix_rows=[
            "US,1,HR_MDS,0.7",
            "US,1,LR_MDS,0.4",
        ],
        cml_prevalent_rows=[],
    )

    result = run_phase1_scenario(scenario_path)
    assert result.validation.has_errors
    assert "segment_mix.sum_not_one" in {issue.code for issue in result.validation.issues}
    aml_issue = next(
        issue for issue in result.validation.issues if issue.code == "segment_mix.sum_not_one" and issue.context["module"] == "AML"
    )
    mds_issue = next(
        issue for issue in result.validation.issues if issue.code == "segment_mix.sum_not_one" and issue.context["module"] == "MDS"
    )
    assert aml_issue.context == {
        "scenario_name": "ACCEPTANCE_BASE",
        "geography_code": "US",
        "module": "AML",
        "month_index": "1",
    }
    assert mds_issue.context == {
        "scenario_name": "ACCEPTANCE_BASE",
        "geography_code": "US",
        "module": "MDS",
        "month_index": "1",
    }
    assert "sum to" in aml_issue.message
    assert "sum to" in mds_issue.message


def test_phase1_acceptance_missing_mix_rows_fail_with_actionable_context(tmp_path: Path) -> None:
    scenario_path = write_curated_phase1_scenario(
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
    issue = next(issue for issue in result.validation.issues if issue.code == "segment_mix.missing_required_rows")

    assert result.validation.has_errors
    assert issue.context == {
        "scenario_name": "ACCEPTANCE_BASE",
        "geography_code": "US",
        "module": "AML",
        "month_index": "1",
    }
    assert "RR" in issue.message


def test_phase1_acceptance_cml_prevalent_pool_guardrail_is_actionable_without_requiring_depletion_engine(
    tmp_path: Path,
) -> None:
    scenario_path = write_curated_phase1_scenario(
        tmp_path,
        forecast_grain="module_level",
        module_level_rows=["US,CML_Prevalent,1,30"],
        cml_prevalent_rows=["US,1,20"],
    )

    result = run_phase1_scenario(scenario_path)
    issue = next(issue for issue in result.validation.issues if issue.code == "cml_prevalent.pool_exceeded")

    assert result.validation.has_errors
    assert issue.context == {
        "scenario_name": "ACCEPTANCE_BASE",
        "geography_code": "US",
        "module": "CML_Prevalent",
        "month_index": "1",
    }
    assert "30.0 > 20.0" in issue.message


def test_phase1_acceptance_invalid_segment_codes_fail_fast_in_current_phase1_behavior(
    tmp_path: Path,
) -> None:
    scenario_path = write_curated_phase1_scenario(
        tmp_path,
        forecast_grain="segment_level",
        segment_level_rows=["US,AML,NOT_A_REAL_SEGMENT,1,10"],
    )

    with pytest.raises(ValueError, match="segment_code"):
        run_phase1_scenario(scenario_path)


def test_phase1_acceptance_real_scenario_loads_expected_geographies_and_full_horizon() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    scenario_path = repo_root / "config" / "scenarios" / "real_scenario_01.toml"

    result = run_phase1_scenario(scenario_path)
    geography_codes = {row["geography_code"] for row in result.dimensions["dim_geography"]}

    assert len(result.calendar.months) == 240
    assert geography_codes == {"EU", "US"}
    assert not result.validation.has_errors


def test_phase1_acceptance_patient_starts_mode_builds_authoritative_treated_census(tmp_path: Path) -> None:
    workbook_path = tmp_path / "patient_starts.xlsx"
    output_dir = tmp_path / "curated_patient_starts"
    treatment_duration_path = tmp_path / "treatment_duration_assumptions.csv"

    build_commercial_forecast_template(workbook_path)
    configure_template_for_mode(
        workbook_path,
        forecast_grain="segment_level",
        forecast_frequency="monthly",
        demand_basis="patient_starts",
    )
    treatment_duration_path.write_text(
        "\n".join(
            [
                "scenario_name,geography_code,module,segment_code,treatment_duration_months,active_flag,notes",
                "BASE_2029,ALL,AML,1L_fit,12,true,Approved base-case duration default.",
                "BASE_2029,ALL,AML,1L_unfit,10,true,Approved base-case duration default.",
                "BASE_2029,ALL,AML,RR,6,true,Approved base-case duration default.",
                "BASE_2029,ALL,MDS,HR_MDS,12,true,Approved base-case duration default.",
                "BASE_2029,ALL,MDS,LR_MDS,12,true,Approved base-case duration default.",
                "BASE_2029,ALL,CML_Incident,CML_Incident,24,true,Approved base-case duration default.",
                "BASE_2029,ALL,CML_Prevalent,CML_Prevalent,24,true,Approved base-case duration default.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    from _phase1_acceptance_support import clear_cells, set_cell

    clear_cells(
        workbook_path,
        "CML_Prevalent_Assumptions",
        (
            "B2", "C2", "E2", "F2", "G2", "H2", "I2", "J2", "K2",
            "B3", "C3", "E3", "F3", "G3", "H3", "I3", "J3", "K3",
            "B4", "C4", "E4", "F4", "G4", "H4", "I4", "J4", "K4",
        ),
    )
    clear_cells(
        workbook_path,
        "SegmentLevel_Forecast",
        (
            "B2", "C2", "D2", "E2", "G2", "H2",
            "B3", "C3", "D3", "E3", "G3", "H3",
            "B4", "C4", "D4", "E4", "G4", "H4",
            "B5", "C5", "D5", "E5", "G5", "H5",
        ),
    )
    set_cell(workbook_path, "SegmentLevel_Forecast", "B2", "US")
    set_cell(workbook_path, "SegmentLevel_Forecast", "C2", "AML")
    set_cell(workbook_path, "SegmentLevel_Forecast", "D2", "1L_fit")
    set_cell(workbook_path, "SegmentLevel_Forecast", "E2", 1)
    set_cell(workbook_path, "SegmentLevel_Forecast", "G2", 10)
    set_cell(workbook_path, "SegmentLevel_Forecast", "B3", "US")
    set_cell(workbook_path, "SegmentLevel_Forecast", "C3", "AML")
    set_cell(workbook_path, "SegmentLevel_Forecast", "D3", "1L_fit")
    set_cell(workbook_path, "SegmentLevel_Forecast", "E3", 2)
    set_cell(workbook_path, "SegmentLevel_Forecast", "G3", 10)

    import_result = import_commercial_forecast_workbook(
        workbook_path,
        output_dir=output_dir,
        treatment_duration_path=treatment_duration_path,
    )
    monthlyized_rows = read_csv_rows(import_result.file_paths["monthlyized_output"])
    monthlyized_map = _monthlyized_output_map(monthlyized_rows)

    assert import_result.context.demand_basis == "patient_starts"
    assert monthlyized_map[("BASE_2029", "US", "AML", "1L_fit", 1)] == pytest.approx(10.0)
    assert monthlyized_map[("BASE_2029", "US", "AML", "1L_fit", 2)] == pytest.approx(20.0)
    month_13 = next(
        row
        for row in monthlyized_rows
        if row["module"] == "AML" and row["segment_code"] == "1L_fit" and row["month_index"] == "13"
    )
    assert month_13["patients_treated_monthly"] == "10"
    assert month_13["patients_active"] == "10"
    assert month_13["patient_starts"] == "0"
    assert month_13["patients_continuing"] == "10"
    assert month_13["patients_rolloff"] == "10"
    assert month_13["patients_treated_monthly"] == month_13["patients_active"]
    assert month_13["continuing_patients"] == month_13["patients_continuing"]
    assert month_13["rolloff_patients"] == month_13["patients_rolloff"]
    assert month_13["treatment_duration_months_used"] == "12"

    scenario_path = write_import_backed_phase1_scenario(
        tmp_path / "runner_patient_starts",
        scenario_name=import_result.context.scenario_name,
        forecast_grain="segment_level",
        demand_basis="patient_starts",
        input_dir=output_dir,
    )
    run_result = run_phase1_scenario(scenario_path)
    runner_map = _runner_output_map(run_result)

    assert not run_result.validation.has_errors
    assert runner_map == pytest.approx(monthlyized_map)
