from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from cbx250_model.inputs.assumptions_template import build_model_assumptions_template
from cbx250_model.inputs.excel_import import WorkbookImportResult, WorkbookSubmissionContext
from cbx250_model.inputs.excel_template import build_commercial_forecast_template
from cbx250_model.phase2.config_schema import load_phase2_config
from cbx250_model.workflow import run_forecast_workflow

from _phase1_acceptance_support import clear_cells, configure_template_for_mode, set_cell


def test_forecast_workflow_happy_path_runs_import_and_phase2(tmp_path: Path) -> None:
    workbook_path = tmp_path / "CBX250 Commercial Forecast Real.xlsx"
    build_commercial_forecast_template(workbook_path)
    configure_template_for_mode(
        workbook_path,
        forecast_grain="module_level",
        forecast_frequency="monthly",
    )

    result = run_forecast_workflow(
        workbook_path=workbook_path,
        scenario_name="REAL_2029",
        output_dir=tmp_path / "workflow_output",
    )

    assert result.scenario_name == "REAL_2029"
    assert result.phase1_monthlyized_output_path.exists()
    assert result.phase1_monthlyized_output_path.name == "monthlyized_output.csv"
    assert result.phase2_output_path.exists()
    assert result.phase2_output_path.name == "phase2_deterministic_cascade.csv"
    assert result.summary["scenario_name"] == "REAL_2029"
    assert result.summary["forecast_grain"] == "module_level"
    assert result.summary["forecast_frequency"] == "monthly"
    assert result.summary["demand_basis"] == "treated_census"
    assert result.summary["output_row_count"] > 0
    assert result.summary["validation_issue_count"] == 0
    assert result.summary["assumptions_workbook_used"] is False
    assert result.summary["authoritative_output_files"] == {
        "phase1_monthlyized_output": str(result.phase1_monthlyized_output_path),
        "phase2_deterministic_cascade": str(result.phase2_output_path),
    }


def test_forecast_workflow_fails_for_missing_workbook(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Workbook not found"):
        run_forecast_workflow(workbook_path=tmp_path / "missing_workbook.xlsx")


def test_forecast_workflow_fails_for_missing_assumptions_workbook(tmp_path: Path) -> None:
    workbook_path = tmp_path / "workflow_source.xlsx"
    build_commercial_forecast_template(workbook_path)

    with pytest.raises(FileNotFoundError, match="Assumptions workbook not found"):
        run_forecast_workflow(
            workbook_path=workbook_path,
            assumptions_workbook=tmp_path / "missing_assumptions.xlsx",
        )


def test_forecast_workflow_fails_if_import_does_not_create_monthlyized_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workbook_path = tmp_path / "placeholder.xlsx"
    workbook_path.write_bytes(b"placeholder")

    fake_output_dir = tmp_path / "workflow_output"
    fake_result = WorkbookImportResult(
        workbook_path=workbook_path.resolve(),
        output_dir=fake_output_dir.resolve(),
        context=WorkbookSubmissionContext(
            scenario_name="BROKEN_IMPORT",
            forecast_grain="module_level",
            forecast_frequency="monthly",
            demand_basis="treated_census",
            us_aml_mds_initial_approval_date=date(2029, 1, 1),
            real_geography_list_confirmed=True,
        ),
        file_paths={
            "monthlyized_output": fake_output_dir / "monthlyized_output.csv",
        },
        row_counts={
            "geography_master": 0,
            "monthlyized_output": 0,
        },
        warnings=(),
    )

    def fake_import(
        *,
        workbook_path: Path,
        output_dir: Path,
        scenario_name_override: str | None,
        treatment_duration_path: Path | None,
    ) -> WorkbookImportResult:
        return fake_result

    monkeypatch.setattr("cbx250_model.workflow.import_commercial_forecast_workbook", fake_import)

    with pytest.raises(
        FileNotFoundError,
        match="Workbook import did not produce the authoritative Phase 1 output monthlyized_output.csv",
    ):
        run_forecast_workflow(
            workbook_path=workbook_path,
            scenario_name="BROKEN_IMPORT",
            output_dir=fake_output_dir,
        )


def test_forecast_workflow_generated_phase2_scenario_uses_authoritative_monthlyized_output(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "workflow_source.xlsx"
    build_commercial_forecast_template(workbook_path)
    configure_template_for_mode(
        workbook_path,
        forecast_grain="segment_level",
        forecast_frequency="monthly",
    )

    result = run_forecast_workflow(
        workbook_path=workbook_path,
        scenario_name="SEGMENT_REAL_2029",
        output_dir=tmp_path / "workflow_output",
    )
    phase2_config = load_phase2_config(result.generated_phase2_scenario_path)

    assert phase2_config.input_paths.phase1_monthlyized_output == result.phase1_monthlyized_output_path
    assert result.generated_phase2_scenario_path.exists()


def test_forecast_workflow_happy_path_uses_assumptions_workbook_as_phase2_source(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "workflow_source.xlsx"
    assumptions_workbook_path = tmp_path / "assumptions.xlsx"
    build_commercial_forecast_template(workbook_path)
    build_model_assumptions_template(assumptions_workbook_path)
    configure_template_for_mode(
        workbook_path,
        forecast_grain="module_level",
        forecast_frequency="monthly",
    )
    set_cell(assumptions_workbook_path, "Scenario_Controls", "F2", "treated_census")

    result = run_forecast_workflow(
        workbook_path=workbook_path,
        assumptions_workbook=assumptions_workbook_path,
        scenario_name="ASSUMPTIONS_REAL_2029",
        output_dir=tmp_path / "workflow_output",
    )

    assert result.assumptions_result is not None
    assert result.assumptions_output_dir == (tmp_path / "workflow_output" / "assumptions").resolve()
    assert result.summary["assumptions_workbook_used"] is True
    assert result.summary["phase2_parameter_source"] == "assumptions_workbook"
    assert result.summary["assumptions_artifacts"]["generated_phase2_parameters"].endswith(
        "generated_phase2_parameters.toml"
    )
    assert result.summary["assumptions_artifacts"]["treatment_duration_assumptions"].endswith(
        "treatment_duration_assumptions.csv"
    )
    assert result.phase2_output_path.exists()


def test_forecast_workflow_generated_phase2_scenario_uses_assumptions_generated_parameter_config(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "workflow_source.xlsx"
    assumptions_workbook_path = tmp_path / "assumptions.xlsx"
    build_commercial_forecast_template(workbook_path)
    build_model_assumptions_template(assumptions_workbook_path)
    configure_template_for_mode(
        workbook_path,
        forecast_grain="segment_level",
        forecast_frequency="monthly",
    )
    set_cell(assumptions_workbook_path, "Scenario_Controls", "F2", "treated_census")

    result = run_forecast_workflow(
        workbook_path=workbook_path,
        assumptions_workbook=assumptions_workbook_path,
        scenario_name="ASSUMPTIONS_SEGMENT_2029",
        output_dir=tmp_path / "workflow_output",
    )
    phase2_config = load_phase2_config(result.generated_phase2_scenario_path)

    assert result.assumptions_result is not None
    assert phase2_config.input_paths.phase1_monthlyized_output == result.phase1_monthlyized_output_path
    assert phase2_config.parameter_config_path == result.assumptions_result.file_paths["generated_phase2_parameters"]
    assert result.summary["phase2_parameter_config_used"] == str(
        result.assumptions_result.file_paths["generated_phase2_parameters"]
    )


def test_forecast_workflow_assumptions_workbook_wins_over_phase2_scenario_with_warning(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "workflow_source.xlsx"
    assumptions_workbook_path = tmp_path / "assumptions.xlsx"
    build_commercial_forecast_template(workbook_path)
    build_model_assumptions_template(assumptions_workbook_path)
    configure_template_for_mode(
        workbook_path,
        forecast_grain="module_level",
        forecast_frequency="monthly",
    )
    set_cell(assumptions_workbook_path, "Scenario_Controls", "F2", "treated_census")

    result = run_forecast_workflow(
        workbook_path=workbook_path,
        assumptions_workbook=assumptions_workbook_path,
        phase2_scenario=Path("config/scenarios/base_phase2.toml"),
        scenario_name="ASSUMPTIONS_WIN_2029",
        output_dir=tmp_path / "workflow_output",
    )

    assert result.assumptions_result is not None
    assert result.phase2_template_path == result.assumptions_result.file_paths["generated_phase2_scenario"]
    assert any("phase2_scenario" in warning and "ignored" in warning for warning in result.summary["workflow_warnings"])


def test_forecast_workflow_summary_reflects_no_sharing_fg_and_ss_totals(tmp_path: Path) -> None:
    workbook_path = tmp_path / "cml_prevalent_workflow.xlsx"
    build_commercial_forecast_template(workbook_path)
    configure_template_for_mode(
        workbook_path,
        forecast_grain="segment_level",
        forecast_frequency="monthly",
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
    set_cell(workbook_path, "SegmentLevel_Forecast", "C2", "CML_Prevalent")
    set_cell(workbook_path, "SegmentLevel_Forecast", "D2", "ALL")
    set_cell(workbook_path, "SegmentLevel_Forecast", "E2", 1)
    set_cell(workbook_path, "SegmentLevel_Forecast", "G2", 18)

    result = run_forecast_workflow(
        workbook_path=workbook_path,
        scenario_name="CML_PREVALENT_NO_SHARING",
        output_dir=tmp_path / "workflow_output",
    )

    assert result.summary["total_patients_treated"] == pytest.approx(18.0)
    assert result.summary["total_fg_units_required"] == pytest.approx(18.0)
    assert result.summary["total_ss_units_required"] == pytest.approx(18.0)


def test_forecast_workflow_patient_starts_uses_assumptions_duration_artifact(tmp_path: Path) -> None:
    workbook_path = tmp_path / "patient_starts_forecast.xlsx"
    assumptions_workbook_path = tmp_path / "patient_starts_assumptions.xlsx"
    build_commercial_forecast_template(workbook_path)
    build_model_assumptions_template(assumptions_workbook_path)
    configure_template_for_mode(
        workbook_path,
        forecast_grain="segment_level",
        forecast_frequency="monthly",
        demand_basis="patient_starts",
    )
    set_cell(assumptions_workbook_path, "Scenario_Controls", "F2", "patient_starts")
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

    result = run_forecast_workflow(
        workbook_path=workbook_path,
        assumptions_workbook=assumptions_workbook_path,
        scenario_name="PATIENT_STARTS_2029",
        output_dir=tmp_path / "workflow_output",
    )

    assert result.summary["demand_basis"] == "patient_starts"
    assert result.summary["assumptions_workbook_used"] is True
    assert result.import_result.file_paths["treatment_duration_assumptions"].exists()


def test_forecast_workflow_fails_if_assumptions_and_forecast_demand_basis_conflict(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "treated_forecast.xlsx"
    assumptions_workbook_path = tmp_path / "starts_assumptions.xlsx"
    build_commercial_forecast_template(workbook_path)
    build_model_assumptions_template(assumptions_workbook_path)
    set_cell(workbook_path, "Inputs", "B5", "treated_census")
    set_cell(assumptions_workbook_path, "Scenario_Controls", "F2", "patient_starts")

    with pytest.raises(ValueError, match="does not match forecast workbook demand_basis"):
        run_forecast_workflow(
            workbook_path=workbook_path,
            assumptions_workbook=assumptions_workbook_path,
            scenario_name="CONFLICT_2029",
            output_dir=tmp_path / "workflow_output",
        )
