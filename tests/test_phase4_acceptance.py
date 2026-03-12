from __future__ import annotations

from pathlib import Path

import pytest

from cbx250_model.inputs.excel_import import import_commercial_forecast_workbook
from cbx250_model.inputs.excel_template import build_commercial_forecast_template
from cbx250_model.phase2.runner import run_phase2_scenario
from cbx250_model.phase2.writer import write_phase2_outputs
from cbx250_model.phase3.runner import run_phase3_scenario
from cbx250_model.phase3.writer import write_phase3_outputs
from cbx250_model.phase4.runner import run_phase4_scenario
from cbx250_model.phase4.writer import write_phase4_detail_outputs, write_phase4_monthly_summary

from _phase1_acceptance_support import configure_template_for_mode
from _phase2_support import write_phase2_scenario
from _phase3_support import write_phase3_scenario
from _phase4_support import read_csv_rows as read_phase4_rows, write_phase4_scenario


def test_phase4_acceptance_runs_from_authoritative_phase3_output(tmp_path: Path) -> None:
    workbook_path = tmp_path / "phase4_acceptance.xlsx"
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
    )
    phase2_result = run_phase2_scenario(phase2_scenario)
    phase2_output_path = write_phase2_outputs(
        tmp_path / "phase2_deterministic_cascade.csv",
        phase2_result.outputs,
    )

    phase3_scenario = write_phase3_scenario(
        tmp_path / "phase3",
        scenario_name=import_result.context.scenario_name,
        phase2_deterministic_cascade_path=phase2_output_path,
    )
    phase3_result = run_phase3_scenario(phase3_scenario)
    phase3_output_path = write_phase3_outputs(
        tmp_path / "phase3_trade_layer.csv",
        phase3_result.outputs,
    )

    phase4_scenario = write_phase4_scenario(
        tmp_path / "phase4",
        scenario_name=import_result.context.scenario_name,
        phase3_trade_layer_path=phase3_output_path,
    )
    phase4_result = run_phase4_scenario(phase4_scenario)
    detail_output_path = write_phase4_detail_outputs(
        tmp_path / "phase4_schedule_detail.csv",
        phase4_result.schedule_detail,
    )
    monthly_summary_path = write_phase4_monthly_summary(
        tmp_path / "phase4_monthly_summary.csv",
        phase4_result.monthly_summary,
    )

    detail_rows = read_phase4_rows(detail_output_path)
    summary_rows = read_phase4_rows(monthly_summary_path)

    assert not phase4_result.validation.has_errors
    assert detail_output_path.name == "phase4_schedule_detail.csv"
    assert monthly_summary_path.name == "phase4_monthly_summary.csv"
    assert any(row["stage"] == "FG" for row in detail_rows)
    assert any(row["stage"] == "DP" for row in detail_rows)
    assert any(row["stage"] == "DS" for row in detail_rows)
    assert any(row["stage"] == "SS" for row in detail_rows)
    assert any(float(row["fg_release_units"]) > 0 for row in summary_rows)
    assert any(float(row["dp_release_units"]) > 0 for row in summary_rows)
    assert any(float(row["ds_release_quantity_mg"]) > 0 for row in summary_rows)
    assert any(float(row["ss_release_units"]) > 0 for row in summary_rows)
    assert {"CML_Incident", "CML_Prevalent"} <= {row["module"] for row in summary_rows}
    assert len(
        {
            (
                row["scenario_name"],
                row["geography_code"],
                row["module"],
                row["month_index"],
            )
            for row in summary_rows
        }
    ) == len(summary_rows)


def test_phase4_checked_in_baseline_schedule_is_materially_less_noisy() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    result = run_phase4_scenario(repo_root / "config" / "scenarios" / "baseline_phase4.toml")

    assert not result.validation.has_errors
    assert any(row.stage == "DS" and row.module == "ALL" and row.geography_code == "ALL" for row in result.schedule_detail)
    assert any(row.stage == "DP" and row.module == "ALL" and row.geography_code == "ALL" for row in result.schedule_detail)
    assert any(row.stage == "FG" and row.module == "ALL" for row in result.schedule_detail)
    assert any(row.stage == "SS" and row.module == "ALL" for row in result.schedule_detail)
    assert any("precedes month 1 by design" in row.notes.lower() for row in result.schedule_detail)
