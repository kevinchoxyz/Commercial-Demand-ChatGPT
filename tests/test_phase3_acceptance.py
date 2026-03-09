from __future__ import annotations

from pathlib import Path

import pytest

from cbx250_model.inputs.excel_import import import_commercial_forecast_workbook
from cbx250_model.inputs.excel_template import build_commercial_forecast_template
from cbx250_model.phase2.runner import run_phase2_scenario
from cbx250_model.phase2.writer import write_phase2_outputs
from cbx250_model.phase3.runner import run_phase3_scenario
from cbx250_model.phase3.writer import write_phase3_outputs

from _phase1_acceptance_support import configure_template_for_mode
from _phase2_support import read_csv_rows as read_phase2_csv_rows, write_phase2_scenario
from _phase3_support import read_csv_rows as read_phase3_csv_rows, write_phase3_scenario


def test_phase3_acceptance_runs_from_authoritative_phase2_output(tmp_path: Path) -> None:
    workbook_path = tmp_path / "phase3_acceptance.xlsx"
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
    phase2_rows = read_phase2_csv_rows(phase2_output_path)
    phase3_rows = read_phase3_csv_rows(phase3_output_path)

    assert not phase3_result.validation.has_errors
    assert phase2_output_path.name == "phase2_deterministic_cascade.csv"
    assert phase3_output_path.name == "phase3_trade_layer.csv"
    assert sum(float(row["patient_fg_demand_units"]) for row in phase3_rows) == pytest.approx(
        sum(float(row["fg_units_required"]) for row in phase2_rows)
    )
    assert any(float(row["sublayer2_pull_units"]) > 0 for row in phase3_rows)
    assert any(float(row["ex_factory_fg_demand_units"]) > 0 for row in phase3_rows)
    assert {"CML_Incident", "CML_Prevalent"} <= {row["module"] for row in phase3_rows}
    assert len(
        {
            (
                row["scenario_name"],
                row["geography_code"],
                row["module"],
                row["segment_code"],
                row["month_index"],
            )
            for row in phase3_rows
        }
    ) == len(phase3_rows)
