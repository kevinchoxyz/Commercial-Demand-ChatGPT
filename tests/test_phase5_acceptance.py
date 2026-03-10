from __future__ import annotations

from pathlib import Path

from cbx250_model.inputs.excel_import import import_commercial_forecast_workbook
from cbx250_model.inputs.excel_template import build_commercial_forecast_template
from cbx250_model.phase2.runner import run_phase2_scenario
from cbx250_model.phase2.writer import write_phase2_outputs
from cbx250_model.phase3.runner import run_phase3_scenario
from cbx250_model.phase3.writer import write_phase3_outputs
from cbx250_model.phase4.runner import run_phase4_scenario
from cbx250_model.phase4.writer import write_phase4_detail_outputs, write_phase4_monthly_summary
from cbx250_model.phase5.runner import run_phase5_scenario
from cbx250_model.phase5.writer import (
    write_phase5_cohort_audit,
    write_phase5_inventory_detail,
    write_phase5_monthly_summary as write_phase5_monthly_summary_output,
)

from _phase1_acceptance_support import configure_template_for_mode
from _phase2_support import write_phase2_scenario
from _phase3_support import write_phase3_scenario
from _phase4_support import write_phase4_scenario
from _phase5_support import read_csv_rows as read_phase5_rows, write_phase5_scenario


def test_phase5_acceptance_runs_from_authoritative_phase3_and_phase4_outputs(tmp_path: Path) -> None:
    workbook_path = tmp_path / "phase5_acceptance.xlsx"
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
    phase4_detail_path = write_phase4_detail_outputs(
        tmp_path / "phase4_schedule_detail.csv",
        phase4_result.schedule_detail,
    )
    phase4_summary_path = write_phase4_monthly_summary(
        tmp_path / "phase4_monthly_summary.csv",
        phase4_result.monthly_summary,
    )

    phase5_scenario = write_phase5_scenario(
        tmp_path / "phase5",
        scenario_name=import_result.context.scenario_name,
        phase3_trade_layer_path=phase3_output_path,
        phase4_schedule_detail_path=phase4_detail_path,
        phase4_monthly_summary_path=phase4_summary_path,
    )
    phase5_result = run_phase5_scenario(phase5_scenario)
    detail_output_path = write_phase5_inventory_detail(
        tmp_path / "phase5_inventory_detail.csv",
        phase5_result.inventory_detail,
    )
    summary_output_path = write_phase5_monthly_summary_output(
        tmp_path / "phase5_monthly_summary.csv",
        phase5_result.monthly_summary,
    )
    cohort_output_path = write_phase5_cohort_audit(
        tmp_path / "phase5_cohort_audit.csv",
        phase5_result.cohort_audit,
    )

    detail_rows = read_phase5_rows(detail_output_path)
    summary_rows = read_phase5_rows(summary_output_path)
    cohort_rows = read_phase5_rows(cohort_output_path)

    assert not phase5_result.validation.has_errors
    assert detail_output_path.name == "phase5_inventory_detail.csv"
    assert summary_output_path.name == "phase5_monthly_summary.csv"
    assert cohort_output_path.name == "phase5_cohort_audit.csv"
    assert {"DS", "DP", "FG_Central", "SS_Central", "SubLayer1_FG", "SubLayer2_FG"} <= {
        row["material_node"] for row in detail_rows
    }
    assert any(float(row["fg_inventory_units"]) >= 0 for row in summary_rows)
    if cohort_rows:
        assert any(row["material_node"] == "FG_Central" for row in cohort_rows)
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
