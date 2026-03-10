from __future__ import annotations

from pathlib import Path

import pytest

from cbx250_model.phase4.config_schema import load_phase4_config
from cbx250_model.phase4.runner import run_phase4_scenario
from cbx250_model.phase4.writer import write_phase4_detail_outputs, write_phase4_monthly_summary

from _phase4_support import read_csv_rows, write_phase4_scenario


def test_phase4_checked_in_base_config_uses_expected_defaults() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config = load_phase4_config(repo_root / "config" / "scenarios" / "base_phase4.toml")

    assert config.model.build_scope == "deterministic_production_schedule"
    assert config.model.upstream_demand_contract == "phase3_trade_layer.csv"
    assert config.conversion.dp_to_fg_yield == pytest.approx(0.98)
    assert config.conversion.ds_to_dp_yield == pytest.approx(0.90)
    assert config.conversion.ds_qty_per_dp_unit_mg == pytest.approx(1.0)
    assert config.review.bullwhip_amplification_threshold == pytest.approx(1.25)
    assert config.dp.min_campaign_batches == 3
    assert config.ds.annual_capacity_batches == 5
    assert config.ss.batch_size_units == pytest.approx(100000.0)


def test_phase4_workback_scheduling_applies_stage_lead_times_and_campaign_constraints(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase4_scenario(
        tmp_path,
        scenario_name="WORKBACK",
        phase3_rows=[
            'WORKBACK,US,AML,1L_fit,1,2029-01-01,100000,0,0,0,0,0,100000,0,0,100000,1,false,0,0,0,0,0,0,false,{},fixture',
        ],
    )

    result = run_phase4_scenario(scenario_path)
    assert not result.validation.has_errors

    fg_rows = [row for row in result.schedule_detail if row.stage == "FG"]
    dp_rows = [row for row in result.schedule_detail if row.stage == "DP"]
    ds_rows = [row for row in result.schedule_detail if row.stage == "DS"]
    ss_rows = [row for row in result.schedule_detail if row.stage == "SS"]
    summary_row = result.monthly_summary[0]

    assert summary_row.fg_release_units == pytest.approx(100000.0)
    assert fg_rows[0].planned_release_month_index == 1
    assert fg_rows[0].planned_start_month_index == 0
    assert dp_rows[0].planned_release_month_index == -1
    assert dp_rows[0].planned_start_month_index == -5
    assert ds_rows[0].planned_release_month_index == -6
    assert ds_rows[0].planned_start_month_index == -11
    assert ss_rows[0].planned_release_month_index == 1
    assert ss_rows[0].planned_start_month_index == -5
    assert len(dp_rows) == 3
    assert len(ds_rows) == 3
    assert len(ss_rows) == 3


def test_phase4_annual_capacity_flags_and_unmet_demand_trigger_under_large_volume(tmp_path: Path) -> None:
    phase3_rows = [
        f'CAPACITY,US,AML,1L_fit,{month_index},2029-{month_index:02d}-01,500000,0,0,0,0,0,500000,0,0,500000,1,false,0,0,0,0,0,0,false,{{}},fixture'
        for month_index in range(1, 13)
    ]
    scenario_path = write_phase4_scenario(
        tmp_path,
        scenario_name="CAPACITY",
        phase3_rows=phase3_rows,
    )

    result = run_phase4_scenario(scenario_path)

    assert not result.validation.has_errors
    assert any(row.capacity_flag for row in result.monthly_summary)
    assert any(row.unmet_demand_units > 0 for row in result.monthly_summary)
    assert any(row.supply_gap_flag for row in result.monthly_summary)
    assert any(row.capacity_flag for row in result.schedule_detail if row.stage == "DP")


def test_phase4_ss_sync_rule_flags_when_cumulative_fg_exceeds_ss(tmp_path: Path) -> None:
    phase3_rows = [
        f'SS_SYNC,US,AML,1L_fit,{month_index},2029-{month_index:02d}-01,500000,0,0,0,0,0,500000,0,0,500000,1,false,0,0,0,0,0,0,false,{{}},fixture'
        for month_index in range(1, 5)
    ]
    scenario_path = write_phase4_scenario(
        tmp_path,
        scenario_name="SS_SYNC",
        phase3_rows=phase3_rows,
        ss_annual_capacity_batches=10,
    )

    result = run_phase4_scenario(scenario_path)

    assert not result.validation.has_errors
    assert any(row.ss_fg_sync_flag is False for row in result.monthly_summary)


def test_phase4_bullwhip_review_flag_triggers_within_review_window(tmp_path: Path) -> None:
    scenario_path = write_phase4_scenario(
        tmp_path,
        scenario_name="BULLWHIP",
        phase3_rows=[
            'BULLWHIP,US,AML,1L_fit,1,2029-01-01,100000,0,0,0,0,0,200000,0,0,200000,1.5,true,100000,0,0,0,0,0,false,{},fixture',
        ],
    )

    result = run_phase4_scenario(scenario_path)

    assert not result.validation.has_errors
    assert result.monthly_summary[0].bullwhip_review_flag is True


def test_phase4_cml_prevalent_stepdown_hook_applies_in_forward_window(tmp_path: Path) -> None:
    scenario_path = write_phase4_scenario(
        tmp_path,
        scenario_name="STEPDOWN",
        phase3_rows=[
            'STEPDOWN,US,CML_Prevalent,CML_Prevalent,7,2029-07-01,50,0,0,0,0,0,120,0,0,120,1.0,false,70,0,0,0,0,0,false,{},fixture',
        ],
        projected_cml_prevalent_bolus_exhaustion_month_index=12,
        cml_prevalent_forward_window_months=6,
    )

    result = run_phase4_scenario(scenario_path)

    assert not result.validation.has_errors
    assert result.monthly_summary[0].stepdown_applied is True
    assert "step-down" in result.monthly_summary[0].notes.lower()


def test_phase4_output_keys_are_unique_and_writers_emit_machine_readable_csv(tmp_path: Path) -> None:
    scenario_path = write_phase4_scenario(
        tmp_path,
        scenario_name="UNIQUE",
        phase3_rows=[
            'UNIQUE,US,AML,1L_fit,1,2029-01-01,60000,0,0,0,0,0,60000,0,0,60000,1,false,0,0,0,0,0,0,false,{},fixture',
            'UNIQUE,US,AML,RR,1,2029-01-01,40000,0,0,0,0,0,40000,0,0,40000,1,false,0,0,0,0,0,0,false,{},fixture',
        ],
    )

    result = run_phase4_scenario(scenario_path)
    detail_path = write_phase4_detail_outputs(tmp_path / "detail.csv", result.schedule_detail)
    summary_path = write_phase4_monthly_summary(tmp_path / "summary.csv", result.monthly_summary)
    detail_rows = read_csv_rows(detail_path)
    summary_rows = read_csv_rows(summary_path)

    assert not result.validation.has_errors
    assert len({row.key for row in result.schedule_detail}) == len(result.schedule_detail)
    assert len({row.key for row in result.monthly_summary}) == len(result.monthly_summary)
    assert any(row["stage"] == "DS" for row in detail_rows)
    assert float(summary_rows[0]["ds_release_quantity_g"]) == pytest.approx(
        float(summary_rows[0]["ds_release_quantity_mg"]) / 1000.0
    )
