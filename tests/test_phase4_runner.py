from __future__ import annotations

from pathlib import Path

import pytest

from cbx250_model.constants import PHYSICAL_SHARED_GEOGRAPHY, PHYSICAL_SHARED_MODULE
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
    assert config.review.supply_gap_tolerance_units == pytest.approx(0.000001)
    assert config.review.capacity_clip_tolerance_units == pytest.approx(0.000001)
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
    assert len(dp_rows) == 1
    assert len(ds_rows) == 1
    assert len(ss_rows) == 1
    assert dp_rows[0].module == PHYSICAL_SHARED_MODULE
    assert dp_rows[0].geography_code == PHYSICAL_SHARED_GEOGRAPHY
    assert ds_rows[0].module == PHYSICAL_SHARED_MODULE
    assert ds_rows[0].geography_code == PHYSICAL_SHARED_GEOGRAPHY
    assert fg_rows[0].module == PHYSICAL_SHARED_MODULE
    assert fg_rows[0].geography_code == "US"
    assert ss_rows[0].module == PHYSICAL_SHARED_MODULE
    assert ss_rows[0].geography_code == "US"
    assert dp_rows[0].batch_quantity >= 100000.0
    assert ds_rows[0].batch_quantity >= 2_000_000.0
    assert ss_rows[0].batch_quantity == pytest.approx(100000.0)
    assert summary_row.supply_gap_flag is False
    assert "before month 1 by design" in summary_row.notes.lower()


def test_phase4_unmet_demand_excludes_channel_build_when_underlying_patient_supply_is_supported(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase4_scenario(
        tmp_path,
        scenario_name="CHANNEL_BUILD",
        phase3_rows=[
            'CHANNEL_BUILD,US,AML,1L_fit,1,2029-01-01,100000,0,0,0,0,0,200000,0,0,200000,1.4,true,100000,0,0,0,0,0,false,{},fixture',
        ],
    )

    result = run_phase4_scenario(scenario_path)
    summary_row = result.monthly_summary[0]

    assert not result.validation.has_errors
    assert summary_row.fg_release_units == pytest.approx(100000.0)
    assert summary_row.ex_factory_fg_demand_units == pytest.approx(200000.0)
    assert summary_row.unmet_demand_units == pytest.approx(0.0)
    assert summary_row.capacity_flag is False
    assert summary_row.supply_gap_flag is False
    assert "channel-build inflation excluded" in summary_row.notes.lower()


def test_phase4_dp_ds_ss_batching_uses_required_total_instead_of_minimum_batch_padding(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase4_scenario(
        tmp_path,
        scenario_name="BATCHING",
        phase3_rows=[
            'BATCHING,US,AML,1L_fit,1,2029-01-01,100000,0,0,0,0,0,100000,0,0,100000,1,false,0,0,0,0,0,0,false,{},fixture',
        ],
    )

    result = run_phase4_scenario(scenario_path)
    summary_row = result.monthly_summary[0]
    dp_rows = [row for row in result.schedule_detail if row.stage == "DP"]
    ds_rows = [row for row in result.schedule_detail if row.stage == "DS"]
    ss_rows = [row for row in result.schedule_detail if row.stage == "SS"]

    assert not result.validation.has_errors
    assert len(dp_rows) == 1
    assert len(ds_rows) == 1
    assert len(ss_rows) == 1
    assert dp_rows[0].allocated_support_quantity == pytest.approx(summary_row.dp_release_units)
    assert ds_rows[0].allocated_support_quantity == pytest.approx(summary_row.ds_release_quantity_mg)
    assert ss_rows[0].allocated_support_quantity == pytest.approx(summary_row.ss_release_units)
    assert dp_rows[0].batch_quantity >= dp_rows[0].allocated_support_quantity
    assert ds_rows[0].batch_quantity >= ds_rows[0].allocated_support_quantity
    assert ss_rows[0].batch_quantity >= ss_rows[0].allocated_support_quantity


def test_phase4_small_allocation_quantities_are_kept_in_allocation_audit_not_as_fake_batches(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase4_scenario(
        tmp_path,
        scenario_name="PHYSICAL_VS_ALLOCATION",
        phase3_rows=[
            'PHYSICAL_VS_ALLOCATION,US,AML,1L_fit,1,2029-01-01,100000,0,0,0,0,0,100000,0,0,100000,1,false,0,0,0,0,0,0,false,{},fixture',
            'PHYSICAL_VS_ALLOCATION,US,AML,1L_fit,2,2029-02-01,141.446666666667,0,0,0,0,0,141.446666666667,0,0,141.446666666667,1,false,0,0,0,0,0,0,false,{},fixture',
        ],
    )

    result = run_phase4_scenario(scenario_path)
    dp_rows = [row for row in result.schedule_detail if row.stage == "DP"]
    dp_allocation_rows = [row for row in result.allocation_detail if row.stage == "DP"]

    assert not result.validation.has_errors
    assert len(dp_rows) == 1
    assert dp_rows[0].module == PHYSICAL_SHARED_MODULE
    assert dp_rows[0].geography_code == PHYSICAL_SHARED_GEOGRAPHY
    assert dp_rows[0].batch_quantity >= 100000.0
    assert dp_rows[0].allocated_support_quantity == pytest.approx(
        sum(row.allocated_support_quantity for row in dp_allocation_rows)
    )
    assert {row.allocated_to_demand_month_index for row in dp_allocation_rows} == {1, 2}
    assert any(
        row.allocated_support_quantity < 1000.0 for row in dp_allocation_rows
    )
    assert "allocation" in dp_rows[0].notes.lower()


def test_phase4_shared_ds_dp_and_geography_level_fg_ss_preserve_module_traceability(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase4_scenario(
        tmp_path,
        scenario_name="SHARED_GRAIN",
        phase3_rows=[
            'SHARED_GRAIN,US,AML,1L_fit,1,2029-01-01,60000,0,0,0,0,0,60000,0,0,60000,1,false,0,0,0,0,0,0,false,{},fixture',
            'SHARED_GRAIN,US,MDS,HR_MDS,1,2029-01-01,40000,0,0,0,0,0,40000,0,0,40000,1,false,0,0,0,0,0,0,false,{},fixture',
        ],
    )

    result = run_phase4_scenario(scenario_path)

    ds_rows = [row for row in result.schedule_detail if row.stage == "DS"]
    dp_rows = [row for row in result.schedule_detail if row.stage == "DP"]
    fg_rows = [row for row in result.schedule_detail if row.stage == "FG"]
    ss_rows = [row for row in result.schedule_detail if row.stage == "SS"]
    fg_allocations = [row for row in result.allocation_detail if row.stage == "FG"]

    assert not result.validation.has_errors
    assert len(ds_rows) == 1
    assert len(dp_rows) == 1
    assert len(fg_rows) >= 1
    assert len(ss_rows) >= 1
    assert ds_rows[0].module == PHYSICAL_SHARED_MODULE
    assert ds_rows[0].geography_code == PHYSICAL_SHARED_GEOGRAPHY
    assert dp_rows[0].module == PHYSICAL_SHARED_MODULE
    assert dp_rows[0].geography_code == PHYSICAL_SHARED_GEOGRAPHY
    assert {row.module for row in fg_rows} == {PHYSICAL_SHARED_MODULE}
    assert {row.geography_code for row in fg_rows} == {"US"}
    assert {row.module for row in ss_rows} == {PHYSICAL_SHARED_MODULE}
    assert {row.geography_code for row in ss_rows} == {"US"}
    assert {row.allocated_module for row in fg_allocations} == {"AML", "MDS"}
    assert {row.allocated_geography_code for row in fg_allocations} == {"US"}


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


def test_phase4_near_zero_capacity_clip_and_gap_do_not_trigger_flags(tmp_path: Path) -> None:
    scenario_path = write_phase4_scenario(
        tmp_path,
        scenario_name="TOLERANCE",
        phase3_rows=[
            'TOLERANCE,US,AML,1L_fit,1,2029-01-01,98.0000000000001,0,0,0,0,0,98.0000000000001,0,0,98.0000000000001,1,false,0,0,0,0,0,0,false,{},fixture',
        ],
        dp_min_batch_size_units=100.0,
        dp_max_batch_size_units=100.0,
        dp_annual_capacity_batches=1,
        ds_max_batch_size_kg=4.0,
        ds_annual_capacity_batches=5,
        ss_batch_size_units=100000.0,
        ss_annual_capacity_batches=10,
    )

    result = run_phase4_scenario(scenario_path)
    summary_row = result.monthly_summary[0]

    assert not result.validation.has_errors
    assert summary_row.unmet_demand_units < 0.000001
    assert summary_row.capacity_flag is False
    assert summary_row.supply_gap_flag is False


def test_phase4_material_capacity_clip_above_tolerance_still_triggers_flags(tmp_path: Path) -> None:
    scenario_path = write_phase4_scenario(
        tmp_path,
        scenario_name="TOLERANCE_MATERIAL",
        phase3_rows=[
            'TOLERANCE_MATERIAL,US,AML,1L_fit,1,2029-01-01,196,0,0,0,0,0,196,0,0,196,1,false,0,0,0,0,0,0,false,{},fixture',
        ],
        dp_min_batch_size_units=100.0,
        dp_max_batch_size_units=100.0,
        dp_annual_capacity_batches=1,
    )

    result = run_phase4_scenario(scenario_path)
    summary_row = result.monthly_summary[0]

    assert not result.validation.has_errors
    assert summary_row.unmet_demand_units > 0.000001
    assert summary_row.capacity_flag is True
    assert summary_row.supply_gap_flag is True


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
    allocation_path = tmp_path / "allocation.csv"
    from cbx250_model.phase4.writer import write_phase4_allocation_outputs

    write_phase4_allocation_outputs(allocation_path, result.allocation_detail)
    detail_rows = read_csv_rows(detail_path)
    summary_rows = read_csv_rows(summary_path)
    allocation_rows = read_csv_rows(allocation_path)

    assert not result.validation.has_errors
    assert len({row.key for row in result.schedule_detail}) == len(result.schedule_detail)
    assert len({row.key for row in result.monthly_summary}) == len(result.monthly_summary)
    assert len({row.key for row in result.allocation_detail}) == len(result.allocation_detail)
    assert any(row["stage"] == "DS" for row in detail_rows)
    assert "allocated_support_quantity" in detail_rows[0]
    assert "source_batch_number" in allocation_rows[0]
    assert float(summary_rows[0]["ds_release_quantity_g"]) == pytest.approx(
        float(summary_rows[0]["ds_release_quantity_mg"]) / 1000.0
    )


def test_phase4_pre_month1_starts_remain_allowed_without_gap_flags(tmp_path: Path) -> None:
    scenario_path = write_phase4_scenario(
        tmp_path,
        scenario_name="PREHORIZON",
        phase3_rows=[
            'PREHORIZON,US,AML,1L_fit,1,2029-01-01,60000,0,0,0,0,0,60000,0,0,60000,1,false,0,0,0,0,0,0,false,{},fixture',
        ],
    )

    result = run_phase4_scenario(scenario_path)

    assert not result.validation.has_errors
    assert result.monthly_summary[0].supply_gap_flag is False
    assert any(row.planned_start_month_index < 1 for row in result.schedule_detail)
    assert all(
        row.supply_gap_flag is False
        for row in result.schedule_detail
        if row.planned_start_month_index < 1
    )
    assert any("precedes month 1 by design" in row.notes.lower() for row in result.schedule_detail)
