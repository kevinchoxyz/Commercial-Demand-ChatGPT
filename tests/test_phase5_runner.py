from __future__ import annotations

from pathlib import Path
import pytest

from cbx250_model.phase5.config_schema import load_phase5_config
from cbx250_model.phase5.runner import run_phase5_scenario
from cbx250_model.phase5.writer import (
    write_phase5_cohort_audit,
    write_phase5_inventory_detail,
    write_phase5_monthly_summary,
)

from _phase5_support import read_csv_rows, write_phase5_scenario


def _summary_row(result, geography_code: str, module: str, month_index: int):
    return next(
        row
        for row in result.monthly_summary
        if row.geography_code == geography_code
        and row.module == module
        and row.month_index == month_index
    )


def test_phase5_checked_in_base_config_uses_expected_defaults() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config = load_phase5_config(repo_root / "config" / "scenarios" / "base_phase5.toml")

    assert config.model.build_scope == "deterministic_inventory_shelf_life"
    assert config.model.upstream_supply_contract == "phase3_trade_layer.csv + phase4_schedule_outputs.csv"
    assert config.starting_inventory.fg_units == pytest.approx(0.0)
    assert config.shelf_life.ds_months == 48
    assert config.shelf_life.dp_months == 36
    assert config.shelf_life.fg_months == 36
    assert config.shelf_life.ss_months == 48
    assert config.policy.excess_inventory_threshold_months_of_cover == pytest.approx(18.0)
    assert config.policy.fefo_enabled is True
    assert config.policy.ss_fg_match_required is True
    assert config.policy.allow_prelaunch_inventory_build is True
    assert config.conversion.ds_to_dp_yield == pytest.approx(0.90)


def test_phase5_pre_month1_releases_become_opening_inventory(tmp_path: Path) -> None:
    scenario_path = write_phase5_scenario(
        tmp_path,
        scenario_name="PRELAUNCH",
        phase3_rows=[
            "PRELAUNCH,US,AML,1L_fit,1,2029-01-01,20,0,0,0,0,0,20,0,0,20,1,false,0,0,0,0,0,0,false,{},fixture",
        ],
        phase4_detail_rows=[
            "PRELAUNCH,FG,AML,US,FG-AML-US-2028-001,1,2029-01-01,0,2028-12-01,-1,2028-11-01,0,2028-12-01,50,units,50,1,999,batches_per_year,false,false,false,false,true,fixture",
            "PRELAUNCH,SS,AML,US,SS-AML-US-2028-001,1,2029-01-01,0,2028-12-01,-1,2028-11-01,0,2028-12-01,50,units,50,1,999,batches_per_year,false,false,false,false,true,fixture",
        ],
        phase4_summary_rows=[
            "PRELAUNCH,US,AML,1,2029-01-01,20,20,20,0,0,50,0,0,0,0,50,50,50,0,false,false,false,false,true,false,fixture",
        ],
    )

    result = run_phase5_scenario(scenario_path)
    fg_row = next(
        row for row in result.inventory_detail if row.material_node == "FG_Central" and row.month_index == 1
    )
    ss_row = next(
        row for row in result.inventory_detail if row.material_node == "SS_Central" and row.month_index == 1
    )
    sublayer2_row = next(
        row for row in result.inventory_detail if row.material_node == "SubLayer2_FG" and row.month_index == 1
    )

    assert not result.validation.has_errors
    assert fg_row.opening_inventory == pytest.approx(30.0)
    assert fg_row.issues == pytest.approx(0.0)
    assert fg_row.ending_inventory == pytest.approx(30.0)
    assert ss_row.opening_inventory == pytest.approx(30.0)
    assert sublayer2_row.opening_inventory == pytest.approx(20.0)
    assert sublayer2_row.issues == pytest.approx(20.0)
    assert sublayer2_row.ending_inventory == pytest.approx(0.0)
    assert "pre-month-1 released inventory" in fg_row.notes.lower()


def test_phase5_receipts_follow_physical_phase4_releases_not_fake_later_allocation_rows(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase5_scenario(
        tmp_path,
        scenario_name="PHYSICAL_RELEASES",
        phase3_rows=[
            "PHYSICAL_RELEASES,US,AML,1L_fit,1,2029-01-01,49000,0,0,0,0,0,49000,0,0,49000,1,false,0,0,0,0,0,0,false,{},fixture",
            "PHYSICAL_RELEASES,US,AML,1L_fit,2,2029-02-01,49000,0,0,0,0,0,49000,0,0,49000,1,false,0,0,0,0,0,0,false,{},fixture",
        ],
        phase4_detail_rows=[
            "PHYSICAL_RELEASES,DP,AML,US,DP-AML-US-2029-001,1,2029-01-01,1,2029-01-01,1,2029-01-01,1,2029-01-01,100000,units,100000,1,999,batches_per_year,false,false,false,false,true,fixture",
            "PHYSICAL_RELEASES,FG,AML,US,FG-AML-US-2029-001,1,2029-01-01,1,2029-01-01,1,2029-01-01,1,2029-01-01,49000,units,49000,1,999,batches_per_year,false,false,false,false,true,fixture",
            "PHYSICAL_RELEASES,FG,AML,US,FG-AML-US-2029-002,2,2029-02-01,2,2029-02-01,2,2029-02-01,2,2029-02-01,49000,units,98000,2,999,batches_per_year,false,false,false,false,true,fixture",
            "PHYSICAL_RELEASES,SS,AML,US,SS-AML-US-2029-001,1,2029-01-01,1,2029-01-01,1,2029-01-01,1,2029-01-01,100000,units,100000,1,999,batches_per_year,false,false,false,false,true,fixture",
        ],
        phase4_summary_rows=[
            "PHYSICAL_RELEASES,US,AML,1,2029-01-01,49000,49000,49000,0,0,49000,50000,0,0,0,49000,49000,49000,0,false,false,false,false,true,false,fixture",
            "PHYSICAL_RELEASES,US,AML,2,2029-02-01,49000,49000,49000,0,0,49000,50000,0,0,0,49000,98000,98000,0,false,false,false,false,true,false,fixture",
        ],
    )

    result = run_phase5_scenario(scenario_path)
    dp_month1 = next(
        row for row in result.inventory_detail if row.material_node == "DP" and row.month_index == 1
    )
    dp_month2 = next(
        row for row in result.inventory_detail if row.material_node == "DP" and row.month_index == 2
    )

    assert not result.validation.has_errors
    assert dp_month1.receipts == pytest.approx(100000.0)
    assert dp_month1.issues == pytest.approx(50000.0)
    assert dp_month1.ending_inventory == pytest.approx(50000.0)
    assert dp_month2.opening_inventory == pytest.approx(50000.0)
    assert dp_month2.receipts == pytest.approx(0.0)
    assert dp_month2.issues == pytest.approx(50000.0)
    assert dp_month2.ending_inventory == pytest.approx(0.0)


def test_phase5_fefo_consumes_oldest_inventory_and_expired_inventory_is_not_consumed(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase5_scenario(
        tmp_path,
        scenario_name="FEFO",
        fg_shelf_life_months=2,
        ss_shelf_life_months=2,
        phase3_rows=[
            "FEFO,US,AML,1L_fit,1,2029-01-01,0,0,0,0,0,0,0,0,0,0,1,false,0,0,0,0,0,0,false,{},fixture",
            "FEFO,US,AML,1L_fit,2,2029-02-01,5,0,0,0,0,0,5,0,0,5,1,false,0,0,0,0,0,0,false,{},fixture",
            "FEFO,US,AML,1L_fit,3,2029-03-01,10,0,0,0,0,0,10,0,0,10,1,false,0,0,0,0,0,0,false,{},fixture",
        ],
        phase4_detail_rows=[
            "FEFO,FG,AML,US,FG-AML-US-2029-001,1,2029-01-01,1,2029-01-01,1,2029-01-01,1,2029-01-01,10,units,10,1,999,batches_per_year,false,false,false,false,true,fixture",
            "FEFO,SS,AML,US,SS-AML-US-2029-001,1,2029-01-01,1,2029-01-01,1,2029-01-01,1,2029-01-01,10,units,10,1,999,batches_per_year,false,false,false,false,true,fixture",
            "FEFO,FG,AML,US,FG-AML-US-2029-002,2,2029-02-01,2,2029-02-01,2,2029-02-01,2,2029-02-01,10,units,20,2,999,batches_per_year,false,false,false,false,true,fixture",
            "FEFO,SS,AML,US,SS-AML-US-2029-002,2,2029-02-01,2,2029-02-01,2,2029-02-01,2,2029-02-01,10,units,20,2,999,batches_per_year,false,false,false,false,true,fixture",
        ],
        phase4_summary_rows=[
            "FEFO,US,AML,1,2029-01-01,0,0,0,0,0,10,0,0,0,0,10,10,10,0,false,false,false,false,true,false,fixture",
            "FEFO,US,AML,2,2029-02-01,5,5,5,0,0,10,0,0,0,0,10,20,20,0,false,false,false,false,true,false,fixture",
            "FEFO,US,AML,3,2029-03-01,10,10,10,0,0,0,0,0,0,0,0,20,20,0,false,false,false,false,true,false,fixture",
        ],
    )

    result = run_phase5_scenario(scenario_path)
    month2_fg_cohorts = [
        row
        for row in result.cohort_audit
        if row.material_node == "FG_Central" and row.month_index == 2
    ]
    month3_summary = _summary_row(result, "US", "ALL", 3)

    assert not result.validation.has_errors
    assert any(
        row.original_receipt_month_index == 1 and row.ending_quantity == pytest.approx(5.0)
        for row in month2_fg_cohorts
    )
    assert any(
        row.original_receipt_month_index == 2 and row.ending_quantity == pytest.approx(10.0)
        for row in month2_fg_cohorts
    )
    assert month3_summary.expired_fg_units == pytest.approx(5.0)
    assert month3_summary.stockout_flag is False


def test_phase5_stockout_flag_triggers_only_on_true_shortage_and_no_starting_inventory_default(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase5_scenario(
        tmp_path,
        scenario_name="STOCKOUT",
        phase3_rows=[
            "STOCKOUT,US,AML,1L_fit,1,2029-01-01,10,0,0,0,0,0,10,0,0,10,1,false,0,0,0,0,0,0,false,{},fixture",
        ],
        phase4_summary_rows=[
            "STOCKOUT,US,AML,1,2029-01-01,10,10,10,0,0,0,0,0,0,0,0,0,0,0,false,false,false,false,true,false,fixture",
        ],
    )

    result = run_phase5_scenario(scenario_path)
    fg_row = next(
        row for row in result.inventory_detail if row.material_node == "FG_Central" and row.month_index == 1
    )

    assert not result.validation.has_errors
    assert fg_row.opening_inventory == pytest.approx(0.0)
    assert fg_row.required_administrable_demand_units == pytest.approx(10.0)
    assert fg_row.policy_excluded_channel_build_units == pytest.approx(0.0)
    assert fg_row.shortfall_units == pytest.approx(10.0)
    assert fg_row.stockout_flag is True
    assert _summary_row(result, "US", "ALL", 1).stockout_flag is True


def test_phase5_policy_excluded_channel_build_does_not_trigger_stockout_for_patient_supported_demand(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase5_scenario(
        tmp_path,
        scenario_name="CHANNEL_POLICY",
        phase3_rows=[
            "CHANNEL_POLICY,US,AML,1L_fit,1,2029-01-01,10,0,0,0,0,0,10,0,0,30,3,false,20,10,1,1,0,0,false,{},fixture",
        ],
        phase4_detail_rows=[
            "CHANNEL_POLICY,DS,AML,US,DS-AML-US-2029-001,1,2029-01-01,1,2029-01-01,1,2029-01-01,1,2029-01-01,11.904761904761905,mg,11.904761904761905,1,999,batches_per_year,false,false,false,false,true,fixture",
            "CHANNEL_POLICY,DP,AML,US,DP-AML-US-2029-001,1,2029-01-01,1,2029-01-01,1,2029-01-01,1,2029-01-01,10.204081632653061,units,10.204081632653061,1,999,batches_per_year,false,false,false,false,true,fixture",
            "CHANNEL_POLICY,FG,AML,US,FG-AML-US-2029-001,1,2029-01-01,1,2029-01-01,1,2029-01-01,1,2029-01-01,10,units,10,1,999,batches_per_year,false,false,false,false,true,fixture",
            "CHANNEL_POLICY,SS,AML,US,SS-AML-US-2029-001,1,2029-01-01,1,2029-01-01,1,2029-01-01,1,2029-01-01,10,units,10,1,999,batches_per_year,false,false,false,false,true,fixture",
        ],
        phase4_summary_rows=[
            "CHANNEL_POLICY,US,AML,1,2029-01-01,10,30,10,20,20,10,10.204081632653061,11.904761904761905,0.0119047619047619,1.19047619047619e-05,10,10,10,0,false,false,false,false,true,false,fixture",
        ],
    )

    result = run_phase5_scenario(scenario_path)
    fg_row = next(
        row for row in result.inventory_detail if row.material_node == "FG_Central" and row.month_index == 1
    )
    summary_row = _summary_row(result, "US", "ALL", 1)

    assert not result.validation.has_errors
    assert fg_row.demand_signal_units == pytest.approx(30.0)
    assert fg_row.required_administrable_demand_units == pytest.approx(0.0)
    assert fg_row.policy_excluded_channel_build_units == pytest.approx(20.0)
    assert fg_row.shortfall_units == pytest.approx(0.0)
    assert fg_row.stockout_flag is False
    assert summary_row.stockout_flag is False


def test_phase5_excess_inventory_flag_uses_months_of_cover_threshold(tmp_path: Path) -> None:
    scenario_path = write_phase5_scenario(
        tmp_path,
        scenario_name="EXCESS",
        starting_fg_units=100.0,
        starting_ss_units=100.0,
        excess_inventory_threshold_months_of_cover=3.0,
        allow_prelaunch_inventory_build=False,
        phase3_rows=[
            "EXCESS,US,AML,1L_fit,1,2029-01-01,10,0,0,0,0,0,10,0,0,10,1,false,0,0,0,0,0,0,false,{},fixture",
        ],
        phase4_summary_rows=[
            "EXCESS,US,AML,1,2029-01-01,10,10,10,0,0,0,0,0,0,0,0,0,0,0,false,false,false,false,true,false,fixture",
        ],
    )

    result = run_phase5_scenario(scenario_path)
    fg_row = next(
        row for row in result.inventory_detail if row.material_node == "FG_Central" and row.month_index == 1
    )

    assert not result.validation.has_errors
    assert fg_row.ending_inventory == pytest.approx(90.0)
    assert fg_row.cover_demand_units == pytest.approx(10.0)
    assert fg_row.effective_cover_demand_units == pytest.approx(10.0)
    assert fg_row.months_of_cover == pytest.approx(9.0)
    assert fg_row.excess_inventory_flag is True


def test_phase5_zero_current_required_demand_does_not_create_false_positive_excess_inventory(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase5_scenario(
        tmp_path,
        scenario_name="ZERO_DEMAND_EXCESS",
        starting_dp_units=50.0,
        phase3_rows=[
            "ZERO_DEMAND_EXCESS,US,AML,1L_fit,1,2029-01-01,0,0,0,0,0,0,0,0,0,0,0,false,0,0,0,0,0,0,false,{},fixture",
        ],
        phase4_summary_rows=[
            "ZERO_DEMAND_EXCESS,US,AML,1,2029-01-01,0,0,0,0,0,0,0,0,0,0,0,0,0,0,false,false,false,false,true,false,fixture",
        ],
    )

    result = run_phase5_scenario(scenario_path)
    dp_row = next(
        row for row in result.inventory_detail if row.material_node == "DP" and row.month_index == 1
    )
    summary_row = _summary_row(result, "US", "ALL", 1)

    assert not result.validation.has_errors
    assert dp_row.ending_inventory == pytest.approx(50.0)
    assert dp_row.required_administrable_demand_units == pytest.approx(0.0)
    assert dp_row.months_of_cover == pytest.approx(0.0)
    assert dp_row.excess_inventory_flag is False
    assert summary_row.excess_inventory_flag is False


def test_phase5_fg_ss_mismatch_logic_flags_unmatched_fg(tmp_path: Path) -> None:
    scenario_path = write_phase5_scenario(
        tmp_path,
        scenario_name="MISMATCH",
        starting_fg_units=20.0,
        starting_ss_units=10.0,
        phase3_rows=[
            "MISMATCH,US,AML,1L_fit,1,2029-01-01,0,0,0,0,0,0,0,0,0,0,1,false,0,0,0,0,0,0,false,{},fixture",
        ],
        phase4_summary_rows=[
            "MISMATCH,US,AML,1,2029-01-01,0,0,0,0,0,0,0,0,0,0,0,0,0,0,false,false,false,false,true,false,fixture",
        ],
    )

    result = run_phase5_scenario(scenario_path)
    summary_row = _summary_row(result, "US", "ALL", 1)

    assert not result.validation.has_errors
    assert summary_row.unmatched_fg_units == pytest.approx(10.0)
    assert summary_row.fg_ss_mismatch_flag is True


def test_phase5_output_keys_are_unique_and_writers_emit_machine_readable_csv(tmp_path: Path) -> None:
    scenario_path = write_phase5_scenario(
        tmp_path,
        scenario_name="UNIQUE",
        starting_fg_units=6.0,
        starting_ss_units=6.0,
        phase3_rows=[
            "UNIQUE,US,AML,1L_fit,1,2029-01-01,5,0,0,0,0,0,5,0,0,5,1,false,0,0,0,0,0,0,false,{},fixture",
        ],
        phase4_summary_rows=[
            "UNIQUE,US,AML,1,2029-01-01,5,5,5,0,0,0,0,0,0,0,0,0,0,0,false,false,false,false,true,false,fixture",
        ],
    )

    result = run_phase5_scenario(scenario_path)
    detail_path = write_phase5_inventory_detail(tmp_path / "inventory_detail.csv", result.inventory_detail)
    summary_path = write_phase5_monthly_summary(tmp_path / "inventory_summary.csv", result.monthly_summary)
    cohort_path = write_phase5_cohort_audit(tmp_path / "cohort_audit.csv", result.cohort_audit)

    detail_rows = read_csv_rows(detail_path)
    summary_rows = read_csv_rows(summary_path)
    cohort_rows = read_csv_rows(cohort_path)

    assert not result.validation.has_errors
    assert len({row.key for row in result.inventory_detail}) == len(result.inventory_detail)
    assert len({row.key for row in result.monthly_summary}) == len(result.monthly_summary)
    assert len({row.key for row in result.cohort_audit}) == len(result.cohort_audit)
    assert any(row["material_node"] == "FG_Central" for row in detail_rows)
    assert any(row["material_node"] == "SubLayer2_FG" for row in detail_rows)
    assert "required_administrable_demand_units" in detail_rows[0]
    assert "policy_excluded_channel_build_units" in detail_rows[0]
    assert "inventory_policy_gap_units" in detail_rows[0]
    assert "cover_demand_units" in detail_rows[0]
    assert "effective_cover_demand_units" in detail_rows[0]
    assert "unmatched_fg_units" in summary_rows[0]
    assert "cohort_id" in cohort_rows[0]


def test_phase5_policy_drawdown_gap_is_audited_without_triggering_false_stockout(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase5_scenario(
        tmp_path,
        scenario_name="POLICY_GAP",
        phase3_rows=[
            "POLICY_GAP,US,AML,1L_fit,1,2029-01-01,100,0,0,0,0,0,60,0,0,60,0.6,false,0,60,1,0,0,0,false,{},fixture",
        ],
        phase4_detail_rows=[
            "POLICY_GAP,FG,AML,US,FG-AML-US-2029-001,1,2029-01-01,1,2029-01-01,1,2029-01-01,1,2029-01-01,60,units,60,1,999,batches_per_year,false,false,false,false,true,fixture",
            "POLICY_GAP,SS,AML,US,SS-AML-US-2029-001,1,2029-01-01,1,2029-01-01,1,2029-01-01,1,2029-01-01,60,units,60,1,999,batches_per_year,false,false,false,false,true,fixture",
        ],
        phase4_summary_rows=[
            "POLICY_GAP,US,AML,1,2029-01-01,100,60,100,0,0,60,61.2244897959184,71.4285714285714,0.0714285714285714,7.14285714285714e-05,60,60,60,0,false,false,false,false,true,false,fixture",
        ],
    )

    result = run_phase5_scenario(scenario_path)
    fg_row = next(
        row for row in result.inventory_detail if row.material_node == "FG_Central" and row.month_index == 1
    )
    sublayer1_row = next(
        row for row in result.inventory_detail if row.material_node == "SubLayer1_FG" and row.month_index == 1
    )
    sublayer2_row = next(
        row for row in result.inventory_detail if row.material_node == "SubLayer2_FG" and row.month_index == 1
    )

    assert not result.validation.has_errors
    assert fg_row.required_administrable_demand_units == pytest.approx(0.0)
    assert fg_row.inventory_policy_gap_units == pytest.approx(40.0)
    assert fg_row.effective_cover_demand_units == pytest.approx(40.0)
    assert fg_row.stockout_flag is False
    assert sublayer1_row.required_administrable_demand_units == pytest.approx(0.0)
    assert sublayer1_row.inventory_policy_gap_units == pytest.approx(40.0)
    assert sublayer1_row.stockout_flag is False
    assert sublayer2_row.required_administrable_demand_units == pytest.approx(60.0)
    assert sublayer2_row.inventory_policy_gap_units == pytest.approx(40.0)
    assert sublayer2_row.stockout_flag is False


def test_phase5_recalibrated_policy_defaults_reduce_excess_and_expiry_in_regression_fixture(
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    old_policy_path = write_phase5_scenario(
        tmp_path / "old_policy",
        scenario_name="POLICY_OLD",
        phase3_trade_layer_path=repo_root / "data" / "outputs" / "base_phase3_trade_layer.csv",
        phase4_schedule_detail_path=repo_root / "data" / "outputs" / "base_phase4_schedule_detail.csv",
        phase4_monthly_summary_path=repo_root / "data" / "outputs" / "base_phase4_monthly_summary.csv",
        ds_shelf_life_months=24,
        dp_shelf_life_months=24,
        fg_shelf_life_months=24,
        ss_shelf_life_months=24,
        excess_inventory_threshold_months_of_cover=6.0,
    )
    new_policy_path = write_phase5_scenario(
        tmp_path / "new_policy",
        scenario_name="POLICY_NEW",
        phase3_trade_layer_path=repo_root / "data" / "outputs" / "base_phase3_trade_layer.csv",
        phase4_schedule_detail_path=repo_root / "data" / "outputs" / "base_phase4_schedule_detail.csv",
        phase4_monthly_summary_path=repo_root / "data" / "outputs" / "base_phase4_monthly_summary.csv",
    )

    old_result = run_phase5_scenario(old_policy_path)
    new_result = run_phase5_scenario(new_policy_path)

    old_excess = sum(1 for row in old_result.monthly_summary if row.excess_inventory_flag)
    old_expiry = sum(1 for row in old_result.monthly_summary if row.expiry_flag)
    new_excess = sum(1 for row in new_result.monthly_summary if row.excess_inventory_flag)
    new_expiry = sum(1 for row in new_result.monthly_summary if row.expiry_flag)

    assert new_excess <= old_excess
    assert new_expiry <= old_expiry
