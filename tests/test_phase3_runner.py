from __future__ import annotations

from pathlib import Path

import pytest

from cbx250_model.phase3.config_schema import load_phase3_config
from cbx250_model.phase3.runner import run_phase3_scenario
from cbx250_model.phase3.writer import write_phase3_outputs

from _phase3_support import read_csv_rows, write_phase3_scenario


def test_phase3_checked_in_base_config_uses_expected_trade_defaults() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config = load_phase3_config(repo_root / "config" / "scenarios" / "base_phase3.toml")

    assert config.model.build_scope == "deterministic_trade_layer"
    assert config.model.upstream_demand_contract == "phase2_deterministic_cascade.csv"
    assert config.trade.sublayer1_target_weeks_on_hand == pytest.approx(2.5)
    assert config.trade.sublayer2_target_weeks_on_hand == pytest.approx(1.5)
    assert config.trade.initial_stocking_units_per_new_site == pytest.approx(6.0)
    assert config.trade.ss_units_per_new_site == pytest.approx(6.0)
    assert config.trade.bullwhip_flag_threshold == pytest.approx(0.25)
    assert config.get_launch_event("AML", "US").launch_month_index == 1


def test_phase3_two_sublayer_trade_math_uses_site_stocking_and_targets(tmp_path: Path) -> None:
    scenario_path = write_phase3_scenario(
        tmp_path,
        scenario_name="TRADE_BASE",
        phase2_rows=[
            "TRADE_BASE,US,AML,1L_fit,1,2029-01-01,10,fixture",
        ],
        sublayer1_target_weeks_on_hand=4.0,
        sublayer2_target_weeks_on_hand=4.0,
        sublayer1_launch_fill_months_of_demand=0.0,
        channel_fill_start_prelaunch_weeks=0.0,
        sublayer2_fill_distribution_weeks=4.0,
        weeks_per_month=4.0,
        geography_defaults_overrides={
            "US": {
                "site_activation_rate": 1.0,
                "certified_sites_at_launch": 1.0,
                "certified_sites_at_peak": 1.0,
            }
        },
    )

    result = run_phase3_scenario(scenario_path)
    row = result.outputs[0]

    assert not result.validation.has_errors
    assert row.patient_fg_demand_units == pytest.approx(10.0)
    assert row.active_certified_sites == pytest.approx(1.0)
    assert row.new_certified_sites == pytest.approx(1.0)
    assert row.new_site_stocking_orders_units == pytest.approx(6.0)
    assert row.ss_site_stocking_units == pytest.approx(6.0)
    assert row.sublayer2_inventory_target_units == pytest.approx(10.0)
    assert row.sublayer2_inventory_adjustment_units == pytest.approx(20.0)
    assert row.sublayer2_pull_units == pytest.approx(26.0)
    assert row.sublayer1_inventory_target_units == pytest.approx(26.0)
    assert row.sublayer1_inventory_adjustment_units == pytest.approx(52.0)
    assert row.ex_factory_fg_demand_units == pytest.approx(52.0)
    assert row.bullwhip_amplification_factor == pytest.approx(5.2)
    assert row.bullwhip_flag is True


def test_phase3_cml_prevalent_drawdown_can_run_below_patient_demand_due_to_channel_inventory(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase3_scenario(
        tmp_path,
        scenario_name="CML_DRAW",
        phase2_rows=[
            "CML_DRAW,US,CML_Prevalent,CML_Prevalent,1,2029-01-01,18,fixture",
            "CML_DRAW,US,CML_Prevalent,CML_Prevalent,2,2029-02-01,6,fixture",
        ],
        sublayer1_target_weeks_on_hand=4.0,
        sublayer2_target_weeks_on_hand=4.0,
        sublayer1_launch_fill_months_of_demand=1.0,
        channel_fill_start_prelaunch_weeks=0.0,
        sublayer2_fill_distribution_weeks=4.0,
        weeks_per_month=4.0,
    )

    result = run_phase3_scenario(scenario_path)
    month1, month2 = result.outputs

    assert not result.validation.has_errors
    assert month1.module == "CML_Prevalent"
    assert month1.ex_factory_fg_demand_units > month1.patient_fg_demand_units
    assert month2.module == "CML_Prevalent"
    assert month2.patient_fg_demand_units == pytest.approx(6.0)
    assert month2.sublayer2_pull_units == pytest.approx(0.0)
    assert month2.ex_factory_fg_demand_units == pytest.approx(0.0)
    assert month2.ex_factory_fg_demand_units < month2.patient_fg_demand_units


def test_phase3_january_softening_hook_reduces_ongoing_ex_factory_replenishment(
    tmp_path: Path,
) -> None:
    base_scenario = write_phase3_scenario(
        tmp_path / "base",
        scenario_name="JAN_SOFT",
        phase2_rows=[
            "JAN_SOFT,US,AML,1L_fit,1,2030-01-01,10,fixture",
        ],
        sublayer1_target_weeks_on_hand=4.0,
        sublayer2_target_weeks_on_hand=4.0,
        sublayer1_launch_fill_months_of_demand=0.0,
        channel_fill_start_prelaunch_weeks=0.0,
        sublayer2_fill_distribution_weeks=4.0,
        weeks_per_month=4.0,
    )
    softened_scenario = write_phase3_scenario(
        tmp_path / "softened",
        scenario_name="JAN_SOFT",
        phase2_rows=[
            "JAN_SOFT,US,AML,1L_fit,1,2030-01-01,10,fixture",
        ],
        sublayer1_target_weeks_on_hand=4.0,
        sublayer2_target_weeks_on_hand=4.0,
        sublayer1_launch_fill_months_of_demand=0.0,
        channel_fill_start_prelaunch_weeks=0.0,
        sublayer2_fill_distribution_weeks=4.0,
        weeks_per_month=4.0,
        january_softening_enabled=True,
        january_softening_factor=0.5,
    )

    base_row = run_phase3_scenario(base_scenario).outputs[0]
    softened_row = run_phase3_scenario(softened_scenario).outputs[0]

    assert base_row.sublayer1_inventory_adjustment_units == pytest.approx(52.0)
    assert softened_row.sublayer1_inventory_adjustment_units == pytest.approx(26.0)
    assert softened_row.ex_factory_fg_demand_units == pytest.approx(26.0)
    assert softened_row.january_softening_applied is True


def test_phase3_sequential_launches_add_clean_demand_without_negative_destocking(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase3_scenario(
        tmp_path,
        scenario_name="SEQ_LAUNCH",
        phase2_rows=[
            "SEQ_LAUNCH,US,AML,1L_fit,1,2029-01-01,10,fixture",
            "SEQ_LAUNCH,US,AML,1L_fit,2,2029-02-01,10,fixture",
            "SEQ_LAUNCH,EU,AML,1L_fit,3,2029-03-01,8,fixture",
            "SEQ_LAUNCH,EU,AML,1L_fit,4,2029-04-01,8,fixture",
        ],
        sublayer1_target_weeks_on_hand=4.0,
        sublayer2_target_weeks_on_hand=4.0,
        sublayer1_launch_fill_months_of_demand=0.0,
        channel_fill_start_prelaunch_weeks=0.0,
        sublayer2_fill_distribution_weeks=4.0,
        weeks_per_month=4.0,
        launch_events_overrides={
            ("AML", "US"): 1,
            ("AML", "EU"): 3,
        },
    )

    result = run_phase3_scenario(scenario_path)
    total_ex_factory_by_month: dict[int, float] = {}
    for row in result.outputs:
        total_ex_factory_by_month.setdefault(row.month_index, 0.0)
        total_ex_factory_by_month[row.month_index] += row.ex_factory_fg_demand_units

    assert not result.validation.has_errors
    assert all(row.sublayer1_inventory_adjustment_units >= 0 for row in result.outputs)
    assert all(row.sublayer2_inventory_adjustment_units >= 0 for row in result.outputs)
    assert total_ex_factory_by_month[3] > total_ex_factory_by_month[2]


def test_phase3_output_keys_are_unique_and_writer_emits_machine_readable_csv(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase3_scenario(
        tmp_path,
        scenario_name="UNIQUE_KEYS",
        phase2_rows=[
            "UNIQUE_KEYS,US,AML,1L_fit,1,2029-01-01,6,fixture",
            "UNIQUE_KEYS,US,AML,RR,1,2029-01-01,4,fixture",
        ],
        sublayer1_target_weeks_on_hand=4.0,
        sublayer2_target_weeks_on_hand=4.0,
        sublayer1_launch_fill_months_of_demand=0.0,
        channel_fill_start_prelaunch_weeks=0.0,
        sublayer2_fill_distribution_weeks=4.0,
        weeks_per_month=4.0,
    )

    result = run_phase3_scenario(scenario_path)
    output_path = write_phase3_outputs(tmp_path / "phase3_output.csv", result.outputs)
    rows = read_csv_rows(output_path)

    assert not result.validation.has_errors
    assert len({row.key for row in result.outputs}) == len(result.outputs)
    assert len(rows) == 2
    assert rows[0]["trade_parameters_used"].startswith("{")
    assert sum(float(row["patient_fg_demand_units"]) for row in rows) == pytest.approx(10.0)
    assert sum(float(row["ex_factory_fg_demand_units"]) for row in rows) == pytest.approx(52.0)
    assert rows[0]["bullwhip_flag"] in {"true", "false"}
