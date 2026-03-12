from __future__ import annotations

from pathlib import Path
import pytest

from cbx250_model.phase6.config_schema import load_phase6_config
from cbx250_model.phase6.runner import run_phase6_scenario
from cbx250_model.phase6.summary import build_phase6_run_summary
from cbx250_model.phase6.writer import (
    write_phase6_annual_summary,
    write_phase6_financial_detail,
    write_phase6_monthly_summary,
)

from _phase6_support import read_csv_rows, write_phase6_scenario


def _monthly_row(result, geography_code: str, module: str, month_index: int):
    return next(
        row
        for row in result.monthly_summary
        if row.geography_code == geography_code and row.module == module and row.month_index == month_index
    )


def test_phase6_checked_in_base_config_uses_expected_defaults() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config = load_phase6_config(repo_root / "config" / "scenarios" / "base_phase6.toml")

    assert config.model.build_scope == "deterministic_financial_value_layer"
    assert config.model.upstream_value_contract == "phase4_monthly_summary.csv + phase5_inventory_outputs.csv"
    assert config.cost_basis.ds_standard_cost_per_mg == pytest.approx(0.002)
    assert config.cost_basis.dp_conversion_cost_per_unit == pytest.approx(0.50)
    assert config.cost_basis.fg_packaging_labeling_cost_per_unit == pytest.approx(0.25)
    assert config.cost_basis.ss_standard_cost_per_unit == pytest.approx(0.10)
    assert config.carrying_cost.annual_inventory_carry_rate == pytest.approx(0.20)
    assert config.carrying_cost.monthly_inventory_carry_rate == pytest.approx(0.0166666666666667)
    assert config.expiry_writeoff.expired_inventory_writeoff_rate == pytest.approx(1.0)
    assert config.valuation_policy.include_trade_node_fg_value is True
    assert config.shipping_cold_chain.us_fg_sub1_to_sub2_cost_per_unit == pytest.approx(25.0)
    assert config.shipping_cold_chain.eu_fg_sub1_to_sub2_cost_per_unit == pytest.approx(57.5)
    assert config.shipping_cold_chain.us_ss_sub1_to_sub2_cost_per_unit == pytest.approx(25.0)
    assert config.shipping_cold_chain.eu_ss_sub1_to_sub2_cost_per_unit == pytest.approx(57.5)


def test_phase6_standard_cost_inventory_and_release_values_are_correct(tmp_path: Path) -> None:
    scenario_path = write_phase6_scenario(
        tmp_path,
        scenario_name="FINANCE_SIMPLE",
        ds_standard_cost_per_mg=1.0,
        dp_conversion_cost_per_unit=1.0,
        fg_packaging_labeling_cost_per_unit=1.0,
        ss_standard_cost_per_unit=0.5,
        annual_inventory_carry_rate=1.2,
        monthly_inventory_carry_rate=0.1,
        dp_to_fg_yield=1.0,
        ds_to_dp_yield=1.0,
        ds_qty_per_dp_unit_mg=1.0,
        ds_overage_factor=0.0,
        phase4_monthly_summary_rows=[
            "FINANCE_SIMPLE,US,ALL,1,2029-01-01,0,0,0,0,0,4,5,10,0.01,0.00001,4,0,0,0,false,false,false,false,false,false,fixture",
        ],
        phase5_inventory_detail_rows=[
            "FINANCE_SIMPLE,US,ALL,1,2029-01-01,DS,0,0,0,1,5,5,0,0,0,0,5,5,0,0,false,false,true,false,0,0,fixture",
            "FINANCE_SIMPLE,US,ALL,1,2029-01-01,DP,0,0,0,0,4,4,0,0,0,0,4,4,0,0,false,false,false,false,0,0,fixture",
            "FINANCE_SIMPLE,US,ALL,1,2029-01-01,FG_Central,0,0,0,2,3,3,0,0,0,0,3,3,0,0,false,false,true,false,2,1,fixture",
            "FINANCE_SIMPLE,US,ALL,1,2029-01-01,SS_Central,0,0,0,0,2,2,0,0,0,0,2,2,0,0,false,false,false,false,0,0,fixture",
            "FINANCE_SIMPLE,US,ALL,1,2029-01-01,SubLayer1_FG,0,0,0,0,1,1,0,0,0,0,1,1,0,0,false,false,false,false,0,0,fixture",
            "FINANCE_SIMPLE,US,ALL,1,2029-01-01,SubLayer2_FG,0,0,0,0,2,2,0,0,0,0,2,2,0,0,false,false,false,false,0,0,fixture",
        ],
        phase5_monthly_inventory_summary_rows=[
            "FINANCE_SIMPLE,US,ALL,1,2029-01-01,5,4,3,2,1,2,1,0,2,0,1,2,false,false,true,false,fixture",
        ],
    )

    result = run_phase6_scenario(scenario_path)
    row = _monthly_row(result, "US", "ALL", 1)

    assert not result.validation.has_errors
    assert row.ds_inventory_value == pytest.approx(5.0)
    assert row.dp_inventory_value == pytest.approx(8.0)
    assert row.fg_inventory_value == pytest.approx(9.0)
    assert row.ss_inventory_value == pytest.approx(1.0)
    assert row.sublayer1_fg_inventory_value == pytest.approx(3.0)
    assert row.sublayer2_fg_inventory_value == pytest.approx(6.0)
    assert row.total_inventory_value == pytest.approx(32.0)
    assert row.ds_release_value == pytest.approx(10.0)
    assert row.dp_release_value == pytest.approx(10.0)
    assert row.fg_release_value == pytest.approx(12.0)
    assert row.ss_release_value == pytest.approx(2.0)
    assert row.total_release_value == pytest.approx(14.0)
    assert row.matched_administrable_fg_value == pytest.approx(6.0)
    assert row.unmatched_fg_value_at_risk == pytest.approx(3.0)
    assert row.carrying_cost_total == pytest.approx(3.2)


def test_phase6_expired_inventory_uses_writeoff_policy_and_carrying_cost(tmp_path: Path) -> None:
    scenario_path = write_phase6_scenario(
        tmp_path,
        scenario_name="WRITE_OFF",
        ds_standard_cost_per_mg=2.0,
        monthly_inventory_carry_rate=0.05,
        expired_inventory_writeoff_rate=1.0,
        expired_inventory_salvage_rate=0.25,
        phase5_inventory_detail_rows=[
            "WRITE_OFF,ALL,ALL,1,2029-01-01,DS,0,0,0,3,7,7,0,0,0,0,7,7,0,0,false,false,true,false,0,0,fixture",
        ],
        phase5_monthly_inventory_summary_rows=[
            "WRITE_OFF,ALL,ALL,1,2029-01-01,7,0,0,0,0,0,3,0,0,0,0,0,false,false,true,false,fixture",
        ],
    )

    result = run_phase6_scenario(scenario_path)
    detail_row = next(row for row in result.financial_detail if row.financial_node_or_stage == "DS")
    summary_row = _monthly_row(result, "ALL", "ALL", 1)

    assert not result.validation.has_errors
    assert detail_row.expired_value == pytest.approx(4.5)
    assert detail_row.carrying_cost_value == pytest.approx(0.7)
    assert summary_row.expired_ds_value == pytest.approx(4.5)
    assert summary_row.expired_value_total == pytest.approx(4.5)


def test_phase6_trade_node_value_can_be_excluded_without_breaking_other_values(tmp_path: Path) -> None:
    scenario_path = write_phase6_scenario(
        tmp_path,
        scenario_name="NO_TRADE_VALUE",
        ds_standard_cost_per_mg=1.0,
        dp_conversion_cost_per_unit=0.0,
        fg_packaging_labeling_cost_per_unit=0.0,
        ss_standard_cost_per_unit=0.0,
        monthly_inventory_carry_rate=0.1,
        include_trade_node_fg_value=False,
        dp_to_fg_yield=1.0,
        ds_to_dp_yield=1.0,
        ds_qty_per_dp_unit_mg=1.0,
        ds_overage_factor=0.0,
        phase5_inventory_detail_rows=[
            "NO_TRADE_VALUE,US,ALL,1,2029-01-01,FG_Central,0,0,0,0,5,5,0,0,0,0,5,5,0,0,false,false,false,false,5,0,fixture",
            "NO_TRADE_VALUE,US,ALL,1,2029-01-01,SubLayer1_FG,0,0,0,0,4,4,0,0,0,0,4,4,0,0,false,false,false,false,0,0,fixture",
            "NO_TRADE_VALUE,US,ALL,1,2029-01-01,SubLayer2_FG,0,0,0,0,3,3,0,0,0,0,3,3,0,0,false,false,false,false,0,0,fixture",
        ],
        phase5_monthly_inventory_summary_rows=[
            "NO_TRADE_VALUE,US,ALL,1,2029-01-01,0,0,5,0,4,3,0,0,0,0,0,5,false,false,false,false,fixture",
        ],
    )

    result = run_phase6_scenario(scenario_path)
    row = _monthly_row(result, "US", "ALL", 1)

    assert not result.validation.has_errors
    assert row.fg_inventory_value == pytest.approx(5.0)
    assert row.sublayer1_fg_inventory_value == pytest.approx(0.0)
    assert row.sublayer2_fg_inventory_value == pytest.approx(0.0)
    assert row.total_inventory_value == pytest.approx(5.0)


def test_phase6_shipping_cold_chain_cost_uses_actual_sublayer1_to_sublayer2_shipments_by_region(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase6_scenario(
        tmp_path,
        scenario_name="SHIP_COST",
        dp_conversion_cost_per_unit=0.0,
        fg_packaging_labeling_cost_per_unit=2.0,
        us_fg_sub1_to_sub2_cost_per_unit=20.0,
        eu_fg_sub1_to_sub2_cost_per_unit=50.0,
        us_ss_sub1_to_sub2_cost_per_unit=20.0,
        eu_ss_sub1_to_sub2_cost_per_unit=50.0,
        ss_ratio_to_fg=1.0,
        dp_to_fg_yield=1.0,
        ds_to_dp_yield=1.0,
        ds_qty_per_dp_unit_mg=1.0,
        ds_overage_factor=0.0,
        phase5_inventory_detail_rows=[
            "SHIP_COST,US,ALL,1,2029-01-01,SubLayer1_FG,0,0,10,0,0,0,0,0,0,0,0,0,0,0,false,false,false,false,0,0,fixture",
            "SHIP_COST,EU,ALL,1,2029-01-01,SubLayer1_FG,0,0,8,0,0,0,0,0,0,0,0,0,0,0,false,false,false,false,0,0,fixture",
        ],
        phase5_monthly_inventory_summary_rows=[
            "SHIP_COST,US,ALL,1,2029-01-01,0,0,0,0,0,0,0,0,0,0,0,0,false,false,false,false,fixture",
            "SHIP_COST,EU,ALL,1,2029-01-01,0,0,0,0,0,0,0,0,0,0,0,0,false,false,false,false,fixture",
        ],
    )

    result = run_phase6_scenario(scenario_path)
    us_row = _monthly_row(result, "US", "ALL", 1)
    eu_row = _monthly_row(result, "EU", "ALL", 1)
    us_fg_shipping = next(
        row
        for row in result.financial_detail
        if row.geography_code == "US" and row.financial_node_or_stage == "FG_Sub1_to_Sub2_Shipping"
    )
    eu_fg_shipping = next(
        row
        for row in result.financial_detail
        if row.geography_code == "EU" and row.financial_node_or_stage == "FG_Sub1_to_Sub2_Shipping"
    )
    us_ss_shipping = next(
        row
        for row in result.financial_detail
        if row.geography_code == "US" and row.financial_node_or_stage == "SS_Sub1_to_Sub2_Shipping"
    )

    assert not result.validation.has_errors
    assert us_fg_shipping.shipment_quantity_basis_units == pytest.approx(10.0)
    assert us_fg_shipping.shipping_cold_chain_cost_rate == pytest.approx(20.0)
    assert us_fg_shipping.shipping_cold_chain_cost_value == pytest.approx(200.0)
    assert eu_fg_shipping.shipment_quantity_basis_units == pytest.approx(8.0)
    assert eu_fg_shipping.shipping_cold_chain_cost_rate == pytest.approx(50.0)
    assert eu_fg_shipping.shipping_cold_chain_cost_value == pytest.approx(400.0)
    assert us_ss_shipping.shipping_cold_chain_cost_value == pytest.approx(200.0)
    assert us_row.fg_shipping_cold_chain_cost == pytest.approx(200.0)
    assert us_row.ss_shipping_cold_chain_cost == pytest.approx(200.0)
    assert us_row.total_shipping_cold_chain_cost == pytest.approx(400.0)
    assert eu_row.total_shipping_cold_chain_cost == pytest.approx(800.0)


def test_phase6_no_sublayer1_shipment_means_zero_shipping_cost_and_no_packaging_double_count(
    tmp_path: Path,
) -> None:
    scenario_path = write_phase6_scenario(
        tmp_path,
        scenario_name="NO_SHIP",
        dp_conversion_cost_per_unit=0.0,
        fg_packaging_labeling_cost_per_unit=3.0,
        us_fg_sub1_to_sub2_cost_per_unit=25.0,
        us_ss_sub1_to_sub2_cost_per_unit=25.0,
        dp_to_fg_yield=1.0,
        ds_to_dp_yield=1.0,
        ds_qty_per_dp_unit_mg=1.0,
        ds_overage_factor=0.0,
        phase4_monthly_summary_rows=[
            "NO_SHIP,US,ALL,1,2029-01-01,0,0,0,0,0,5,0,0,0,0,0,0,0,0,false,false,false,false,false,false,fixture",
        ],
        phase5_inventory_detail_rows=[
            "NO_SHIP,US,ALL,1,2029-01-01,SubLayer1_FG,0,0,0,0,0,0,0,0,0,0,0,0,0,0,false,false,false,false,0,0,fixture",
        ],
        phase5_monthly_inventory_summary_rows=[
            "NO_SHIP,US,ALL,1,2029-01-01,0,0,0,0,0,0,0,0,0,0,0,0,false,false,false,false,fixture",
        ],
    )

    result = run_phase6_scenario(scenario_path)
    row = _monthly_row(result, "US", "ALL", 1)
    fg_release_row = next(
        detail
        for detail in result.financial_detail
        if detail.geography_code == "US" and detail.financial_node_or_stage == "FG_Release"
    )
    fg_shipping_row = next(
        detail
        for detail in result.financial_detail
        if detail.geography_code == "US" and detail.financial_node_or_stage == "FG_Sub1_to_Sub2_Shipping"
    )

    assert not result.validation.has_errors
    assert fg_release_row.release_value == pytest.approx(15.01)
    assert fg_shipping_row.shipping_cold_chain_cost_value == pytest.approx(0.0)
    assert row.total_shipping_cold_chain_cost == pytest.approx(0.0)


def test_phase6_output_keys_are_unique_and_writers_emit_machine_readable_csv(tmp_path: Path) -> None:
    scenario_path = write_phase6_scenario(
        tmp_path,
        scenario_name="UNIQUE",
        phase4_monthly_summary_rows=[
            "UNIQUE,US,ALL,1,2029-01-01,0,0,0,0,0,2,3,4,0.004,0.000004,2,0,0,0,false,false,false,false,false,false,fixture",
        ],
        phase5_inventory_detail_rows=[
            "UNIQUE,US,ALL,1,2029-01-01,FG_Central,0,0,0,0,2,2,0,0,0,0,2,2,0,0,false,false,false,false,2,0,fixture",
        ],
        phase5_monthly_inventory_summary_rows=[
            "UNIQUE,US,ALL,1,2029-01-01,0,0,2,0,0,0,0,0,0,0,0,2,false,false,false,false,fixture",
        ],
    )

    result = run_phase6_scenario(scenario_path)
    detail_path = write_phase6_financial_detail(tmp_path / "detail.csv", result.financial_detail)
    monthly_path = write_phase6_monthly_summary(tmp_path / "monthly.csv", result.monthly_summary)
    annual_path = write_phase6_annual_summary(tmp_path / "annual.csv", result.annual_summary)

    detail_rows = read_csv_rows(detail_path)
    monthly_rows = read_csv_rows(monthly_path)
    annual_rows = read_csv_rows(annual_path)

    assert not result.validation.has_errors
    assert len({row.key for row in result.financial_detail}) == len(result.financial_detail)
    assert len({row.key for row in result.monthly_summary}) == len(result.monthly_summary)
    assert len({row.key for row in result.annual_summary}) == len(result.annual_summary)
    assert "financial_node_or_stage" in detail_rows[0]
    assert "shipping_cold_chain_cost_value" in detail_rows[0]
    assert "total_inventory_value" in monthly_rows[0]
    assert "total_shipping_cold_chain_cost" in monthly_rows[0]
    assert "calendar_year" in annual_rows[0]


def test_phase6_carrying_cost_interpretation_note_is_explicit_for_cumulative_metric(tmp_path: Path) -> None:
    scenario_path = write_phase6_scenario(
        tmp_path,
        scenario_name="CARRY_NOTE",
        monthly_inventory_carry_rate=0.6,
        phase5_inventory_detail_rows=[
            "CARRY_NOTE,US,ALL,1,2029-01-01,FG_Central,0,0,0,0,10,10,0,0,0,0,10,10,0,0,false,false,false,false,0,0,fixture",
            "CARRY_NOTE,US,ALL,2,2029-02-01,FG_Central,0,0,0,0,10,10,0,0,0,0,10,10,0,0,false,false,false,false,0,0,fixture",
        ],
        phase5_monthly_inventory_summary_rows=[
            "CARRY_NOTE,US,ALL,1,2029-01-01,0,0,10,0,0,0,0,0,0,0,0,0,false,false,false,false,fixture",
            "CARRY_NOTE,US,ALL,2,2029-02-01,0,0,10,0,0,0,0,0,0,0,0,0,false,false,false,false,fixture",
        ],
    )

    result = run_phase6_scenario(scenario_path)
    summary = build_phase6_run_summary(result)

    assert summary["carrying_cost_exceeds_ending_inventory_value"] is True
    assert "cumulative across monthly periods" in summary["carrying_cost_interpretation_note"]
