from __future__ import annotations

from pathlib import Path

from cbx250_model.phase6.runner import run_phase6_scenario
from cbx250_model.phase6.writer import (
    write_phase6_annual_summary,
    write_phase6_financial_detail,
    write_phase6_monthly_summary,
)

from _phase6_support import read_csv_rows, write_phase6_scenario


def test_phase6_acceptance_runs_from_authoritative_phase4_and_phase5_outputs(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    scenario_path = write_phase6_scenario(
        tmp_path,
        scenario_name="BASE_2029",
        phase4_monthly_summary_path=repo_root / "data" / "outputs" / "base_phase4_monthly_summary.csv",
        phase5_inventory_detail_path=repo_root / "data" / "outputs" / "base_phase5_inventory_detail.csv",
        phase5_monthly_inventory_summary_path=repo_root / "data" / "outputs" / "base_phase5_monthly_inventory_summary.csv",
    )

    result = run_phase6_scenario(scenario_path)
    detail_output_path = write_phase6_financial_detail(tmp_path / "phase6_financial_detail.csv", result.financial_detail)
    monthly_output_path = write_phase6_monthly_summary(
        tmp_path / "phase6_monthly_financial_summary.csv",
        result.monthly_summary,
    )
    annual_output_path = write_phase6_annual_summary(
        tmp_path / "phase6_annual_financial_summary.csv",
        result.annual_summary,
    )

    detail_rows = read_csv_rows(detail_output_path)
    monthly_rows = read_csv_rows(monthly_output_path)
    annual_rows = read_csv_rows(annual_output_path)

    assert not result.validation.has_errors
    assert detail_output_path.name == "phase6_financial_detail.csv"
    assert monthly_output_path.name == "phase6_monthly_financial_summary.csv"
    assert annual_output_path.name == "phase6_annual_financial_summary.csv"
    assert any(row["financial_node_or_stage"] == "FG_Central" for row in detail_rows)
    assert any(row["financial_node_or_stage"] == "FG_Release" for row in detail_rows)
    assert any(row["financial_node_or_stage"] == "FG_Sub1_to_Sub2_Shipping" for row in detail_rows)
    assert "shipping_cold_chain_cost_value" in detail_rows[0]
    assert any(float(row["total_inventory_value"]) >= 0 for row in monthly_rows)
    assert "total_shipping_cold_chain_cost" in monthly_rows[0]
    assert any(int(row["calendar_year"]) >= 2029 for row in annual_rows)
    assert "total_shipping_cold_chain_cost" in annual_rows[0]
