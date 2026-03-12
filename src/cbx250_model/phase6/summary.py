"""Summary helpers for deterministic Phase 6 financial analytics."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .runner import Phase6RunResult


def build_phase6_run_summary(
    result: "Phase6RunResult",
    detail_output_path: str | None = None,
    monthly_summary_path: str | None = None,
    annual_summary_path: str | None = None,
) -> dict[str, object]:
    latest_month_index = max((row.month_index for row in result.monthly_summary), default=0)
    ending_inventory_value = sum(
        row.total_inventory_value for row in result.monthly_summary if row.month_index == latest_month_index
    )
    total_carrying_cost = sum(row.carrying_cost_total for row in result.monthly_summary)
    total_shipping_cold_chain_cost = sum(row.total_shipping_cold_chain_cost for row in result.monthly_summary)
    total_fg_shipping_cold_chain_cost = sum(row.fg_shipping_cold_chain_cost for row in result.monthly_summary)
    total_ss_shipping_cold_chain_cost = sum(row.ss_shipping_cold_chain_cost for row in result.monthly_summary)
    total_us_shipping_cold_chain_cost = sum(
        row.total_shipping_cold_chain_cost for row in result.monthly_summary if row.geography_code == "US"
    )
    total_eu_shipping_cold_chain_cost = sum(
        row.total_shipping_cold_chain_cost for row in result.monthly_summary if row.geography_code != "US"
    )
    ending_matched_fg_value = sum(
        row.matched_administrable_fg_value for row in result.monthly_summary if row.month_index == latest_month_index
    )
    carrying_cost_exceeds_ending_inventory_value = total_carrying_cost > ending_inventory_value
    carrying_cost_interpretation_note = (
        "total_carrying_cost is cumulative across monthly periods, while ending_total_inventory_value is a point-in-time ending snapshot; carrying cost can therefore exceed ending inventory value without indicating a bug."
        if carrying_cost_exceeds_ending_inventory_value
        else "total_carrying_cost remains below or equal to the ending_total_inventory_value snapshot in this run; the carrying-cost metric is still cumulative across monthly periods."
    )
    return {
        "scenario_name": result.config.scenario_name,
        "input_phase4_monthly_summary_row_count": len(result.inputs.phase4_monthly_summary),
        "input_phase5_inventory_detail_row_count": len(result.inputs.phase5_inventory_detail),
        "input_phase5_monthly_summary_row_count": len(result.inputs.phase5_monthly_inventory_summary),
        "financial_detail_row_count": len(result.financial_detail),
        "monthly_financial_summary_row_count": len(result.monthly_summary),
        "annual_financial_summary_row_count": len(result.annual_summary),
        "ending_total_inventory_value": ending_inventory_value,
        "total_expired_value": sum(row.expired_value_total for row in result.monthly_summary),
        "total_carrying_cost": total_carrying_cost,
        "total_shipping_cold_chain_cost": total_shipping_cold_chain_cost,
        "total_fg_shipping_cold_chain_cost": total_fg_shipping_cold_chain_cost,
        "total_ss_shipping_cold_chain_cost": total_ss_shipping_cold_chain_cost,
        "total_us_shipping_cold_chain_cost": total_us_shipping_cold_chain_cost,
        "total_eu_shipping_cold_chain_cost": total_eu_shipping_cold_chain_cost,
        "ending_matched_administrable_fg_value": ending_matched_fg_value,
        "carrying_cost_exceeds_ending_inventory_value": carrying_cost_exceeds_ending_inventory_value,
        "carrying_cost_interpretation_note": carrying_cost_interpretation_note,
        "validation_issue_count": len(result.validation.issues),
        "authoritative_financial_detail_file": detail_output_path,
        "authoritative_monthly_financial_summary_file": monthly_summary_path,
        "authoritative_annual_financial_summary_file": annual_summary_path,
    }
