from __future__ import annotations

import csv
from pathlib import Path


def write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_phase6_scenario(
    tmp_path: Path,
    *,
    phase4_monthly_summary_path: Path | None = None,
    phase5_inventory_detail_path: Path | None = None,
    phase5_monthly_inventory_summary_path: Path | None = None,
    phase4_monthly_summary_rows: list[str] | None = None,
    phase5_inventory_detail_rows: list[str] | None = None,
    phase5_monthly_inventory_summary_rows: list[str] | None = None,
    scenario_name: str = "PHASE6_BASE",
    ds_standard_cost_per_mg: float = 0.002,
    dp_conversion_cost_per_unit: float = 0.50,
    fg_packaging_labeling_cost_per_unit: float = 0.25,
    ss_standard_cost_per_unit: float = 0.10,
    annual_inventory_carry_rate: float = 0.20,
    monthly_inventory_carry_rate: float = 0.0166666666666667,
    expired_inventory_writeoff_rate: float = 1.0,
    expired_inventory_salvage_rate: float = 0.0,
    value_unmatched_fg_at_fg_standard_cost: bool = True,
    include_trade_node_fg_value: bool = True,
    use_matched_administrable_fg_value: bool = True,
    dp_to_fg_yield: float = 0.98,
    ds_to_dp_yield: float = 0.90,
    ds_qty_per_dp_unit_mg: float = 1.0,
    ds_overage_factor: float = 0.05,
    ss_ratio_to_fg: float = 1.0,
) -> Path:
    config_dir = tmp_path / "config"
    parameters_dir = config_dir / "parameters"
    scenarios_dir = config_dir / "scenarios"
    data_dir = tmp_path / "data"
    outputs_dir = tmp_path / "outputs"
    parameters_dir.mkdir(parents=True, exist_ok=True)
    scenarios_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    write_lines(
        parameters_dir / "phase6.toml",
        [
            "[model]",
            "phase = 6",
            'build_scope = "deterministic_financial_value_layer"',
            'upstream_value_contract = "phase4_monthly_summary.csv + phase5_inventory_outputs.csv"',
            "",
            "[modules]",
            'enabled = ["AML", "MDS", "CML_Incident", "CML_Prevalent"]',
            'disabled = ["revenue_forecast", "monte_carlo", "allocation_optimization", "dashboard_ui"]',
            "",
            "[cost_basis]",
            'ds_standard_cost_basis_unit = "mg"',
            f"ds_standard_cost_per_mg = {ds_standard_cost_per_mg}",
            f"dp_conversion_cost_per_unit = {dp_conversion_cost_per_unit}",
            f"fg_packaging_labeling_cost_per_unit = {fg_packaging_labeling_cost_per_unit}",
            f"ss_standard_cost_per_unit = {ss_standard_cost_per_unit}",
            "",
            "[carrying_cost]",
            f"annual_inventory_carry_rate = {annual_inventory_carry_rate}",
            f"monthly_inventory_carry_rate = {monthly_inventory_carry_rate}",
            "",
            "[expiry_writeoff]",
            f"expired_inventory_writeoff_rate = {expired_inventory_writeoff_rate}",
            f"expired_inventory_salvage_rate = {expired_inventory_salvage_rate}",
            "",
            "[valuation_policy]",
            f"value_unmatched_fg_at_fg_standard_cost = {str(value_unmatched_fg_at_fg_standard_cost).lower()}",
            f"include_trade_node_fg_value = {str(include_trade_node_fg_value).lower()}",
            f"use_matched_administrable_fg_value = {str(use_matched_administrable_fg_value).lower()}",
            "",
            "[conversion]",
            f"dp_to_fg_yield = {dp_to_fg_yield}",
            f"ds_to_dp_yield = {ds_to_dp_yield}",
            f"ds_qty_per_dp_unit_mg = {ds_qty_per_dp_unit_mg}",
            f"ds_overage_factor = {ds_overage_factor}",
            f"ss_ratio_to_fg = {ss_ratio_to_fg}",
            "",
            "[validation]",
            "enforce_unique_output_keys = true",
            "reconciliation_tolerance_value = 0.000001",
        ],
    )

    write_lines(
        scenarios_dir / "scenario.toml",
        [
            f'scenario_name = "{scenario_name}"',
            'parameter_config = "../parameters/phase6.toml"',
            "",
            "[inputs]",
            f'phase4_monthly_summary = "{_resolve_input_path(phase4_monthly_summary_path, "../../data/phase4_monthly_summary.csv")}"',
            f'phase5_inventory_detail = "{_resolve_input_path(phase5_inventory_detail_path, "../../data/phase5_inventory_detail.csv")}"',
            f'phase5_monthly_inventory_summary = "{_resolve_input_path(phase5_monthly_inventory_summary_path, "../../data/phase5_monthly_inventory_summary.csv")}"',
            "",
            "[outputs]",
            'financial_detail = "../../outputs/phase6_financial_detail.csv"',
            'monthly_financial_summary = "../../outputs/phase6_monthly_financial_summary.csv"',
            'annual_financial_summary = "../../outputs/phase6_annual_financial_summary.csv"',
        ],
    )

    if phase4_monthly_summary_path is None:
        write_lines(
            data_dir / "phase4_monthly_summary.csv",
            [
                "scenario_name,geography_code,module,month_index,calendar_month,patient_fg_demand_units,ex_factory_fg_demand_units,underlying_patient_consumption_units,channel_inventory_build_units,launch_fill_component_units,fg_release_units,dp_release_units,ds_release_quantity_mg,ds_release_quantity_g,ds_release_quantity_kg,ss_release_units,cumulative_fg_released,cumulative_ss_released,unmet_demand_units,capacity_flag,supply_gap_flag,excess_build_flag,bullwhip_review_flag,ss_fg_sync_flag,stepdown_applied,notes",
                *(phase4_monthly_summary_rows or []),
            ],
        )
    if phase5_inventory_detail_path is None:
        write_lines(
            data_dir / "phase5_inventory_detail.csv",
            [
                "scenario_name,geography_code,module,month_index,calendar_month,material_node,opening_inventory,receipts,issues,expired_quantity,ending_inventory,available_nonexpired_inventory,demand_signal_units,required_administrable_demand_units,policy_excluded_channel_build_units,inventory_policy_gap_units,cover_demand_units,effective_cover_demand_units,shortfall_units,months_of_cover,stockout_flag,excess_inventory_flag,expiry_flag,fg_ss_mismatch_flag,matched_administrable_fg_units,fg_ss_mismatch_units,notes",
                *(phase5_inventory_detail_rows or []),
            ],
        )
    if phase5_monthly_inventory_summary_path is None:
        write_lines(
            data_dir / "phase5_monthly_inventory_summary.csv",
            [
                "scenario_name,geography_code,module,month_index,calendar_month,ds_inventory_mg,dp_inventory_units,fg_inventory_units,ss_inventory_units,sublayer1_fg_inventory_units,sublayer2_fg_inventory_units,expired_ds_mg,expired_dp_units,expired_fg_units,expired_ss_units,unmatched_fg_units,matched_administrable_fg_units,stockout_flag,excess_inventory_flag,expiry_flag,fg_ss_mismatch_flag,notes",
                *(phase5_monthly_inventory_summary_rows or []),
            ],
        )

    return scenarios_dir / "scenario.toml"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _resolve_input_path(path: Path | None, default_relative_path: str) -> str:
    if path is None:
        return default_relative_path
    return path.as_posix()
