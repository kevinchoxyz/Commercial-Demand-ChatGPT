from __future__ import annotations

from pathlib import Path
import csv


def write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_phase4_scenario(
    tmp_path: Path,
    *,
    phase3_rows: list[str] | None = None,
    phase3_trade_layer_path: Path | None = None,
    scenario_name: str = "PHASE4_BASE",
    bullwhip_amplification_threshold: float = 1.25,
    bullwhip_review_window_months: int = 2,
    excess_build_threshold_ratio: float = 0.25,
    projected_cml_prevalent_bolus_exhaustion_month_index: int = 0,
    cml_prevalent_forward_window_months: int = 6,
    dp_to_fg_yield: float = 0.98,
    ds_to_dp_yield: float = 0.90,
    ds_qty_per_dp_unit_mg: float = 1.0,
    ds_overage_factor: float = 0.05,
    ss_ratio_to_fg: float = 1.0,
    weeks_per_month: float = 4.33,
    fg_packaging_campaign_size_units: float = 50000.0,
    dp_min_batch_size_units: float = 100000.0,
    dp_max_batch_size_units: float = 500000.0,
    dp_min_campaign_batches: int = 3,
    dp_annual_capacity_batches: int = 10,
    ds_min_batch_size_kg: float = 2.0,
    ds_max_batch_size_kg: float = 4.0,
    ds_min_campaign_batches: int = 3,
    ds_annual_capacity_batches: int = 5,
    ss_batch_size_units: float = 100000.0,
    ss_min_campaign_batches: int = 3,
    ss_annual_capacity_batches: int = 10,
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
        parameters_dir / "phase4.toml",
        [
            "[model]",
            "phase = 4",
            'build_scope = "deterministic_production_schedule"',
            'upstream_demand_contract = "phase3_trade_layer.csv"',
            "",
            "[modules]",
            'enabled = ["AML", "MDS", "CML_Incident", "CML_Prevalent"]',
            'disabled = ["inventory", "financials", "monte_carlo"]',
            "",
            "[conversion]",
            f"dp_to_fg_yield = {dp_to_fg_yield}",
            f"ds_to_dp_yield = {ds_to_dp_yield}",
            f"ds_qty_per_dp_unit_mg = {ds_qty_per_dp_unit_mg}",
            f"ds_overage_factor = {ds_overage_factor}",
            f"ss_ratio_to_fg = {ss_ratio_to_fg}",
            f"weeks_per_month = {weeks_per_month}",
            "",
            "[review]",
            f"bullwhip_amplification_threshold = {bullwhip_amplification_threshold}",
            f"bullwhip_review_window_months = {bullwhip_review_window_months}",
            f"excess_build_threshold_ratio = {excess_build_threshold_ratio}",
            "",
            "[stepdown]",
            f"cml_prevalent_forward_window_months = {cml_prevalent_forward_window_months}",
            f"projected_cml_prevalent_bolus_exhaustion_month_index = {projected_cml_prevalent_bolus_exhaustion_month_index}",
            "",
            "[fg]",
            "lead_time_from_dp_release_weeks = 4.0",
            "packaging_cycle_weeks = 2.0",
            "release_qa_weeks = 2.0",
            "total_order_to_release_weeks = 8.0",
            f"packaging_campaign_size_units = {fg_packaging_campaign_size_units}",
            "",
            "[dp]",
            "lead_time_from_ds_release_weeks = 4.0",
            "manufacturing_cycle_weeks = 2.0",
            "release_testing_weeks = 12.0",
            "total_order_to_release_weeks = 18.0",
            f"min_batch_size_units = {dp_min_batch_size_units}",
            f"max_batch_size_units = {dp_max_batch_size_units}",
            f"min_campaign_batches = {dp_min_campaign_batches}",
            f"annual_capacity_batches = {dp_annual_capacity_batches}",
            "",
            "[ds]",
            "lead_time_to_batch_start_planning_horizon_weeks = 24.0",
            "manufacturing_cycle_weeks = 8.0",
            "release_testing_weeks = 12.0",
            "total_order_to_release_weeks = 44.0",
            f"min_batch_size_kg = {ds_min_batch_size_kg}",
            f"max_batch_size_kg = {ds_max_batch_size_kg}",
            f"min_campaign_batches = {ds_min_campaign_batches}",
            f"annual_capacity_batches = {ds_annual_capacity_batches}",
            "",
            "[ss]",
            "order_to_release_lead_time_weeks = 24.0",
            f"batch_size_units = {ss_batch_size_units}",
            f"min_campaign_batches = {ss_min_campaign_batches}",
            f"annual_capacity_batches = {ss_annual_capacity_batches}",
            "release_must_coincide_with_or_precede_fg = true",
            "",
            "[validation]",
            "enforce_unique_output_keys = true",
        ],
    )

    write_lines(
        scenarios_dir / "scenario.toml",
        [
            f'scenario_name = "{scenario_name}"',
            'parameter_config = "../parameters/phase4.toml"',
            "",
            "[inputs]",
            f'phase3_trade_layer = "{_resolve_phase3_input_path(phase3_trade_layer_path)}"',
            "",
            "[outputs]",
            'schedule_detail = "../../outputs/phase4_schedule_detail.csv"',
            'monthly_summary = "../../outputs/phase4_monthly_summary.csv"',
        ],
    )

    if phase3_trade_layer_path is None:
        write_lines(
            data_dir / "phase3_trade_layer.csv",
            [
                "scenario_name,geography_code,module,segment_code,month_index,calendar_month,patient_fg_demand_units,sublayer2_wastage_units,sublayer2_inventory_target_units,sublayer2_inventory_adjustment_units,new_site_stocking_orders_units,ss_site_stocking_units,sublayer2_pull_units,sublayer1_inventory_target_units,sublayer1_inventory_adjustment_units,ex_factory_fg_demand_units,bullwhip_amplification_factor,bullwhip_flag,launch_fill_component_units,ongoing_replenishment_component_units,active_certified_sites,new_certified_sites,sublayer2_inventory_on_hand_end_units,sublayer1_inventory_on_hand_end_units,january_softening_applied,trade_parameters_used,notes",
                *(phase3_rows or []),
            ],
        )
    return scenarios_dir / "scenario.toml"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _resolve_phase3_input_path(phase3_trade_layer_path: Path | None) -> str:
    if phase3_trade_layer_path is None:
        return "../../data/phase3_trade_layer.csv"
    return phase3_trade_layer_path.as_posix()
