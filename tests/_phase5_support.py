from __future__ import annotations

from pathlib import Path
import csv


def write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_phase5_scenario(
    tmp_path: Path,
    *,
    phase3_rows: list[str] | None = None,
    phase4_detail_rows: list[str] | None = None,
    phase4_summary_rows: list[str] | None = None,
    phase3_trade_layer_path: Path | None = None,
    phase4_schedule_detail_path: Path | None = None,
    phase4_monthly_summary_path: Path | None = None,
    scenario_name: str = "PHASE5_BASE",
    starting_ds_mg: float = 0.0,
    starting_dp_units: float = 0.0,
    starting_fg_units: float = 0.0,
    starting_ss_units: float = 0.0,
    starting_sublayer1_fg_units: float = 0.0,
    starting_sublayer2_fg_units: float = 0.0,
    ds_shelf_life_months: int = 48,
    dp_shelf_life_months: int = 36,
    fg_shelf_life_months: int = 36,
    ss_shelf_life_months: int = 48,
    excess_inventory_threshold_months_of_cover: float = 18.0,
    stockout_tolerance_units: float = 0.000001,
    fefo_enabled: bool = True,
    ss_fg_match_required: bool = True,
    allow_prelaunch_inventory_build: bool = True,
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
        parameters_dir / "phase5.toml",
        [
            "[model]",
            "phase = 5",
            'build_scope = "deterministic_inventory_shelf_life"',
            'upstream_supply_contract = "phase3_trade_layer.csv + phase4_schedule_outputs.csv"',
            "",
            "[modules]",
            'enabled = ["AML", "MDS", "CML_Incident", "CML_Prevalent"]',
            'disabled = ["financials", "monte_carlo", "allocation_optimization"]',
            "",
            "[starting_inventory]",
            f"ds_mg = {starting_ds_mg}",
            f"dp_units = {starting_dp_units}",
            f"fg_units = {starting_fg_units}",
            f"ss_units = {starting_ss_units}",
            f"sublayer1_fg_units = {starting_sublayer1_fg_units}",
            f"sublayer2_fg_units = {starting_sublayer2_fg_units}",
            "",
            "[shelf_life]",
            f"ds_months = {ds_shelf_life_months}",
            f"dp_months = {dp_shelf_life_months}",
            f"fg_months = {fg_shelf_life_months}",
            f"ss_months = {ss_shelf_life_months}",
            "",
            "[policy]",
            f"excess_inventory_threshold_months_of_cover = {excess_inventory_threshold_months_of_cover}",
            f"stockout_tolerance_units = {stockout_tolerance_units}",
            f"fefo_enabled = {str(fefo_enabled).lower()}",
            f"ss_fg_match_required = {str(ss_fg_match_required).lower()}",
            f"allow_prelaunch_inventory_build = {str(allow_prelaunch_inventory_build).lower()}",
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
            "reconcile_phase4_receipts = true",
            "reconciliation_tolerance_units = 0.000001",
        ],
    )

    write_lines(
        scenarios_dir / "scenario.toml",
        [
            f'scenario_name = "{scenario_name}"',
            'parameter_config = "../parameters/phase5.toml"',
            "",
            "[inputs]",
            f'phase3_trade_layer = "{_resolve_input_path(phase3_trade_layer_path, "../../data/phase3_trade_layer.csv")}"',
            f'phase4_schedule_detail = "{_resolve_input_path(phase4_schedule_detail_path, "../../data/phase4_schedule_detail.csv")}"',
            f'phase4_monthly_summary = "{_resolve_input_path(phase4_monthly_summary_path, "../../data/phase4_monthly_summary.csv")}"',
            "",
            "[outputs]",
            'inventory_detail = "../../outputs/phase5_inventory_detail.csv"',
            'monthly_inventory_summary = "../../outputs/phase5_monthly_inventory_summary.csv"',
            'cohort_audit = "../../outputs/phase5_inventory_cohort_audit.csv"',
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
    if phase4_schedule_detail_path is None:
        write_lines(
            data_dir / "phase4_schedule_detail.csv",
            [
                "scenario_name,stage,module,geography_code,batch_number,demand_month_index,demand_calendar_month,month_index,calendar_month,planned_start_month_index,planned_start_month,planned_release_month_index,planned_release_month,batch_quantity,quantity_unit,cumulative_released_quantity,capacity_used,capacity_limit,capacity_metric,capacity_flag,supply_gap_flag,excess_build_flag,bullwhip_review_flag,ss_fg_sync_flag,notes",
                *(phase4_detail_rows or []),
            ],
        )
    if phase4_monthly_summary_path is None:
        write_lines(
            data_dir / "phase4_monthly_summary.csv",
            [
                "scenario_name,geography_code,module,month_index,calendar_month,patient_fg_demand_units,ex_factory_fg_demand_units,underlying_patient_consumption_units,channel_inventory_build_units,launch_fill_component_units,fg_release_units,dp_release_units,ds_release_quantity_mg,ds_release_quantity_g,ds_release_quantity_kg,ss_release_units,cumulative_fg_released,cumulative_ss_released,unmet_demand_units,capacity_flag,supply_gap_flag,excess_build_flag,bullwhip_review_flag,ss_fg_sync_flag,stepdown_applied,notes",
                *(phase4_summary_rows or []),
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
