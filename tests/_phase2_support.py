from __future__ import annotations

from pathlib import Path
import csv

APPROVED_MODULE_SETTINGS = {
    "AML": {
        "fixed_dose_mg": 0.15,
        "weight_based_dose_mg_per_kg": 0.0023,
        "average_patient_weight_kg": 80.0,
        "patient_weight_distribution": "PLACEHOLDER_DETERMINISTIC_AVERAGE_ONLY",
        "doses_per_patient_per_month": 4.33,
        "fg_vialing_rule": "ceil_mg_per_unit_no_sharing",
        "fg_mg_per_unit": 1.0,
    },
    "MDS": {
        "fixed_dose_mg": 0.15,
        "weight_based_dose_mg_per_kg": 0.0023,
        "average_patient_weight_kg": 80.0,
        "patient_weight_distribution": "PLACEHOLDER_DETERMINISTIC_AVERAGE_ONLY",
        "doses_per_patient_per_month": 4.33,
        "fg_vialing_rule": "ceil_mg_per_unit_no_sharing",
        "fg_mg_per_unit": 1.0,
    },
    "CML_Incident": {
        "fixed_dose_mg": 0.15,
        "weight_based_dose_mg_per_kg": 0.0023,
        "average_patient_weight_kg": 80.0,
        "patient_weight_distribution": "PLACEHOLDER_DETERMINISTIC_AVERAGE_ONLY",
        "doses_per_patient_per_month": 1.0,
        "fg_vialing_rule": "ceil_mg_per_unit_no_sharing",
        "fg_mg_per_unit": 1.0,
    },
    "CML_Prevalent": {
        "fixed_dose_mg": 0.15,
        "weight_based_dose_mg_per_kg": 0.0023,
        "average_patient_weight_kg": 80.0,
        "patient_weight_distribution": "PLACEHOLDER_DETERMINISTIC_AVERAGE_ONLY",
        "doses_per_patient_per_month": 1.0,
        "fg_vialing_rule": "ceil_mg_per_unit_no_sharing",
        "fg_mg_per_unit": 1.0,
    },
}


def write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_phase2_scenario(
    tmp_path: Path,
    *,
    monthlyized_rows: list[str] | None = None,
    phase1_monthlyized_output_path: Path | None = None,
    scenario_name: str = "PHASE2_BASE",
    dose_basis: str = "fixed",
    step_up_enabled: bool = False,
    step_up_schedule_id: str = "PLACEHOLDER_INACTIVE",
    dose_reduction_enabled: bool = False,
    dose_reduction_pct: float = 0.0,
    adherence_rate: float = 1.0,
    free_goods_pct: float = 0.0,
    ds_to_dp_yield: float = 0.95,
    dp_to_fg_yield: float = 0.98,
    fg_pack_yield: float = 1.0,
    ss_yield: float = 1.0,
    ss_ratio_to_fg: float = 1.0,
    module_settings_overrides: dict[str, dict[str, float | str]] | None = None,
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

    module_settings = _build_module_settings(module_settings_overrides)
    parameter_lines = [
        "[model]",
        "phase = 2",
        'build_scope = "deterministic_dose_unit_cascade"',
        'upstream_demand_contract = "monthlyized_output.csv"',
        f'dose_basis = "{dose_basis}"',
        'co_pack_mode = "separate_sku_first"',
        "",
        "[modules]",
        'enabled = ["AML", "MDS", "CML_Incident", "CML_Prevalent"]',
        'disabled = ["trade", "production", "inventory", "financials", "monte_carlo"]',
        "",
    ]
    for module_name, module_values in module_settings.items():
        parameter_lines.extend(
            [
                f"[module_settings.{module_name}]",
                f"fixed_dose_mg = {module_values['fixed_dose_mg']}",
                f"weight_based_dose_mg_per_kg = {module_values['weight_based_dose_mg_per_kg']}",
                f"average_patient_weight_kg = {module_values['average_patient_weight_kg']}",
                f'patient_weight_distribution = "{module_values["patient_weight_distribution"]}"',
                f"doses_per_patient_per_month = {module_values['doses_per_patient_per_month']}",
                f'fg_vialing_rule = "{module_values["fg_vialing_rule"]}"',
                f"fg_mg_per_unit = {module_values['fg_mg_per_unit']}",
                "",
            ]
        )

    parameter_lines.extend(
        [
            "[step_up]",
            f"enabled = {'true' if step_up_enabled else 'false'}",
            f'schedule_id = "{step_up_schedule_id}"',
            "",
            "[dose_reduction]",
            f"enabled = {'true' if dose_reduction_enabled else 'false'}",
            f"pct = {dose_reduction_pct}",
            "",
            "[commercial_adjustments]",
            f"adherence_rate = {adherence_rate}",
            f"free_goods_pct = {free_goods_pct}",
            "",
            "[yield.plan]",
            f"ds_to_dp = {ds_to_dp_yield}",
            f"dp_to_fg = {dp_to_fg_yield}",
            f"fg_pack = {fg_pack_yield}",
            f"ss = {ss_yield}",
            "",
            "[ss]",
            f"ratio_to_fg = {ss_ratio_to_fg}",
            "",
            "[validation]",
            "enforce_unique_output_keys = true",
        ]
    )
    write_lines(parameters_dir / "phase2.toml", parameter_lines)

    write_lines(
        scenarios_dir / "scenario.toml",
        [
            f'scenario_name = "{scenario_name}"',
            'parameter_config = "../parameters/phase2.toml"',
            "",
            "[inputs]",
            f'phase1_monthlyized_output = "{_resolve_phase1_input_path(phase1_monthlyized_output_path)}"',
            "",
            "[outputs]",
            'deterministic_cascade = "../../outputs/phase2_deterministic_cascade.csv"',
        ],
    )

    if phase1_monthlyized_output_path is None:
        write_lines(
            data_dir / "monthlyized_output.csv",
            [
                "scenario_name,geography_code,module,segment_code,month_index,calendar_month,patients_treated_monthly,source_frequency,source_grain,source_sheet,profile_id_used,notes",
                *(monthlyized_rows or []),
            ],
        )
    return scenarios_dir / "scenario.toml"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _resolve_phase1_input_path(phase1_monthlyized_output_path: Path | None) -> str:
    if phase1_monthlyized_output_path is None:
        return "../../data/monthlyized_output.csv"
    return phase1_monthlyized_output_path.as_posix()


def _build_module_settings(
    overrides: dict[str, dict[str, float | str]] | None,
) -> dict[str, dict[str, float | str]]:
    module_settings = {
        module: values.copy() for module, values in APPROVED_MODULE_SETTINGS.items()
    }
    if overrides is None:
        return module_settings
    for module, module_overrides in overrides.items():
        module_settings[module].update(module_overrides)
    return module_settings
