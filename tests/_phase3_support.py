from __future__ import annotations

from pathlib import Path
import csv


DEFAULT_GEOGRAPHY_DEFAULTS = {
    "US": {
        "site_activation_rate": 1.0,
        "certified_sites_at_launch": 1.0,
        "certified_sites_at_peak": 1.0,
    },
    "EU": {
        "site_activation_rate": 1.0,
        "certified_sites_at_launch": 1.0,
        "certified_sites_at_peak": 1.0,
    },
}

DEFAULT_LAUNCH_EVENTS = {
    ("AML", "US"): 1,
    ("MDS", "US"): 1,
    ("CML_Incident", "US"): 1,
    ("CML_Prevalent", "US"): 1,
    ("AML", "EU"): 1,
    ("MDS", "EU"): 1,
    ("CML_Incident", "EU"): 1,
    ("CML_Prevalent", "EU"): 1,
}


def write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_phase3_scenario(
    tmp_path: Path,
    *,
    phase2_rows: list[str] | None = None,
    phase2_deterministic_cascade_path: Path | None = None,
    scenario_name: str = "PHASE3_BASE",
    sublayer1_target_weeks_on_hand: float = 2.5,
    sublayer2_target_weeks_on_hand: float = 1.5,
    sublayer2_wastage_rate: float = 0.0,
    initial_stocking_units_per_new_site: float = 6.0,
    ss_units_per_new_site: float = 6.0,
    sublayer1_launch_fill_months_of_demand: float = 1.0,
    rems_certification_lag_weeks: float = 0.0,
    january_softening_enabled: bool = False,
    january_softening_factor: float = 1.0,
    bullwhip_flag_threshold: float = 0.25,
    channel_fill_start_prelaunch_weeks: float = 4.0,
    sublayer2_fill_distribution_weeks: float = 8.0,
    weeks_per_month: float = 4.33,
    geography_defaults_overrides: dict[str, dict[str, float]] | None = None,
    launch_events_overrides: dict[tuple[str, str], int] | None = None,
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

    geography_defaults = {
        geography_code: values.copy()
        for geography_code, values in DEFAULT_GEOGRAPHY_DEFAULTS.items()
    }
    if geography_defaults_overrides is not None:
        for geography_code, override_values in geography_defaults_overrides.items():
            geography_defaults.setdefault(geography_code, {}).update(override_values)

    launch_events = DEFAULT_LAUNCH_EVENTS.copy()
    if launch_events_overrides is not None:
        launch_events.update(launch_events_overrides)

    parameter_lines = [
        "[model]",
        "phase = 3",
        'build_scope = "deterministic_trade_layer"',
        'upstream_demand_contract = "phase2_deterministic_cascade.csv"',
        "",
        "[modules]",
        'enabled = ["AML", "MDS", "CML_Incident", "CML_Prevalent"]',
        'disabled = ["production", "inventory", "financials", "monte_carlo"]',
        "",
        "[trade]",
        f"sublayer1_target_weeks_on_hand = {sublayer1_target_weeks_on_hand}",
        f"sublayer2_target_weeks_on_hand = {sublayer2_target_weeks_on_hand}",
        f"sublayer2_wastage_rate = {sublayer2_wastage_rate}",
        f"initial_stocking_units_per_new_site = {initial_stocking_units_per_new_site}",
        f"ss_units_per_new_site = {ss_units_per_new_site}",
        f"sublayer1_launch_fill_months_of_demand = {sublayer1_launch_fill_months_of_demand}",
        f"rems_certification_lag_weeks = {rems_certification_lag_weeks}",
        f"january_softening_enabled = {'true' if january_softening_enabled else 'false'}",
        f"january_softening_factor = {january_softening_factor}",
        f"bullwhip_flag_threshold = {bullwhip_flag_threshold}",
        f"channel_fill_start_prelaunch_weeks = {channel_fill_start_prelaunch_weeks}",
        f"sublayer2_fill_distribution_weeks = {sublayer2_fill_distribution_weeks}",
        f"weeks_per_month = {weeks_per_month}",
        "",
    ]
    for geography_code, values in geography_defaults.items():
        parameter_lines.extend(
            [
                f"[geography_defaults.{geography_code}]",
                f"site_activation_rate = {values['site_activation_rate']}",
                f"certified_sites_at_launch = {values['certified_sites_at_launch']}",
                f"certified_sites_at_peak = {values['certified_sites_at_peak']}",
                "",
            ]
        )
    for (module, geography_code), launch_month_index in launch_events.items():
        parameter_lines.extend(
            [
                f"[launch_events.{module}.{geography_code}]",
                f"launch_month_index = {launch_month_index}",
                "",
            ]
        )
    parameter_lines.extend(
        [
            "[validation]",
            "enforce_unique_output_keys = true",
        ]
    )
    write_lines(parameters_dir / "phase3.toml", parameter_lines)

    write_lines(
        scenarios_dir / "scenario.toml",
        [
            f'scenario_name = "{scenario_name}"',
            'parameter_config = "../parameters/phase3.toml"',
            "",
            "[inputs]",
            f'phase2_deterministic_cascade = "{_resolve_phase2_input_path(phase2_deterministic_cascade_path)}"',
            "",
            "[outputs]",
            'deterministic_trade_layer = "../../outputs/phase3_trade_layer.csv"',
        ],
    )

    if phase2_deterministic_cascade_path is None:
        write_lines(
            data_dir / "phase2_deterministic_cascade.csv",
            [
                "scenario_name,geography_code,module,segment_code,month_index,calendar_month,fg_units_required,notes",
                *(phase2_rows or []),
            ],
        )
    return scenarios_dir / "scenario.toml"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _resolve_phase2_input_path(phase2_deterministic_cascade_path: Path | None) -> str:
    if phase2_deterministic_cascade_path is None:
        return "../../data/phase2_deterministic_cascade.csv"
    return phase2_deterministic_cascade_path.as_posix()
