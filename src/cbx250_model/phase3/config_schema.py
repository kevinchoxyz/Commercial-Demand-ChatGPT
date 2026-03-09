"""Config schema and loader for the deterministic Phase 3 trade layer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib

from ..constants import (
    PHASE1_MODULES,
    PHASE3_BUILD_SCOPE,
    PHASE3_DISABLED_CAPABILITIES,
    PHASE3_UPSTREAM_DEMAND_CONTRACT,
)


def _load_toml(path: Path) -> dict:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def _resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else (base_dir / path).resolve()


def _parse_positive_float(value: object, field_name: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise ValueError(f"{field_name} must be positive, received {parsed}.")
    return parsed


def _parse_nonnegative_float(value: object, field_name: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise ValueError(f"{field_name} must be non-negative, received {parsed}.")
    return parsed


def _parse_probability(value: object, field_name: str) -> float:
    parsed = float(value)
    if parsed < 0 or parsed > 1:
        raise ValueError(f"{field_name} must be between 0 and 1, received {parsed}.")
    return parsed


@dataclass(frozen=True)
class ModelConfig:
    phase: int
    build_scope: str
    upstream_demand_contract: str

    def __post_init__(self) -> None:
        if self.phase != 3:
            raise ValueError(f"Phase 3 trade layer only supports phase=3, received {self.phase}.")
        if self.build_scope != PHASE3_BUILD_SCOPE:
            raise ValueError(
                f"Phase 3 trade layer expects build_scope={PHASE3_BUILD_SCOPE!r}, "
                f"received {self.build_scope!r}."
            )
        if self.upstream_demand_contract != PHASE3_UPSTREAM_DEMAND_CONTRACT:
            raise ValueError(
                "Phase 3 trade layer expects upstream_demand_contract to be "
                f"{PHASE3_UPSTREAM_DEMAND_CONTRACT!r}, received {self.upstream_demand_contract!r}."
            )


@dataclass(frozen=True)
class ModulesConfig:
    enabled: tuple[str, ...]
    disabled: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.enabled != PHASE1_MODULES:
            raise ValueError(
                f"Phase 3 trade layer expects enabled modules {PHASE1_MODULES}, received {self.enabled}."
            )
        missing_disabled = [item for item in PHASE3_DISABLED_CAPABILITIES if item not in self.disabled]
        if missing_disabled:
            raise ValueError(
                "Phase 3 trade layer requires later-phase capabilities to remain disabled. "
                f"Missing disabled items: {missing_disabled}."
            )


@dataclass(frozen=True)
class InputPaths:
    phase2_deterministic_cascade: Path


@dataclass(frozen=True)
class OutputPaths:
    deterministic_trade_layer: Path


@dataclass(frozen=True)
class TradeConfig:
    sublayer1_target_weeks_on_hand: float
    sublayer2_target_weeks_on_hand: float
    sublayer2_wastage_rate: float
    initial_stocking_units_per_new_site: float
    ss_units_per_new_site: float
    sublayer1_launch_fill_months_of_demand: float
    rems_certification_lag_weeks: float
    january_softening_enabled: bool
    january_softening_factor: float
    bullwhip_flag_threshold: float
    channel_fill_start_prelaunch_weeks: float
    sublayer2_fill_distribution_weeks: float
    weeks_per_month: float

    def __post_init__(self) -> None:
        if self.ss_units_per_new_site != self.initial_stocking_units_per_new_site:
            raise ValueError(
                "ss_units_per_new_site must equal initial_stocking_units_per_new_site in the "
                "current deterministic Phase 3 design."
            )
        if self.january_softening_factor <= 0 or self.january_softening_factor > 1:
            raise ValueError(
                "january_softening_factor must be greater than 0 and less than or equal to 1."
            )


@dataclass(frozen=True)
class GeographyTradeConfig:
    geography_code: str
    site_activation_rate: float
    certified_sites_at_launch: float
    certified_sites_at_peak: float

    def __post_init__(self) -> None:
        if self.certified_sites_at_peak < self.certified_sites_at_launch:
            raise ValueError(
                f"geography_defaults.{self.geography_code}.certified_sites_at_peak must be "
                "greater than or equal to certified_sites_at_launch."
            )


@dataclass(frozen=True)
class LaunchEventConfig:
    module: str
    geography_code: str
    launch_month_index: int


@dataclass(frozen=True)
class ValidationConfig:
    enforce_unique_output_keys: bool


@dataclass(frozen=True)
class Phase3Config:
    scenario_name: str
    parameter_config_path: Path
    model: ModelConfig
    modules: ModulesConfig
    input_paths: InputPaths
    output_paths: OutputPaths
    trade: TradeConfig
    geography_defaults: dict[str, GeographyTradeConfig]
    launch_events: dict[tuple[str, str], LaunchEventConfig]
    validation: ValidationConfig

    def get_geography_defaults(self, geography_code: str) -> GeographyTradeConfig:
        try:
            return self.geography_defaults[geography_code]
        except KeyError as exc:
            raise ValueError(
                f"Phase 3 geography_defaults is missing required geography {geography_code!r}."
            ) from exc

    def get_launch_event(self, module: str, geography_code: str) -> LaunchEventConfig:
        key = (module, geography_code)
        try:
            return self.launch_events[key]
        except KeyError as exc:
            raise ValueError(
                f"Phase 3 launch_events is missing required module/geography event {key!r}."
            ) from exc


def load_phase3_config(scenario_path: Path) -> Phase3Config:
    scenario_path = scenario_path.resolve()
    scenario_data = _load_toml(scenario_path)
    scenario_dir = scenario_path.parent

    parameter_config_path = _resolve_path(scenario_dir, scenario_data["parameter_config"])
    parameter_data = _load_toml(parameter_config_path)

    model_data = parameter_data["model"]
    modules_data = parameter_data["modules"]
    trade_data = parameter_data["trade"]
    geography_defaults_data = parameter_data["geography_defaults"]
    launch_events_data = parameter_data["launch_events"]
    validation_data = parameter_data["validation"]
    inputs_data = scenario_data["inputs"]
    outputs_data = scenario_data["outputs"]

    model = ModelConfig(
        phase=int(model_data["phase"]),
        build_scope=str(model_data["build_scope"]),
        upstream_demand_contract=str(model_data["upstream_demand_contract"]),
    )
    modules = ModulesConfig(
        enabled=tuple(modules_data["enabled"]),
        disabled=tuple(modules_data["disabled"]),
    )
    input_paths = InputPaths(
        phase2_deterministic_cascade=_resolve_path(
            scenario_dir, inputs_data["phase2_deterministic_cascade"]
        )
    )
    output_paths = OutputPaths(
        deterministic_trade_layer=_resolve_path(
            scenario_dir, outputs_data["deterministic_trade_layer"]
        )
    )
    trade = TradeConfig(
        sublayer1_target_weeks_on_hand=_parse_positive_float(
            trade_data["sublayer1_target_weeks_on_hand"],
            "trade.sublayer1_target_weeks_on_hand",
        ),
        sublayer2_target_weeks_on_hand=_parse_positive_float(
            trade_data["sublayer2_target_weeks_on_hand"],
            "trade.sublayer2_target_weeks_on_hand",
        ),
        sublayer2_wastage_rate=_parse_probability(
            trade_data["sublayer2_wastage_rate"],
            "trade.sublayer2_wastage_rate",
        ),
        initial_stocking_units_per_new_site=_parse_positive_float(
            trade_data["initial_stocking_units_per_new_site"],
            "trade.initial_stocking_units_per_new_site",
        ),
        ss_units_per_new_site=_parse_positive_float(
            trade_data["ss_units_per_new_site"],
            "trade.ss_units_per_new_site",
        ),
        sublayer1_launch_fill_months_of_demand=_parse_nonnegative_float(
            trade_data["sublayer1_launch_fill_months_of_demand"],
            "trade.sublayer1_launch_fill_months_of_demand",
        ),
        rems_certification_lag_weeks=_parse_nonnegative_float(
            trade_data["rems_certification_lag_weeks"],
            "trade.rems_certification_lag_weeks",
        ),
        january_softening_enabled=bool(trade_data["january_softening_enabled"]),
        january_softening_factor=_parse_probability(
            trade_data["january_softening_factor"],
            "trade.january_softening_factor",
        ),
        bullwhip_flag_threshold=_parse_nonnegative_float(
            trade_data["bullwhip_flag_threshold"],
            "trade.bullwhip_flag_threshold",
        ),
        channel_fill_start_prelaunch_weeks=_parse_nonnegative_float(
            trade_data["channel_fill_start_prelaunch_weeks"],
            "trade.channel_fill_start_prelaunch_weeks",
        ),
        sublayer2_fill_distribution_weeks=_parse_positive_float(
            trade_data["sublayer2_fill_distribution_weeks"],
            "trade.sublayer2_fill_distribution_weeks",
        ),
        weeks_per_month=_parse_positive_float(
            trade_data.get("weeks_per_month", 4.33),
            "trade.weeks_per_month",
        ),
    )
    geography_defaults = _load_geography_defaults(geography_defaults_data)
    launch_events = _load_launch_events(launch_events_data)
    validation = ValidationConfig(
        enforce_unique_output_keys=bool(validation_data["enforce_unique_output_keys"])
    )

    return Phase3Config(
        scenario_name=str(scenario_data["scenario_name"]),
        parameter_config_path=parameter_config_path,
        model=model,
        modules=modules,
        input_paths=input_paths,
        output_paths=output_paths,
        trade=trade,
        geography_defaults=geography_defaults,
        launch_events=launch_events,
        validation=validation,
    )


def _load_geography_defaults(
    raw_geography_defaults: dict[str, dict[str, object]]
) -> dict[str, GeographyTradeConfig]:
    if not raw_geography_defaults:
        raise ValueError("Phase 3 geography_defaults must define at least one geography.")

    geography_defaults: dict[str, GeographyTradeConfig] = {}
    for geography_code, geography_data in raw_geography_defaults.items():
        geography_defaults[geography_code] = GeographyTradeConfig(
            geography_code=geography_code,
            site_activation_rate=_parse_positive_float(
                geography_data["site_activation_rate"],
                f"geography_defaults.{geography_code}.site_activation_rate",
            ),
            certified_sites_at_launch=_parse_nonnegative_float(
                geography_data["certified_sites_at_launch"],
                f"geography_defaults.{geography_code}.certified_sites_at_launch",
            ),
            certified_sites_at_peak=_parse_positive_float(
                geography_data["certified_sites_at_peak"],
                f"geography_defaults.{geography_code}.certified_sites_at_peak",
            ),
        )
    return geography_defaults


def _load_launch_events(
    raw_launch_events: dict[str, dict[str, dict[str, object]]]
) -> dict[tuple[str, str], LaunchEventConfig]:
    launch_events: dict[tuple[str, str], LaunchEventConfig] = {}
    for module, module_events in raw_launch_events.items():
        if module not in PHASE1_MODULES:
            raise ValueError(f"Phase 3 launch_events contains unsupported module {module!r}.")
        for geography_code, event_data in module_events.items():
            key = (module, geography_code)
            if key in launch_events:
                raise ValueError(f"Phase 3 launch_events has duplicate entry for {key!r}.")
            launch_events[key] = LaunchEventConfig(
                module=module,
                geography_code=geography_code,
                launch_month_index=int(event_data["launch_month_index"]),
            )
            if launch_events[key].launch_month_index <= 0:
                raise ValueError(
                    f"launch_events.{module}.{geography_code}.launch_month_index must be positive."
                )
    if not launch_events:
        raise ValueError("Phase 3 launch_events must define at least one module/geography event.")
    return launch_events
