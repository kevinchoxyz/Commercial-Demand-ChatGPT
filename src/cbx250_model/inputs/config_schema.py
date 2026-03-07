"""Config schema and loader for the Phase 1 scaffold."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import tomllib

from ..constants import (
    FORECAST_GRAIN_MODULE_LEVEL,
    PHASE1_DISABLED_CAPABILITIES,
    PHASE1_HORIZON_MONTHS,
    PHASE1_MODULES,
    PHASE1_TIME_GRAIN,
    PRIMARY_DEMAND_INPUT,
    SUPPORTED_FORECAST_GRAINS,
)


def _load_toml(path: Path) -> dict:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def _parse_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date, received {value!r}.") from exc


def _resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else (base_dir / path).resolve()


@dataclass(frozen=True)
class ModelConfig:
    phase: int
    build_scope: str
    primary_demand_input: str
    forecast_grain: str

    def __post_init__(self) -> None:
        if self.phase != 1:
            raise ValueError(f"Phase 1 scaffold only supports phase=1, received {self.phase}.")
        if self.primary_demand_input != PRIMARY_DEMAND_INPUT:
            raise ValueError(
                "Phase 1 scaffold expects primary_demand_input to be "
                f"{PRIMARY_DEMAND_INPUT!r}, received {self.primary_demand_input!r}."
            )
        if self.forecast_grain not in SUPPORTED_FORECAST_GRAINS:
            raise ValueError(
                "forecast_grain must be one of "
                f"{SUPPORTED_FORECAST_GRAINS}, received {self.forecast_grain!r}."
            )


@dataclass(frozen=True)
class HorizonConfig:
    us_aml_mds_initial_approval_date: date
    forecast_horizon_months: int
    time_grain: str

    def __post_init__(self) -> None:
        if self.forecast_horizon_months != PHASE1_HORIZON_MONTHS:
            raise ValueError(
                "Phase 1 scaffold requires a 240-month horizon, received "
                f"{self.forecast_horizon_months}."
            )
        if self.time_grain != PHASE1_TIME_GRAIN:
            raise ValueError(
                "Phase 1 scaffold requires monthly engine grain, received "
                f"{self.time_grain!r}."
            )


@dataclass(frozen=True)
class ModulesConfig:
    enabled: tuple[str, ...]
    disabled: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.enabled != PHASE1_MODULES:
            raise ValueError(
                "Phase 1 scaffold expects enabled modules to be "
                f"{PHASE1_MODULES}, received {self.enabled}."
            )
        missing_disabled = [item for item in PHASE1_DISABLED_CAPABILITIES if item not in self.disabled]
        if missing_disabled:
            raise ValueError(
                "Phase 1 scaffold requires later-phase capabilities to remain disabled. "
                f"Missing disabled items: {missing_disabled}."
            )


@dataclass(frozen=True)
class ValidationConfig:
    enforce_segment_share_rules: bool
    enforce_cml_prevalent_pool_constraints: bool
    enforce_epi_crosscheck_warning: bool = False


@dataclass(frozen=True)
class InputPaths:
    commercial_forecast_module_level: Path
    commercial_forecast_segment_level: Path
    epi_crosscheck: Path | None
    aml_segment_mix: Path
    mds_segment_mix: Path
    cml_prevalent: Path


@dataclass(frozen=True)
class Phase1Config:
    scenario_name: str
    parameter_config_path: Path
    model: ModelConfig
    horizon: HorizonConfig
    modules: ModulesConfig
    validation: ValidationConfig
    input_paths: InputPaths


def load_phase1_config(scenario_path: Path) -> Phase1Config:
    scenario_path = scenario_path.resolve()
    scenario_data = _load_toml(scenario_path)
    scenario_dir = scenario_path.parent

    parameter_config_path = _resolve_path(scenario_dir, scenario_data["parameter_config"])
    parameter_data = _load_toml(parameter_config_path)
    inputs_data = scenario_data["inputs"]

    model_data = parameter_data["model"]
    horizon_data = parameter_data["horizon"]
    modules_data = parameter_data["modules"]
    validation_data = parameter_data["validation"]

    model = ModelConfig(
        phase=int(model_data["phase"]),
        build_scope=str(model_data["build_scope"]),
        primary_demand_input=str(model_data["primary_demand_input"]),
        forecast_grain=str(model_data.get("forecast_grain", FORECAST_GRAIN_MODULE_LEVEL)),
    )
    horizon = HorizonConfig(
        us_aml_mds_initial_approval_date=_parse_date(
            str(horizon_data["us_aml_mds_initial_approval_date"]),
            "horizon.us_aml_mds_initial_approval_date",
        ),
        forecast_horizon_months=int(horizon_data["forecast_horizon_months"]),
        time_grain=str(horizon_data["time_grain"]),
    )
    modules = ModulesConfig(
        enabled=tuple(modules_data["enabled"]),
        disabled=tuple(modules_data["disabled"]),
    )
    validation = ValidationConfig(
        enforce_segment_share_rules=bool(validation_data["enforce_segment_share_rules"]),
        enforce_cml_prevalent_pool_constraints=bool(
            validation_data["enforce_cml_prevalent_pool_constraints"]
        ),
        enforce_epi_crosscheck_warning=bool(validation_data["enforce_epi_crosscheck_warning"]),
    )
    input_paths = InputPaths(
        commercial_forecast_module_level=_resolve_path(
            scenario_dir, inputs_data["commercial_forecast_module_level"]
        ),
        commercial_forecast_segment_level=_resolve_path(
            scenario_dir, inputs_data["commercial_forecast_segment_level"]
        ),
        epi_crosscheck=_resolve_path(scenario_dir, inputs_data["epi_crosscheck"])
        if inputs_data.get("epi_crosscheck")
        else None,
        aml_segment_mix=_resolve_path(scenario_dir, inputs_data["aml_segment_mix"]),
        mds_segment_mix=_resolve_path(scenario_dir, inputs_data["mds_segment_mix"]),
        cml_prevalent=_resolve_path(scenario_dir, inputs_data["cml_prevalent"]),
    )

    return Phase1Config(
        scenario_name=str(scenario_data["scenario_name"]),
        parameter_config_path=parameter_config_path,
        model=model,
        horizon=horizon,
        modules=modules,
        validation=validation,
        input_paths=input_paths,
    )
