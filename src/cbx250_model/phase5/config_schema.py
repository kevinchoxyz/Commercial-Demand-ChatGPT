"""Config schema and loader for deterministic Phase 5 inventory and shelf-life."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib

from ..constants import (
    PHASE1_MODULES,
    PHASE5_BUILD_SCOPE,
    PHASE5_DISABLED_CAPABILITIES,
    PHASE5_UPSTREAM_SUPPLY_CONTRACT,
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


def _parse_positive_int(value: object, field_name: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{field_name} must be a positive integer, received {parsed}.")
    return parsed


@dataclass(frozen=True)
class ModelConfig:
    phase: int
    build_scope: str
    upstream_supply_contract: str

    def __post_init__(self) -> None:
        if self.phase != 5:
            raise ValueError(f"Phase 5 inventory only supports phase=5, received {self.phase}.")
        if self.build_scope != PHASE5_BUILD_SCOPE:
            raise ValueError(
                f"Phase 5 inventory expects build_scope={PHASE5_BUILD_SCOPE!r}, "
                f"received {self.build_scope!r}."
            )
        if self.upstream_supply_contract != PHASE5_UPSTREAM_SUPPLY_CONTRACT:
            raise ValueError(
                "Phase 5 inventory expects upstream_supply_contract to be "
                f"{PHASE5_UPSTREAM_SUPPLY_CONTRACT!r}, received {self.upstream_supply_contract!r}."
            )


@dataclass(frozen=True)
class ModulesConfig:
    enabled: tuple[str, ...]
    disabled: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.enabled != PHASE1_MODULES:
            raise ValueError(
                f"Phase 5 inventory expects enabled modules {PHASE1_MODULES}, received {self.enabled}."
            )
        missing_disabled = [item for item in PHASE5_DISABLED_CAPABILITIES if item not in self.disabled]
        if missing_disabled:
            raise ValueError(
                "Phase 5 inventory requires later-phase capabilities to remain disabled. "
                f"Missing disabled items: {missing_disabled}."
            )


@dataclass(frozen=True)
class InputPaths:
    phase3_trade_layer: Path
    phase4_schedule_detail: Path
    phase4_monthly_summary: Path


@dataclass(frozen=True)
class OutputPaths:
    inventory_detail: Path
    monthly_inventory_summary: Path
    cohort_audit: Path


@dataclass(frozen=True)
class StartingInventoryConfig:
    ds_mg: float
    dp_units: float
    fg_units: float
    ss_units: float
    sublayer1_fg_units: float
    sublayer2_fg_units: float


@dataclass(frozen=True)
class ShelfLifeConfig:
    ds_months: int
    dp_months: int
    fg_months: int
    ss_months: int


@dataclass(frozen=True)
class PolicyConfig:
    excess_inventory_threshold_months_of_cover: float
    stockout_tolerance_units: float
    fefo_enabled: bool
    ss_fg_match_required: bool
    allow_prelaunch_inventory_build: bool


@dataclass(frozen=True)
class ConversionConfig:
    dp_to_fg_yield: float
    ds_to_dp_yield: float
    ds_qty_per_dp_unit_mg: float
    ds_overage_factor: float
    ss_ratio_to_fg: float


@dataclass(frozen=True)
class ValidationConfig:
    enforce_unique_output_keys: bool
    reconcile_phase4_receipts: bool
    reconciliation_tolerance_units: float


@dataclass(frozen=True)
class Phase5Config:
    scenario_name: str
    parameter_config_path: Path
    model: ModelConfig
    modules: ModulesConfig
    input_paths: InputPaths
    output_paths: OutputPaths
    starting_inventory: StartingInventoryConfig
    shelf_life: ShelfLifeConfig
    policy: PolicyConfig
    conversion: ConversionConfig
    validation: ValidationConfig


def load_phase5_config(scenario_path: Path) -> Phase5Config:
    scenario_path = scenario_path.resolve()
    scenario_data = _load_toml(scenario_path)
    scenario_dir = scenario_path.parent

    parameter_config_path = _resolve_path(scenario_dir, str(scenario_data["parameter_config"]))
    parameter_data = _load_toml(parameter_config_path)

    model_data = parameter_data["model"]
    modules_data = parameter_data["modules"]
    starting_inventory_data = parameter_data["starting_inventory"]
    shelf_life_data = parameter_data["shelf_life"]
    policy_data = parameter_data["policy"]
    conversion_data = parameter_data["conversion"]
    validation_data = parameter_data["validation"]
    inputs_data = scenario_data["inputs"]
    outputs_data = scenario_data["outputs"]

    return Phase5Config(
        scenario_name=str(scenario_data["scenario_name"]),
        parameter_config_path=parameter_config_path,
        model=ModelConfig(
            phase=int(model_data["phase"]),
            build_scope=str(model_data["build_scope"]),
            upstream_supply_contract=str(model_data["upstream_supply_contract"]),
        ),
        modules=ModulesConfig(
            enabled=tuple(modules_data["enabled"]),
            disabled=tuple(modules_data["disabled"]),
        ),
        input_paths=InputPaths(
            phase3_trade_layer=_resolve_path(scenario_dir, str(inputs_data["phase3_trade_layer"])),
            phase4_schedule_detail=_resolve_path(scenario_dir, str(inputs_data["phase4_schedule_detail"])),
            phase4_monthly_summary=_resolve_path(scenario_dir, str(inputs_data["phase4_monthly_summary"])),
        ),
        output_paths=OutputPaths(
            inventory_detail=_resolve_path(scenario_dir, str(outputs_data["inventory_detail"])),
            monthly_inventory_summary=_resolve_path(scenario_dir, str(outputs_data["monthly_inventory_summary"])),
            cohort_audit=_resolve_path(scenario_dir, str(outputs_data["cohort_audit"])),
        ),
        starting_inventory=StartingInventoryConfig(
            ds_mg=_parse_nonnegative_float(starting_inventory_data["ds_mg"], "starting_inventory.ds_mg"),
            dp_units=_parse_nonnegative_float(starting_inventory_data["dp_units"], "starting_inventory.dp_units"),
            fg_units=_parse_nonnegative_float(starting_inventory_data["fg_units"], "starting_inventory.fg_units"),
            ss_units=_parse_nonnegative_float(starting_inventory_data["ss_units"], "starting_inventory.ss_units"),
            sublayer1_fg_units=_parse_nonnegative_float(
                starting_inventory_data["sublayer1_fg_units"],
                "starting_inventory.sublayer1_fg_units",
            ),
            sublayer2_fg_units=_parse_nonnegative_float(
                starting_inventory_data["sublayer2_fg_units"],
                "starting_inventory.sublayer2_fg_units",
            ),
        ),
        shelf_life=ShelfLifeConfig(
            ds_months=_parse_positive_int(shelf_life_data["ds_months"], "shelf_life.ds_months"),
            dp_months=_parse_positive_int(shelf_life_data["dp_months"], "shelf_life.dp_months"),
            fg_months=_parse_positive_int(shelf_life_data["fg_months"], "shelf_life.fg_months"),
            ss_months=_parse_positive_int(shelf_life_data["ss_months"], "shelf_life.ss_months"),
        ),
        policy=PolicyConfig(
            excess_inventory_threshold_months_of_cover=_parse_nonnegative_float(
                policy_data["excess_inventory_threshold_months_of_cover"],
                "policy.excess_inventory_threshold_months_of_cover",
            ),
            stockout_tolerance_units=_parse_nonnegative_float(
                policy_data["stockout_tolerance_units"],
                "policy.stockout_tolerance_units",
            ),
            fefo_enabled=bool(policy_data["fefo_enabled"]),
            ss_fg_match_required=bool(policy_data["ss_fg_match_required"]),
            allow_prelaunch_inventory_build=bool(policy_data["allow_prelaunch_inventory_build"]),
        ),
        conversion=ConversionConfig(
            dp_to_fg_yield=_parse_probability(
                conversion_data["dp_to_fg_yield"],
                "conversion.dp_to_fg_yield",
            ),
            ds_to_dp_yield=_parse_probability(
                conversion_data["ds_to_dp_yield"],
                "conversion.ds_to_dp_yield",
            ),
            ds_qty_per_dp_unit_mg=_parse_positive_float(
                conversion_data["ds_qty_per_dp_unit_mg"],
                "conversion.ds_qty_per_dp_unit_mg",
            ),
            ds_overage_factor=_parse_nonnegative_float(
                conversion_data["ds_overage_factor"],
                "conversion.ds_overage_factor",
            ),
            ss_ratio_to_fg=_parse_positive_float(
                conversion_data["ss_ratio_to_fg"],
                "conversion.ss_ratio_to_fg",
            ),
        ),
        validation=ValidationConfig(
            enforce_unique_output_keys=bool(validation_data["enforce_unique_output_keys"]),
            reconcile_phase4_receipts=bool(validation_data["reconcile_phase4_receipts"]),
            reconciliation_tolerance_units=_parse_nonnegative_float(
                validation_data["reconciliation_tolerance_units"],
                "validation.reconciliation_tolerance_units",
            ),
        ),
    )
