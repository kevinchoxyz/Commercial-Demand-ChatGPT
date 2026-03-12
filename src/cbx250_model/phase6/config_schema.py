"""Config schema and loader for deterministic Phase 6 financial analytics."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib

from ..constants import (
    PHASE1_MODULES,
    PHASE6_BUILD_SCOPE,
    PHASE6_DISABLED_CAPABILITIES,
    PHASE6_UPSTREAM_VALUE_CONTRACT,
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
    upstream_value_contract: str

    def __post_init__(self) -> None:
        if self.phase != 6:
            raise ValueError(f"Phase 6 financial layer only supports phase=6, received {self.phase}.")
        if self.build_scope != PHASE6_BUILD_SCOPE:
            raise ValueError(
                f"Phase 6 financial layer expects build_scope={PHASE6_BUILD_SCOPE!r}, "
                f"received {self.build_scope!r}."
            )
        if self.upstream_value_contract != PHASE6_UPSTREAM_VALUE_CONTRACT:
            raise ValueError(
                "Phase 6 financial layer expects upstream_value_contract to be "
                f"{PHASE6_UPSTREAM_VALUE_CONTRACT!r}, received {self.upstream_value_contract!r}."
            )


@dataclass(frozen=True)
class ModulesConfig:
    enabled: tuple[str, ...]
    disabled: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.enabled != PHASE1_MODULES:
            raise ValueError(
                f"Phase 6 financial layer expects enabled modules {PHASE1_MODULES}, received {self.enabled}."
            )
        missing_disabled = [item for item in PHASE6_DISABLED_CAPABILITIES if item not in self.disabled]
        if missing_disabled:
            raise ValueError(
                "Phase 6 financial layer requires later-phase capabilities to remain disabled. "
                f"Missing disabled items: {missing_disabled}."
            )


@dataclass(frozen=True)
class InputPaths:
    phase4_monthly_summary: Path
    phase5_inventory_detail: Path
    phase5_monthly_inventory_summary: Path


@dataclass(frozen=True)
class OutputPaths:
    financial_detail: Path
    monthly_financial_summary: Path
    annual_financial_summary: Path


@dataclass(frozen=True)
class CostBasisConfig:
    ds_standard_cost_basis_unit: str
    ds_standard_cost_per_mg: float
    dp_conversion_cost_per_unit: float
    fg_packaging_labeling_cost_per_unit: float
    ss_standard_cost_per_unit: float
    geography_fg_packaging_labeling_cost_overrides: dict[str, float]

    def __post_init__(self) -> None:
        if self.ds_standard_cost_basis_unit != "mg":
            raise ValueError(
                "Phase 6 financial layer currently supports ds_standard_cost_basis_unit='mg' only."
            )


@dataclass(frozen=True)
class CarryingCostConfig:
    annual_inventory_carry_rate: float
    monthly_inventory_carry_rate: float


@dataclass(frozen=True)
class ExpiryWriteoffConfig:
    expired_inventory_writeoff_rate: float
    expired_inventory_salvage_rate: float


@dataclass(frozen=True)
class ValuationPolicyConfig:
    value_unmatched_fg_at_fg_standard_cost: bool
    include_trade_node_fg_value: bool
    use_matched_administrable_fg_value: bool


@dataclass(frozen=True)
class ShippingColdChainConfig:
    us_fg_sub1_to_sub2_cost_per_unit: float
    eu_fg_sub1_to_sub2_cost_per_unit: float
    us_ss_sub1_to_sub2_cost_per_unit: float
    eu_ss_sub1_to_sub2_cost_per_unit: float


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
    reconciliation_tolerance_value: float


@dataclass(frozen=True)
class Phase6Config:
    scenario_name: str
    parameter_config_path: Path
    model: ModelConfig
    modules: ModulesConfig
    input_paths: InputPaths
    output_paths: OutputPaths
    cost_basis: CostBasisConfig
    carrying_cost: CarryingCostConfig
    expiry_writeoff: ExpiryWriteoffConfig
    valuation_policy: ValuationPolicyConfig
    shipping_cold_chain: ShippingColdChainConfig
    conversion: ConversionConfig
    validation: ValidationConfig


def load_phase6_config(scenario_path: Path) -> Phase6Config:
    scenario_path = scenario_path.resolve()
    scenario_data = _load_toml(scenario_path)
    scenario_dir = scenario_path.parent

    parameter_config_path = _resolve_path(scenario_dir, str(scenario_data["parameter_config"]))
    parameter_data = _load_toml(parameter_config_path)

    model_data = parameter_data["model"]
    modules_data = parameter_data["modules"]
    cost_basis_data = parameter_data["cost_basis"]
    carrying_cost_data = parameter_data["carrying_cost"]
    expiry_writeoff_data = parameter_data["expiry_writeoff"]
    valuation_policy_data = parameter_data["valuation_policy"]
    shipping_cold_chain_data = parameter_data.get("shipping_cold_chain", {})
    conversion_data = parameter_data["conversion"]
    validation_data = parameter_data["validation"]
    geography_overrides = parameter_data.get("geography_fg_packaging_labeling_cost_overrides", {})
    inputs_data = scenario_data["inputs"]
    outputs_data = scenario_data["outputs"]

    annual_carry_rate = _parse_nonnegative_float(
        carrying_cost_data["annual_inventory_carry_rate"],
        "carrying_cost.annual_inventory_carry_rate",
    )
    monthly_carry_raw = carrying_cost_data.get("monthly_inventory_carry_rate")
    monthly_carry_rate = (
        annual_carry_rate / 12.0
        if monthly_carry_raw is None
        else _parse_nonnegative_float(
            monthly_carry_raw,
            "carrying_cost.monthly_inventory_carry_rate",
        )
    )

    return Phase6Config(
        scenario_name=str(scenario_data["scenario_name"]),
        parameter_config_path=parameter_config_path,
        model=ModelConfig(
            phase=int(model_data["phase"]),
            build_scope=str(model_data["build_scope"]),
            upstream_value_contract=str(model_data["upstream_value_contract"]),
        ),
        modules=ModulesConfig(
            enabled=tuple(modules_data["enabled"]),
            disabled=tuple(modules_data["disabled"]),
        ),
        input_paths=InputPaths(
            phase4_monthly_summary=_resolve_path(scenario_dir, str(inputs_data["phase4_monthly_summary"])),
            phase5_inventory_detail=_resolve_path(scenario_dir, str(inputs_data["phase5_inventory_detail"])),
            phase5_monthly_inventory_summary=_resolve_path(
                scenario_dir,
                str(inputs_data["phase5_monthly_inventory_summary"]),
            ),
        ),
        output_paths=OutputPaths(
            financial_detail=_resolve_path(scenario_dir, str(outputs_data["financial_detail"])),
            monthly_financial_summary=_resolve_path(
                scenario_dir,
                str(outputs_data["monthly_financial_summary"]),
            ),
            annual_financial_summary=_resolve_path(
                scenario_dir,
                str(outputs_data["annual_financial_summary"]),
            ),
        ),
        cost_basis=CostBasisConfig(
            ds_standard_cost_basis_unit=str(cost_basis_data["ds_standard_cost_basis_unit"]),
            ds_standard_cost_per_mg=_parse_positive_float(
                cost_basis_data["ds_standard_cost_per_mg"],
                "cost_basis.ds_standard_cost_per_mg",
            ),
            dp_conversion_cost_per_unit=_parse_nonnegative_float(
                cost_basis_data["dp_conversion_cost_per_unit"],
                "cost_basis.dp_conversion_cost_per_unit",
            ),
            fg_packaging_labeling_cost_per_unit=_parse_nonnegative_float(
                cost_basis_data["fg_packaging_labeling_cost_per_unit"],
                "cost_basis.fg_packaging_labeling_cost_per_unit",
            ),
            ss_standard_cost_per_unit=_parse_nonnegative_float(
                cost_basis_data["ss_standard_cost_per_unit"],
                "cost_basis.ss_standard_cost_per_unit",
            ),
            geography_fg_packaging_labeling_cost_overrides={
                geography_code: _parse_nonnegative_float(
                    value,
                    f"geography_fg_packaging_labeling_cost_overrides.{geography_code}",
                )
                for geography_code, value in geography_overrides.items()
            },
        ),
        carrying_cost=CarryingCostConfig(
            annual_inventory_carry_rate=annual_carry_rate,
            monthly_inventory_carry_rate=monthly_carry_rate,
        ),
        expiry_writeoff=ExpiryWriteoffConfig(
            expired_inventory_writeoff_rate=_parse_probability(
                expiry_writeoff_data["expired_inventory_writeoff_rate"],
                "expiry_writeoff.expired_inventory_writeoff_rate",
            ),
            expired_inventory_salvage_rate=_parse_probability(
                expiry_writeoff_data["expired_inventory_salvage_rate"],
                "expiry_writeoff.expired_inventory_salvage_rate",
            ),
        ),
        valuation_policy=ValuationPolicyConfig(
            value_unmatched_fg_at_fg_standard_cost=bool(
                valuation_policy_data["value_unmatched_fg_at_fg_standard_cost"]
            ),
            include_trade_node_fg_value=bool(valuation_policy_data["include_trade_node_fg_value"]),
            use_matched_administrable_fg_value=bool(
                valuation_policy_data["use_matched_administrable_fg_value"]
            ),
        ),
        shipping_cold_chain=ShippingColdChainConfig(
            us_fg_sub1_to_sub2_cost_per_unit=_parse_nonnegative_float(
                shipping_cold_chain_data.get("us_fg_sub1_to_sub2_cost_per_unit", 0.0),
                "shipping_cold_chain.us_fg_sub1_to_sub2_cost_per_unit",
            ),
            eu_fg_sub1_to_sub2_cost_per_unit=_parse_nonnegative_float(
                shipping_cold_chain_data.get("eu_fg_sub1_to_sub2_cost_per_unit", 0.0),
                "shipping_cold_chain.eu_fg_sub1_to_sub2_cost_per_unit",
            ),
            us_ss_sub1_to_sub2_cost_per_unit=_parse_nonnegative_float(
                shipping_cold_chain_data.get(
                    "us_ss_sub1_to_sub2_cost_per_unit",
                    shipping_cold_chain_data.get("us_fg_sub1_to_sub2_cost_per_unit", 0.0),
                ),
                "shipping_cold_chain.us_ss_sub1_to_sub2_cost_per_unit",
            ),
            eu_ss_sub1_to_sub2_cost_per_unit=_parse_nonnegative_float(
                shipping_cold_chain_data.get(
                    "eu_ss_sub1_to_sub2_cost_per_unit",
                    shipping_cold_chain_data.get("eu_fg_sub1_to_sub2_cost_per_unit", 0.0),
                ),
                "shipping_cold_chain.eu_ss_sub1_to_sub2_cost_per_unit",
            ),
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
            reconciliation_tolerance_value=_parse_nonnegative_float(
                validation_data["reconciliation_tolerance_value"],
                "validation.reconciliation_tolerance_value",
            ),
        ),
    )
