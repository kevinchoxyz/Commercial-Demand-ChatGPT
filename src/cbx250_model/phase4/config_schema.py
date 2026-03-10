"""Config schema and loader for the deterministic Phase 4 production scheduler."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib

from ..constants import (
    PHASE1_MODULES,
    PHASE4_BUILD_SCOPE,
    PHASE4_DISABLED_CAPABILITIES,
    PHASE4_UPSTREAM_DEMAND_CONTRACT,
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
    upstream_demand_contract: str

    def __post_init__(self) -> None:
        if self.phase != 4:
            raise ValueError(f"Phase 4 production scheduling only supports phase=4, received {self.phase}.")
        if self.build_scope != PHASE4_BUILD_SCOPE:
            raise ValueError(
                f"Phase 4 production scheduling expects build_scope={PHASE4_BUILD_SCOPE!r}, "
                f"received {self.build_scope!r}."
            )
        if self.upstream_demand_contract != PHASE4_UPSTREAM_DEMAND_CONTRACT:
            raise ValueError(
                "Phase 4 production scheduling expects upstream_demand_contract to be "
                f"{PHASE4_UPSTREAM_DEMAND_CONTRACT!r}, received {self.upstream_demand_contract!r}."
            )


@dataclass(frozen=True)
class ModulesConfig:
    enabled: tuple[str, ...]
    disabled: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.enabled != PHASE1_MODULES:
            raise ValueError(
                f"Phase 4 production scheduling expects enabled modules {PHASE1_MODULES}, received {self.enabled}."
            )
        missing_disabled = [item for item in PHASE4_DISABLED_CAPABILITIES if item not in self.disabled]
        if missing_disabled:
            raise ValueError(
                "Phase 4 production scheduling requires later-phase capabilities to remain disabled. "
                f"Missing disabled items: {missing_disabled}."
            )


@dataclass(frozen=True)
class InputPaths:
    phase3_trade_layer: Path


@dataclass(frozen=True)
class OutputPaths:
    schedule_detail: Path
    monthly_summary: Path


@dataclass(frozen=True)
class ConversionConfig:
    dp_to_fg_yield: float
    ds_to_dp_yield: float
    ds_qty_per_dp_unit_mg: float
    ds_overage_factor: float
    ss_ratio_to_fg: float
    weeks_per_month: float


@dataclass(frozen=True)
class ReviewConfig:
    bullwhip_amplification_threshold: float
    bullwhip_review_window_months: int
    excess_build_threshold_ratio: float
    supply_gap_tolerance_units: float
    capacity_clip_tolerance_units: float


@dataclass(frozen=True)
class StepdownConfig:
    cml_prevalent_forward_window_months: int
    projected_cml_prevalent_bolus_exhaustion_month_index: int


@dataclass(frozen=True)
class FGConfig:
    lead_time_from_dp_release_weeks: float
    packaging_cycle_weeks: float
    release_qa_weeks: float
    total_order_to_release_weeks: float
    packaging_campaign_size_units: float


@dataclass(frozen=True)
class DPConfig:
    lead_time_from_ds_release_weeks: float
    manufacturing_cycle_weeks: float
    release_testing_weeks: float
    total_order_to_release_weeks: float
    min_batch_size_units: float
    max_batch_size_units: float
    min_campaign_batches: int
    annual_capacity_batches: int

    def __post_init__(self) -> None:
        if self.max_batch_size_units < self.min_batch_size_units:
            raise ValueError("dp.max_batch_size_units must be greater than or equal to dp.min_batch_size_units.")


@dataclass(frozen=True)
class DSConfig:
    lead_time_to_batch_start_planning_horizon_weeks: float
    manufacturing_cycle_weeks: float
    release_testing_weeks: float
    total_order_to_release_weeks: float
    min_batch_size_kg: float
    max_batch_size_kg: float
    min_campaign_batches: int
    annual_capacity_batches: int

    def __post_init__(self) -> None:
        if self.max_batch_size_kg < self.min_batch_size_kg:
            raise ValueError("ds.max_batch_size_kg must be greater than or equal to ds.min_batch_size_kg.")

    @property
    def min_batch_size_mg(self) -> float:
        return self.min_batch_size_kg * 1_000_000.0

    @property
    def max_batch_size_mg(self) -> float:
        return self.max_batch_size_kg * 1_000_000.0


@dataclass(frozen=True)
class SSConfig:
    order_to_release_lead_time_weeks: float
    batch_size_units: float
    min_campaign_batches: int
    annual_capacity_batches: int
    release_must_coincide_with_or_precede_fg: bool


@dataclass(frozen=True)
class ValidationConfig:
    enforce_unique_output_keys: bool


@dataclass(frozen=True)
class Phase4Config:
    scenario_name: str
    parameter_config_path: Path
    model: ModelConfig
    modules: ModulesConfig
    input_paths: InputPaths
    output_paths: OutputPaths
    conversion: ConversionConfig
    review: ReviewConfig
    stepdown: StepdownConfig
    fg: FGConfig
    dp: DPConfig
    ds: DSConfig
    ss: SSConfig
    validation: ValidationConfig


def load_phase4_config(scenario_path: Path) -> Phase4Config:
    scenario_path = scenario_path.resolve()
    scenario_data = _load_toml(scenario_path)
    scenario_dir = scenario_path.parent

    parameter_config_path = _resolve_path(scenario_dir, str(scenario_data["parameter_config"]))
    parameter_data = _load_toml(parameter_config_path)

    model_data = parameter_data["model"]
    modules_data = parameter_data["modules"]
    conversion_data = parameter_data["conversion"]
    review_data = parameter_data["review"]
    stepdown_data = parameter_data["stepdown"]
    fg_data = parameter_data["fg"]
    dp_data = parameter_data["dp"]
    ds_data = parameter_data["ds"]
    ss_data = parameter_data["ss"]
    validation_data = parameter_data["validation"]
    inputs_data = scenario_data["inputs"]
    outputs_data = scenario_data["outputs"]

    return Phase4Config(
        scenario_name=str(scenario_data["scenario_name"]),
        parameter_config_path=parameter_config_path,
        model=ModelConfig(
            phase=int(model_data["phase"]),
            build_scope=str(model_data["build_scope"]),
            upstream_demand_contract=str(model_data["upstream_demand_contract"]),
        ),
        modules=ModulesConfig(
            enabled=tuple(modules_data["enabled"]),
            disabled=tuple(modules_data["disabled"]),
        ),
        input_paths=InputPaths(
            phase3_trade_layer=_resolve_path(scenario_dir, str(inputs_data["phase3_trade_layer"]))
        ),
        output_paths=OutputPaths(
            schedule_detail=_resolve_path(scenario_dir, str(outputs_data["schedule_detail"])),
            monthly_summary=_resolve_path(scenario_dir, str(outputs_data["monthly_summary"])),
        ),
        conversion=ConversionConfig(
            dp_to_fg_yield=_parse_probability(conversion_data["dp_to_fg_yield"], "conversion.dp_to_fg_yield"),
            ds_to_dp_yield=_parse_probability(conversion_data["ds_to_dp_yield"], "conversion.ds_to_dp_yield"),
            ds_qty_per_dp_unit_mg=_parse_positive_float(
                conversion_data["ds_qty_per_dp_unit_mg"],
                "conversion.ds_qty_per_dp_unit_mg",
            ),
            ds_overage_factor=_parse_nonnegative_float(
                conversion_data["ds_overage_factor"],
                "conversion.ds_overage_factor",
            ),
            ss_ratio_to_fg=_parse_positive_float(conversion_data["ss_ratio_to_fg"], "conversion.ss_ratio_to_fg"),
            weeks_per_month=_parse_positive_float(conversion_data["weeks_per_month"], "conversion.weeks_per_month"),
        ),
        review=ReviewConfig(
            bullwhip_amplification_threshold=_parse_positive_float(
                review_data["bullwhip_amplification_threshold"],
                "review.bullwhip_amplification_threshold",
            ),
            bullwhip_review_window_months=_parse_positive_int(
                review_data["bullwhip_review_window_months"],
                "review.bullwhip_review_window_months",
            ),
            excess_build_threshold_ratio=_parse_nonnegative_float(
                review_data["excess_build_threshold_ratio"],
                "review.excess_build_threshold_ratio",
            ),
            supply_gap_tolerance_units=_parse_nonnegative_float(
                review_data["supply_gap_tolerance_units"],
                "review.supply_gap_tolerance_units",
            ),
            capacity_clip_tolerance_units=_parse_nonnegative_float(
                review_data["capacity_clip_tolerance_units"],
                "review.capacity_clip_tolerance_units",
            ),
        ),
        stepdown=StepdownConfig(
            cml_prevalent_forward_window_months=_parse_positive_int(
                stepdown_data["cml_prevalent_forward_window_months"],
                "stepdown.cml_prevalent_forward_window_months",
            ),
            projected_cml_prevalent_bolus_exhaustion_month_index=int(
                stepdown_data["projected_cml_prevalent_bolus_exhaustion_month_index"]
            ),
        ),
        fg=FGConfig(
            lead_time_from_dp_release_weeks=_parse_nonnegative_float(
                fg_data["lead_time_from_dp_release_weeks"],
                "fg.lead_time_from_dp_release_weeks",
            ),
            packaging_cycle_weeks=_parse_positive_float(
                fg_data["packaging_cycle_weeks"],
                "fg.packaging_cycle_weeks",
            ),
            release_qa_weeks=_parse_positive_float(
                fg_data["release_qa_weeks"],
                "fg.release_qa_weeks",
            ),
            total_order_to_release_weeks=_parse_positive_float(
                fg_data["total_order_to_release_weeks"],
                "fg.total_order_to_release_weeks",
            ),
            packaging_campaign_size_units=_parse_positive_float(
                fg_data["packaging_campaign_size_units"],
                "fg.packaging_campaign_size_units",
            ),
        ),
        dp=DPConfig(
            lead_time_from_ds_release_weeks=_parse_nonnegative_float(
                dp_data["lead_time_from_ds_release_weeks"],
                "dp.lead_time_from_ds_release_weeks",
            ),
            manufacturing_cycle_weeks=_parse_positive_float(
                dp_data["manufacturing_cycle_weeks"],
                "dp.manufacturing_cycle_weeks",
            ),
            release_testing_weeks=_parse_positive_float(
                dp_data["release_testing_weeks"],
                "dp.release_testing_weeks",
            ),
            total_order_to_release_weeks=_parse_positive_float(
                dp_data["total_order_to_release_weeks"],
                "dp.total_order_to_release_weeks",
            ),
            min_batch_size_units=_parse_positive_float(
                dp_data["min_batch_size_units"],
                "dp.min_batch_size_units",
            ),
            max_batch_size_units=_parse_positive_float(
                dp_data["max_batch_size_units"],
                "dp.max_batch_size_units",
            ),
            min_campaign_batches=_parse_positive_int(
                dp_data["min_campaign_batches"],
                "dp.min_campaign_batches",
            ),
            annual_capacity_batches=_parse_positive_int(
                dp_data["annual_capacity_batches"],
                "dp.annual_capacity_batches",
            ),
        ),
        ds=DSConfig(
            lead_time_to_batch_start_planning_horizon_weeks=_parse_positive_float(
                ds_data["lead_time_to_batch_start_planning_horizon_weeks"],
                "ds.lead_time_to_batch_start_planning_horizon_weeks",
            ),
            manufacturing_cycle_weeks=_parse_positive_float(
                ds_data["manufacturing_cycle_weeks"],
                "ds.manufacturing_cycle_weeks",
            ),
            release_testing_weeks=_parse_positive_float(
                ds_data["release_testing_weeks"],
                "ds.release_testing_weeks",
            ),
            total_order_to_release_weeks=_parse_positive_float(
                ds_data["total_order_to_release_weeks"],
                "ds.total_order_to_release_weeks",
            ),
            min_batch_size_kg=_parse_positive_float(ds_data["min_batch_size_kg"], "ds.min_batch_size_kg"),
            max_batch_size_kg=_parse_positive_float(ds_data["max_batch_size_kg"], "ds.max_batch_size_kg"),
            min_campaign_batches=_parse_positive_int(
                ds_data["min_campaign_batches"],
                "ds.min_campaign_batches",
            ),
            annual_capacity_batches=_parse_positive_int(
                ds_data["annual_capacity_batches"],
                "ds.annual_capacity_batches",
            ),
        ),
        ss=SSConfig(
            order_to_release_lead_time_weeks=_parse_positive_float(
                ss_data["order_to_release_lead_time_weeks"],
                "ss.order_to_release_lead_time_weeks",
            ),
            batch_size_units=_parse_positive_float(ss_data["batch_size_units"], "ss.batch_size_units"),
            min_campaign_batches=_parse_positive_int(
                ss_data["min_campaign_batches"],
                "ss.min_campaign_batches",
            ),
            annual_capacity_batches=_parse_positive_int(
                ss_data["annual_capacity_batches"],
                "ss.annual_capacity_batches",
            ),
            release_must_coincide_with_or_precede_fg=bool(
                ss_data["release_must_coincide_with_or_precede_fg"]
            ),
        ),
        validation=ValidationConfig(
            enforce_unique_output_keys=bool(validation_data["enforce_unique_output_keys"])
        ),
    )
