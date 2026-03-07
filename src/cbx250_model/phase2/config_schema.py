"""Config schema and loader for Phase 2 deterministic cascade."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib

from ..constants import (
    CO_PACK_MODE_SEPARATE_SKU_FIRST,
    PHASE1_DISABLED_CAPABILITIES,
    PHASE1_MODULES,
    PHASE2_BUILD_SCOPE,
    PHASE2_UPSTREAM_DEMAND_CONTRACT,
    SUPPORTED_CO_PACK_MODES,
    SUPPORTED_DOSE_BASES,
    SUPPORTED_FG_VIALING_RULES,
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
    dose_basis: str
    co_pack_mode: str

    def __post_init__(self) -> None:
        if self.phase != 2:
            raise ValueError(f"Phase 2 cascade only supports phase=2, received {self.phase}.")
        if self.build_scope != PHASE2_BUILD_SCOPE:
            raise ValueError(
                f"Phase 2 cascade expects build_scope={PHASE2_BUILD_SCOPE!r}, received {self.build_scope!r}."
            )
        if self.upstream_demand_contract != PHASE2_UPSTREAM_DEMAND_CONTRACT:
            raise ValueError(
                "Phase 2 cascade expects upstream_demand_contract to be "
                f"{PHASE2_UPSTREAM_DEMAND_CONTRACT!r}, received {self.upstream_demand_contract!r}."
            )
        if self.dose_basis not in SUPPORTED_DOSE_BASES:
            raise ValueError(
                f"dose_basis must be one of {SUPPORTED_DOSE_BASES}, received {self.dose_basis!r}."
            )
        if self.co_pack_mode not in SUPPORTED_CO_PACK_MODES:
            raise ValueError(
                f"co_pack_mode must be one of {SUPPORTED_CO_PACK_MODES}, received {self.co_pack_mode!r}."
            )


@dataclass(frozen=True)
class ModulesConfig:
    enabled: tuple[str, ...]
    disabled: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.enabled != PHASE1_MODULES:
            raise ValueError(
                f"Phase 2 cascade expects enabled modules {PHASE1_MODULES}, received {self.enabled}."
            )
        missing_disabled = [item for item in PHASE1_DISABLED_CAPABILITIES if item not in self.disabled]
        if missing_disabled:
            raise ValueError(
                "Phase 2 cascade requires later-phase capabilities to remain disabled. "
                f"Missing disabled items: {missing_disabled}."
            )


@dataclass(frozen=True)
class InputPaths:
    phase1_monthlyized_output: Path


@dataclass(frozen=True)
class OutputPaths:
    deterministic_cascade: Path


@dataclass(frozen=True)
class ModuleSettingsConfig:
    module: str
    fixed_dose_mg: float
    weight_based_dose_mg_per_kg: float
    average_patient_weight_kg: float
    patient_weight_distribution: str
    doses_per_patient_per_month: float
    fg_vialing_rule: str
    fg_mg_per_unit: float

    def __post_init__(self) -> None:
        if not self.patient_weight_distribution.strip():
            raise ValueError(f"module_settings.{self.module}.patient_weight_distribution is required.")
        if self.fg_vialing_rule not in SUPPORTED_FG_VIALING_RULES:
            raise ValueError(
                f"module_settings.{self.module}.fg_vialing_rule must be one of "
                f"{SUPPORTED_FG_VIALING_RULES}, received {self.fg_vialing_rule!r}."
            )


@dataclass(frozen=True)
class StepUpConfig:
    enabled: bool
    schedule_id: str

    def __post_init__(self) -> None:
        if not self.schedule_id.strip():
            raise ValueError("step_up.schedule_id is required.")


@dataclass(frozen=True)
class DoseReductionConfig:
    enabled: bool
    pct: float


@dataclass(frozen=True)
class CommercialAdjustmentsConfig:
    adherence_rate: float
    free_goods_pct: float


@dataclass(frozen=True)
class PlanYieldConfig:
    ds_to_dp: float
    dp_to_fg: float
    fg_pack: float
    ss: float


@dataclass(frozen=True)
class SSConfig:
    ratio_to_fg: float


@dataclass(frozen=True)
class ValidationConfig:
    enforce_unique_output_keys: bool


@dataclass(frozen=True)
class Phase2Config:
    scenario_name: str
    parameter_config_path: Path
    model: ModelConfig
    modules: ModulesConfig
    input_paths: InputPaths
    output_paths: OutputPaths
    module_settings: dict[str, ModuleSettingsConfig]
    step_up: StepUpConfig
    dose_reduction: DoseReductionConfig
    commercial_adjustments: CommercialAdjustmentsConfig
    plan_yield: PlanYieldConfig
    ss: SSConfig
    validation: ValidationConfig

    def get_module_settings(self, module: str) -> ModuleSettingsConfig:
        try:
            return self.module_settings[module]
        except KeyError as exc:
            raise ValueError(f"Missing Phase 2 module settings for {module!r}.") from exc


def load_phase2_config(scenario_path: Path) -> Phase2Config:
    scenario_path = scenario_path.resolve()
    scenario_data = _load_toml(scenario_path)
    scenario_dir = scenario_path.parent

    parameter_config_path = _resolve_path(scenario_dir, scenario_data["parameter_config"])
    parameter_data = _load_toml(parameter_config_path)

    model_data = parameter_data["model"]
    modules_data = parameter_data["modules"]
    module_settings_data = parameter_data["module_settings"]
    step_up_data = parameter_data["step_up"]
    dose_reduction_data = parameter_data["dose_reduction"]
    commercial_adjustments_data = parameter_data["commercial_adjustments"]
    plan_yield_data = parameter_data["yield"]["plan"]
    ss_data = parameter_data["ss"]
    validation_data = parameter_data["validation"]
    inputs_data = scenario_data["inputs"]
    outputs_data = scenario_data["outputs"]

    model = ModelConfig(
        phase=int(model_data["phase"]),
        build_scope=str(model_data["build_scope"]),
        upstream_demand_contract=str(model_data["upstream_demand_contract"]),
        dose_basis=str(model_data["dose_basis"]),
        co_pack_mode=str(model_data.get("co_pack_mode", CO_PACK_MODE_SEPARATE_SKU_FIRST)),
    )
    modules = ModulesConfig(
        enabled=tuple(modules_data["enabled"]),
        disabled=tuple(modules_data["disabled"]),
    )
    input_paths = InputPaths(
        phase1_monthlyized_output=_resolve_path(
            scenario_dir, inputs_data["phase1_monthlyized_output"]
        )
    )
    output_paths = OutputPaths(
        deterministic_cascade=_resolve_path(scenario_dir, outputs_data["deterministic_cascade"])
    )
    module_settings = _load_module_settings(module_settings_data)
    step_up = StepUpConfig(
        enabled=bool(step_up_data["enabled"]),
        schedule_id=str(step_up_data["schedule_id"]),
    )
    dose_reduction = DoseReductionConfig(
        enabled=bool(dose_reduction_data["enabled"]),
        pct=_parse_probability(dose_reduction_data["pct"], "dose_reduction.pct"),
    )
    commercial_adjustments = CommercialAdjustmentsConfig(
        adherence_rate=_parse_probability(
            commercial_adjustments_data["adherence_rate"],
            "commercial_adjustments.adherence_rate",
        ),
        free_goods_pct=_parse_nonnegative_float(
            commercial_adjustments_data["free_goods_pct"],
            "commercial_adjustments.free_goods_pct",
        ),
    )
    plan_yield = PlanYieldConfig(
        ds_to_dp=_parse_probability(plan_yield_data["ds_to_dp"], "yield.plan.ds_to_dp"),
        dp_to_fg=_parse_probability(plan_yield_data["dp_to_fg"], "yield.plan.dp_to_fg"),
        fg_pack=_parse_probability(plan_yield_data["fg_pack"], "yield.plan.fg_pack"),
        ss=_parse_probability(plan_yield_data["ss"], "yield.plan.ss"),
    )
    for field_name, value in (
        ("yield.plan.ds_to_dp", plan_yield.ds_to_dp),
        ("yield.plan.dp_to_fg", plan_yield.dp_to_fg),
        ("yield.plan.fg_pack", plan_yield.fg_pack),
        ("yield.plan.ss", plan_yield.ss),
    ):
        if value == 0:
            raise ValueError(f"{field_name} must be greater than zero.")
    ss = SSConfig(
        ratio_to_fg=_parse_nonnegative_float(ss_data["ratio_to_fg"], "ss.ratio_to_fg")
    )
    validation = ValidationConfig(
        enforce_unique_output_keys=bool(validation_data["enforce_unique_output_keys"])
    )

    return Phase2Config(
        scenario_name=str(scenario_data["scenario_name"]),
        parameter_config_path=parameter_config_path,
        model=model,
        modules=modules,
        input_paths=input_paths,
        output_paths=output_paths,
        module_settings=module_settings,
        step_up=step_up,
        dose_reduction=dose_reduction,
        commercial_adjustments=commercial_adjustments,
        plan_yield=plan_yield,
        ss=ss,
        validation=validation,
    )


def _load_module_settings(raw_module_settings: dict[str, dict[str, object]]) -> dict[str, ModuleSettingsConfig]:
    missing_modules = [module for module in PHASE1_MODULES if module not in raw_module_settings]
    if missing_modules:
        raise ValueError(f"Phase 2 module_settings is missing required modules: {missing_modules}.")
    extra_modules = sorted(set(raw_module_settings) - set(PHASE1_MODULES))
    if extra_modules:
        raise ValueError(f"Phase 2 module_settings contains unsupported modules: {extra_modules}.")

    module_settings: dict[str, ModuleSettingsConfig] = {}
    for module in PHASE1_MODULES:
        module_data = raw_module_settings[module]
        module_settings[module] = ModuleSettingsConfig(
            module=module,
            fixed_dose_mg=_parse_positive_float(
                module_data["fixed_dose_mg"],
                f"module_settings.{module}.fixed_dose_mg",
            ),
            weight_based_dose_mg_per_kg=_parse_positive_float(
                module_data["weight_based_dose_mg_per_kg"],
                f"module_settings.{module}.weight_based_dose_mg_per_kg",
            ),
            average_patient_weight_kg=_parse_positive_float(
                module_data["average_patient_weight_kg"],
                f"module_settings.{module}.average_patient_weight_kg",
            ),
            patient_weight_distribution=str(module_data["patient_weight_distribution"]),
            doses_per_patient_per_month=_parse_positive_float(
                module_data["doses_per_patient_per_month"],
                f"module_settings.{module}.doses_per_patient_per_month",
            ),
            fg_vialing_rule=str(module_data["fg_vialing_rule"]),
            fg_mg_per_unit=_parse_positive_float(
                module_data["fg_mg_per_unit"],
                f"module_settings.{module}.fg_mg_per_unit",
            ),
        )
    return module_settings
