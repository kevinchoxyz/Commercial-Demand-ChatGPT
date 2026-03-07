"""Deterministic Phase 2 dose and unit cascade."""

from __future__ import annotations

from dataclasses import dataclass
from json import dumps
from math import ceil

from .config_schema import Phase2Config
from .dose_engine import DeterministicDoseEngine
from .schemas import Phase1MonthlyizedOutputRecord, Phase2CascadeRecord


@dataclass(frozen=True)
class CascadeCalculation:
    fg_units_before_pack_yield: float
    fg_units_required: float
    ss_units_required: float
    dp_units_required: float
    ds_required: float


def build_phase2_outputs(
    config: Phase2Config,
    phase1_rows: tuple[Phase1MonthlyizedOutputRecord, ...],
) -> tuple[Phase2CascadeRecord, ...]:
    dose_engine = DeterministicDoseEngine()
    planning_yields_payload = dumps(
        {
            "plan_ds_to_dp_yield": config.plan_yield.ds_to_dp,
            "plan_dp_to_fg_yield": config.plan_yield.dp_to_fg,
            "plan_ss_yield": config.plan_yield.ss,
        },
        sort_keys=True,
    )

    outputs: list[Phase2CascadeRecord] = []
    for input_row in phase1_rows:
        module_settings = config.get_module_settings(input_row.module)
        dose_calc = dose_engine.calculate(config, input_row)
        cascade_calc = _cascade_from_dose_level(
            config,
            doses_required=dose_calc.doses_required,
            mg_per_dose_after_reduction=dose_calc.mg_per_dose_after_reduction,
            fg_mg_per_unit=module_settings.fg_mg_per_unit,
        )
        outputs.append(
            Phase2CascadeRecord(
                scenario_name=input_row.scenario_name,
                geography_code=input_row.geography_code,
                module=input_row.module,
                segment_code=input_row.segment_code,
                month_index=input_row.month_index,
                calendar_month=input_row.calendar_month,
                patients_treated=input_row.patients_treated,
                doses_required=dose_calc.doses_required,
                mg_per_dose_before_reduction=dose_calc.mg_per_dose_before_reduction,
                mg_per_dose_after_reduction=dose_calc.mg_per_dose_after_reduction,
                mg_required=dose_calc.mg_required,
                fg_units_before_pack_yield=cascade_calc.fg_units_before_pack_yield,
                fg_units_required=cascade_calc.fg_units_required,
                ss_units_required=cascade_calc.ss_units_required,
                dp_units_required=cascade_calc.dp_units_required,
                ds_required=cascade_calc.ds_required,
                dose_basis_used=config.model.dose_basis,
                dose_reduction_applied=dose_calc.dose_reduction_applied,
                dose_reduction_pct=config.dose_reduction.pct if config.dose_reduction.enabled else 0.0,
                adherence_rate_used=config.commercial_adjustments.adherence_rate,
                free_goods_pct_used=config.commercial_adjustments.free_goods_pct,
                fg_vialing_rule_used=module_settings.fg_vialing_rule,
                fg_mg_per_unit_used=module_settings.fg_mg_per_unit,
                ss_ratio_to_fg_used=config.ss.ratio_to_fg,
                planning_yields_used=planning_yields_payload,
                phase1_source_frequency=input_row.source_frequency,
                phase1_source_grain=input_row.source_grain,
                phase1_source_sheet=input_row.source_sheet,
                phase1_profile_id_used=input_row.profile_id_used,
                notes=input_row.notes,
            )
        )

    return tuple(outputs)


def _cascade_from_dose_level(
    config: Phase2Config,
    *,
    doses_required: float,
    mg_per_dose_after_reduction: float,
    fg_mg_per_unit: float,
) -> CascadeCalculation:
    vials_per_dose = (
        float(ceil(mg_per_dose_after_reduction / fg_mg_per_unit))
        if mg_per_dose_after_reduction > 0
        else 0.0
    )
    fg_units_before_pack_yield = doses_required * vials_per_dose
    fg_units_required = fg_units_before_pack_yield * (1.0 + config.commercial_adjustments.free_goods_pct)
    ss_units_required = fg_units_required * config.ss.ratio_to_fg
    dp_units_required = fg_units_required / config.plan_yield.dp_to_fg
    ds_required = dp_units_required / config.plan_yield.ds_to_dp
    return CascadeCalculation(
        fg_units_before_pack_yield=fg_units_before_pack_yield,
        fg_units_required=fg_units_required,
        ss_units_required=ss_units_required,
        dp_units_required=dp_units_required,
        ds_required=ds_required,
    )
