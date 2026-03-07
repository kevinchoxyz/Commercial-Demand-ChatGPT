"""Deterministic Phase 2 dose calculations."""

from __future__ import annotations

from dataclasses import dataclass

from ..constants import DOSE_BASIS_FIXED
from .config_schema import Phase2Config
from .schemas import Phase1MonthlyizedOutputRecord


@dataclass(frozen=True)
class DoseCalculation:
    doses_required: float
    mg_per_dose_before_reduction: float
    mg_per_dose_after_reduction: float
    mg_required: float
    dose_reduction_applied: bool


class DeterministicDoseEngine:
    """Convert normalized Phase 1 patients into deterministic dose demand."""

    def calculate(
        self,
        config: Phase2Config,
        input_row: Phase1MonthlyizedOutputRecord,
    ) -> DoseCalculation:
        if config.step_up.enabled:
            raise NotImplementedError(
                "PLACEHOLDER: step-up schedule execution is wired in config but not active in deterministic Phase 2."
            )

        module_settings = config.get_module_settings(input_row.module)
        mg_per_dose_before_reduction = self._resolve_mg_per_dose(config, input_row.module)
        mg_per_dose_after_reduction = mg_per_dose_before_reduction
        if config.dose_reduction.enabled:
            mg_per_dose_after_reduction *= 1.0 - config.dose_reduction.pct

        doses_required = (
            input_row.patients_treated
            * module_settings.doses_per_patient_per_month
            * config.commercial_adjustments.adherence_rate
        )
        mg_required = doses_required * mg_per_dose_after_reduction
        return DoseCalculation(
            doses_required=doses_required,
            mg_per_dose_before_reduction=mg_per_dose_before_reduction,
            mg_per_dose_after_reduction=mg_per_dose_after_reduction,
            mg_required=mg_required,
            dose_reduction_applied=config.dose_reduction.enabled,
        )

    @staticmethod
    def _resolve_mg_per_dose(config: Phase2Config, module: str) -> float:
        module_settings = config.get_module_settings(module)
        if config.model.dose_basis == DOSE_BASIS_FIXED:
            return module_settings.fixed_dose_mg
        return (
            module_settings.weight_based_dose_mg_per_kg
            * module_settings.average_patient_weight_kg
        )
