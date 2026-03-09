"""Shared deterministic cohort roll-forward helpers for starts-based Phase 1 demand."""

from __future__ import annotations

from dataclasses import dataclass

from ..inputs.schemas import TreatmentDurationRecord


@dataclass(frozen=True)
class CohortMonthAudit:
    month_index: int
    patient_starts: float
    patients_continuing: float
    patients_rolloff: float
    patients_active: float

    @property
    def starts_input(self) -> float:
        return self.patient_starts

    @property
    def continuing_patients(self) -> float:
        return self.patients_continuing

    @property
    def rolloff_patients(self) -> float:
        return self.patients_rolloff

    @property
    def patients_treated(self) -> float:
        return self.patients_active


def build_treated_census_from_patient_starts(
    *,
    starts_by_month: dict[int, float],
    treatment_duration_months: int,
    horizon_months: int,
) -> tuple[CohortMonthAudit, ...]:
    if treatment_duration_months <= 0:
        raise ValueError(
            f"treatment_duration_months must be positive, received {treatment_duration_months}."
        )

    outputs: list[CohortMonthAudit] = []
    for month_index in range(1, horizon_months + 1):
        patient_starts = starts_by_month.get(month_index, 0.0)
        patients_continuing = sum(
            starts_by_month.get(prior_month_index, 0.0)
            for prior_month_index in range(
                max(1, month_index - treatment_duration_months + 1),
                month_index,
            )
        )
        patients_rolloff = starts_by_month.get(
            month_index - treatment_duration_months,
            0.0,
        )
        patients_active = patient_starts + patients_continuing
        if patients_active <= 0:
            continue
        outputs.append(
            CohortMonthAudit(
                month_index=month_index,
                patient_starts=patient_starts,
                patients_continuing=patients_continuing,
                patients_rolloff=patients_rolloff,
                patients_active=patients_active,
            )
        )
    return tuple(outputs)


def resolve_treatment_duration_months(
    *,
    records: tuple[TreatmentDurationRecord, ...],
    geography_code: str,
    module: str,
    segment_code: str,
) -> int:
    candidates: list[tuple[int, TreatmentDurationRecord]] = []
    for record in records:
        if not record.active_flag or record.module != module:
            continue
        geography_score = 1 if record.geography_code == geography_code else 0
        segment_score = 1 if record.segment_code == segment_code else 0
        if record.geography_code not in {geography_code, "ALL"}:
            continue
        if record.segment_code not in {segment_code, "ALL"}:
            continue
        candidates.append(((geography_score * 10) + segment_score, record))

    if not candidates:
        raise ValueError(
            "Missing treatment duration assumption for "
            f"geography_code={geography_code!r}, module={module!r}, segment_code={segment_code!r}."
        )

    candidates.sort(key=lambda item: item[0], reverse=True)
    best_score = candidates[0][0]
    best_matches = [record for score, record in candidates if score == best_score]
    if len(best_matches) > 1:
        raise ValueError(
            "Ambiguous treatment duration assumption scope for "
            f"geography_code={geography_code!r}, module={module!r}, segment_code={segment_code!r}."
        )
    return best_matches[0].treatment_duration_months
