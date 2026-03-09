"""Shared deterministic cohort roll-forward helpers for starts-based Phase 1 demand."""

from __future__ import annotations

from dataclasses import dataclass

from ..inputs.schemas import TreatmentDurationRecord


@dataclass(frozen=True)
class CohortMonthAudit:
    month_index: int
    starts_input: float
    continuing_patients: float
    rolloff_patients: float
    patients_treated: float


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
        starts_input = starts_by_month.get(month_index, 0.0)
        continuing_patients = sum(
            starts_by_month.get(prior_month_index, 0.0)
            for prior_month_index in range(
                max(1, month_index - treatment_duration_months + 1),
                month_index,
            )
        )
        rolloff_patients = starts_by_month.get(
            month_index - treatment_duration_months,
            0.0,
        )
        patients_treated = starts_input + continuing_patients
        if patients_treated <= 0:
            continue
        outputs.append(
            CohortMonthAudit(
                month_index=month_index,
                starts_input=starts_input,
                continuing_patients=continuing_patients,
                rolloff_patients=rolloff_patients,
                patients_treated=patients_treated,
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
