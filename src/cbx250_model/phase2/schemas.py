"""Typed Phase 2 contract records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json

from ..constants import MODULE_TO_SEGMENTS, PHASE1_HORIZON_MONTHS, PHASE1_MODULES


def _require_nonempty(value: str, field_name: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} is required.")
    return stripped


def _parse_month_index(value: str, field_name: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer, received {value!r}.") from exc
    if parsed <= 0 or parsed > PHASE1_HORIZON_MONTHS:
        raise ValueError(
            f"{field_name} must be between 1 and {PHASE1_HORIZON_MONTHS}, received {parsed}."
        )
    return parsed


def _parse_nonnegative_float(value: str, field_name: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be numeric, received {value!r}.") from exc
    if parsed < 0:
        raise ValueError(f"{field_name} must be non-negative, received {parsed}.")
    return parsed


def _parse_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date, received {value!r}.") from exc


@dataclass(frozen=True)
class Phase1MonthlyizedOutputRecord:
    scenario_name: str
    geography_code: str
    module: str
    segment_code: str
    month_index: int
    calendar_month: date
    patients_treated: float
    source_frequency: str
    source_grain: str
    source_sheet: str
    profile_id_used: str
    notes: str

    @property
    def key(self) -> tuple[str, str, str, str, int]:
        return (
            self.scenario_name,
            self.geography_code,
            self.module,
            self.segment_code,
            self.month_index,
        )

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "Phase1MonthlyizedOutputRecord":
        module = _require_nonempty(row["module"], "module")
        if module not in PHASE1_MODULES:
            raise ValueError(f"module must be one of {PHASE1_MODULES}, received {module!r}.")
        segment_code = _require_nonempty(row["segment_code"], "segment_code")
        if segment_code not in MODULE_TO_SEGMENTS[module]:
            raise ValueError(
                f"segment_code {segment_code!r} is not valid for module {module!r}. "
                f"Allowed values: {MODULE_TO_SEGMENTS[module]}."
            )
        return cls(
            scenario_name=_require_nonempty(row["scenario_name"], "scenario_name"),
            geography_code=_require_nonempty(row["geography_code"], "geography_code"),
            module=module,
            segment_code=segment_code,
            month_index=_parse_month_index(row["month_index"], "month_index"),
            calendar_month=_parse_date(row["calendar_month"], "calendar_month"),
            patients_treated=_parse_nonnegative_float(
                row["patients_treated_monthly"], "patients_treated_monthly"
            ),
            source_frequency=row.get("source_frequency", "").strip(),
            source_grain=row.get("source_grain", "").strip(),
            source_sheet=row.get("source_sheet", "").strip(),
            profile_id_used=row.get("profile_id_used", "").strip(),
            notes=row.get("notes", "").strip(),
        )


@dataclass(frozen=True)
class Phase2CascadeRecord:
    scenario_name: str
    geography_code: str
    module: str
    segment_code: str
    month_index: int
    calendar_month: date
    patients_treated: float
    doses_required: float
    mg_per_dose_before_reduction: float
    mg_per_dose_after_reduction: float
    mg_required: float
    fg_units_before_pack_yield: float
    fg_units_required: float
    ss_units_required: float
    dp_units_required: float
    ds_required: float
    dose_basis_used: str
    dose_reduction_applied: bool
    dose_reduction_pct: float
    adherence_rate_used: float
    free_goods_pct_used: float
    fg_vialing_rule_used: str
    fg_mg_per_unit_used: float
    ss_ratio_to_fg_used: float
    planning_yields_used: str
    phase1_source_frequency: str
    phase1_source_grain: str
    phase1_source_sheet: str
    phase1_profile_id_used: str
    notes: str

    @property
    def key(self) -> tuple[str, str, str, str, int]:
        return (
            self.scenario_name,
            self.geography_code,
            self.module,
            self.segment_code,
            self.month_index,
        )

    def as_csv_row(self) -> dict[str, str]:
        return {
            "scenario_name": self.scenario_name,
            "geography_code": self.geography_code,
            "module": self.module,
            "segment_code": self.segment_code,
            "month_index": str(self.month_index),
            "calendar_month": self.calendar_month.isoformat(),
            "patients_treated": _format_numeric(self.patients_treated),
            "doses_required": _format_numeric(self.doses_required),
            "mg_per_dose_before_reduction": _format_numeric(self.mg_per_dose_before_reduction),
            "mg_per_dose_after_reduction": _format_numeric(self.mg_per_dose_after_reduction),
            "mg_required": _format_numeric(self.mg_required),
            "fg_units_before_pack_yield": _format_numeric(self.fg_units_before_pack_yield),
            "fg_units_required": _format_numeric(self.fg_units_required),
            "ss_units_required": _format_numeric(self.ss_units_required),
            "dp_units_required": _format_numeric(self.dp_units_required),
            "ds_required": _format_numeric(self.ds_required),
            "dose_basis_used": self.dose_basis_used,
            "dose_reduction_applied": json.dumps(self.dose_reduction_applied),
            "dose_reduction_pct": _format_numeric(self.dose_reduction_pct),
            "adherence_rate_used": _format_numeric(self.adherence_rate_used),
            "free_goods_pct_used": _format_numeric(self.free_goods_pct_used),
            "fg_vialing_rule_used": self.fg_vialing_rule_used,
            "fg_mg_per_unit_used": _format_numeric(self.fg_mg_per_unit_used),
            "ss_ratio_to_fg_used": _format_numeric(self.ss_ratio_to_fg_used),
            "planning_yields_used": self.planning_yields_used,
            "phase1_source_frequency": self.phase1_source_frequency,
            "phase1_source_grain": self.phase1_source_grain,
            "phase1_source_sheet": self.phase1_source_sheet,
            "phase1_profile_id_used": self.phase1_profile_id_used,
            "notes": self.notes,
        }


def _format_numeric(value: float) -> str:
    return format(value, ".15g")
