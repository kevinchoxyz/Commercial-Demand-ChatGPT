"""Typed input records for Phase 1."""

from __future__ import annotations

from dataclasses import dataclass

from ..constants import MODULE_TO_SEGMENTS, PHASE1_MODULES


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
    if parsed <= 0:
        raise ValueError(f"{field_name} must be positive, received {parsed}.")
    return parsed


def _parse_nonnegative_float(value: str, field_name: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be numeric, received {value!r}.") from exc
    if parsed < 0:
        raise ValueError(f"{field_name} must be non-negative, received {parsed}.")
    return parsed


def _parse_boolish(value: str, field_name: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"", "true", "yes", "1"}:
        return normalized != ""
    if normalized in {"false", "no", "0"}:
        return False
    raise ValueError(f"{field_name} must be a boolean-like value, received {value!r}.")


@dataclass(frozen=True)
class ModuleLevelForecastRecord:
    geography_code: str
    module: str
    month_index: int
    patients_treated: float

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "ModuleLevelForecastRecord":
        module = _require_nonempty(row["module"], "module")
        if module not in PHASE1_MODULES:
            raise ValueError(f"module must be one of {PHASE1_MODULES}, received {module!r}.")
        return cls(
            geography_code=_require_nonempty(row["geography_code"], "geography_code"),
            module=module,
            month_index=_parse_month_index(row["month_index"], "month_index"),
            patients_treated=_parse_nonnegative_float(row["patients_treated"], "patients_treated"),
        )


@dataclass(frozen=True)
class SegmentLevelForecastRecord:
    geography_code: str
    module: str
    segment_code: str
    month_index: int
    patients_treated: float

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "SegmentLevelForecastRecord":
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
            geography_code=_require_nonempty(row["geography_code"], "geography_code"),
            module=module,
            segment_code=segment_code,
            month_index=_parse_month_index(row["month_index"], "month_index"),
            patients_treated=_parse_nonnegative_float(row["patients_treated"], "patients_treated"),
        )


@dataclass(frozen=True)
class EpiCrosscheckRecord:
    geography_code: str
    module: str
    month_index: int
    treatable_patients: float

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "EpiCrosscheckRecord":
        module = _require_nonempty(row["module"], "module")
        if module not in PHASE1_MODULES:
            raise ValueError(f"module must be one of {PHASE1_MODULES}, received {module!r}.")
        return cls(
            geography_code=_require_nonempty(row["geography_code"], "geography_code"),
            module=module,
            month_index=_parse_month_index(row["month_index"], "month_index"),
            treatable_patients=_parse_nonnegative_float(
                row["treatable_patients"], "treatable_patients"
            ),
        )


@dataclass(frozen=True)
class SegmentMixRecord:
    geography_code: str
    module: str
    segment_code: str
    month_index: int
    segment_share: float

    @classmethod
    def from_row(cls, row: dict[str, str], module: str) -> "SegmentMixRecord":
        if module not in ("AML", "MDS"):
            raise ValueError("Segment mix files are only supported for AML and MDS in Phase 1.")
        segment_code = _require_nonempty(row["segment_code"], "segment_code")
        if segment_code not in MODULE_TO_SEGMENTS[module]:
            raise ValueError(
                f"segment_code {segment_code!r} is not valid for module {module!r}. "
                f"Allowed values: {MODULE_TO_SEGMENTS[module]}."
            )
        segment_share = _parse_nonnegative_float(row["segment_share"], "segment_share")
        if segment_share > 1:
            raise ValueError(f"segment_share must be between 0 and 1, received {segment_share}.")
        return cls(
            geography_code=_require_nonempty(row["geography_code"], "geography_code"),
            module=module,
            segment_code=segment_code,
            month_index=_parse_month_index(row["month_index"], "month_index"),
            segment_share=segment_share,
        )


@dataclass(frozen=True)
class CMLPrevalentPoolRecord:
    geography_code: str
    month_index: int
    addressable_prevalent_pool: float

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "CMLPrevalentPoolRecord":
        return cls(
            geography_code=_require_nonempty(row["geography_code"], "geography_code"),
            month_index=_parse_month_index(row["month_index"], "month_index"),
            addressable_prevalent_pool=_parse_nonnegative_float(
                row["addressable_prevalent_pool"], "addressable_prevalent_pool"
            ),
        )


@dataclass(frozen=True)
class TreatmentDurationRecord:
    geography_code: str
    module: str
    segment_code: str
    treatment_duration_months: int
    active_flag: bool
    notes: str

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "TreatmentDurationRecord":
        module = _require_nonempty(row["module"], "module")
        if module not in PHASE1_MODULES:
            raise ValueError(f"module must be one of {PHASE1_MODULES}, received {module!r}.")
        segment_code = _require_nonempty(row["segment_code"], "segment_code")
        allowed_segment_codes = set(MODULE_TO_SEGMENTS[module]) | {"ALL"}
        if segment_code not in allowed_segment_codes:
            raise ValueError(
                f"segment_code {segment_code!r} is not valid for module {module!r}. "
                f"Allowed values: {sorted(allowed_segment_codes)}."
            )
        geography_code = _require_nonempty(row["geography_code"], "geography_code")
        if geography_code != "ALL":
            geography_code = geography_code
        return cls(
            geography_code=geography_code,
            module=module,
            segment_code=segment_code,
            treatment_duration_months=_parse_month_index(
                row["treatment_duration_months"],
                "treatment_duration_months",
            ),
            active_flag=_parse_boolish(row.get("active_flag", "true"), "active_flag"),
            notes=row.get("notes", "").strip(),
        )
