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
