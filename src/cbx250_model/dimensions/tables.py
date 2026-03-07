"""Dimension table builders for the Phase 1 scaffold."""

from __future__ import annotations

from typing import Any

from ..calendar.monthly_calendar import MonthlyCalendar
from ..constants import MODULE_TO_INDICATION, MODULE_TO_SEGMENTS, PHASE1_MODULES
from ..inputs.loaders import InputBundle


def build_module_dimension() -> list[dict[str, str]]:
    return [
        {
            "module": module,
            "indication_code": MODULE_TO_INDICATION[module],
            "phase_scope": "Phase1",
        }
        for module in PHASE1_MODULES
    ]


def build_indication_dimension() -> list[dict[str, str]]:
    indications = ("AML", "MDS", "CML")
    return [{"indication_code": indication, "phase_scope": "Phase1"} for indication in indications]


def build_segment_dimension() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for module in PHASE1_MODULES:
        for segment in MODULE_TO_SEGMENTS[module]:
            rows.append(
                {
                    "module": module,
                    "indication_code": MODULE_TO_INDICATION[module],
                    "segment_code": segment,
                    "phase_scope": "Phase1",
                }
            )
    return rows


def build_geography_dimension(inputs: InputBundle) -> list[dict[str, str]]:
    geographies = {
        record.geography_code
        for record in (
            list(inputs.module_level_forecast)
            + list(inputs.segment_level_forecast)
            + list(inputs.epi_crosscheck)
            + list(inputs.aml_segment_mix)
            + list(inputs.mds_segment_mix)
            + list(inputs.cml_prevalent)
        )
    }
    return [{"geography_code": geography} for geography in sorted(geographies)]


def build_time_dimension(calendar: MonthlyCalendar) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for month in calendar.months:
        rows.append(
            {
                "month_index": month.month_index,
                "month_start": month.month_start.isoformat(),
                "month_id": month.month_id,
                "calendar_year": month.calendar_year,
                "calendar_month": month.calendar_month,
                "calendar_quarter": month.calendar_quarter,
            }
        )
    return rows


def build_dimensions(calendar: MonthlyCalendar, inputs: InputBundle) -> dict[str, list[dict[str, Any]]]:
    return {
        "dim_time": build_time_dimension(calendar),
        "dim_geography": build_geography_dimension(inputs),
        "dim_indication": build_indication_dimension(),
        "dim_segment": build_segment_dimension(),
        "dim_module": build_module_dimension(),
    }
