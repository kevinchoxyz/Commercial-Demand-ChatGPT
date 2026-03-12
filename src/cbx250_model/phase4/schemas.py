"""Typed contracts for deterministic Phase 4 production scheduling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json

from ..constants import MODULE_TO_SEGMENTS, PHASE1_MODULES


def _require_nonempty(value: str, field_name: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} is required.")
    return stripped


def _parse_positive_int(value: str, field_name: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer, received {value!r}.") from exc
    if parsed <= 0:
        raise ValueError(f"{field_name} must be positive, received {parsed}.")
    return parsed


def _parse_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date, received {value!r}.") from exc


def _parse_nonnegative_float(value: str, field_name: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be numeric, received {value!r}.") from exc
    if parsed < 0:
        raise ValueError(f"{field_name} must be non-negative, received {parsed}.")
    return parsed


def _format_numeric(value: float) -> str:
    return format(value, ".15g")


@dataclass(frozen=True)
class Phase3SchedulingInputRecord:
    scenario_name: str
    geography_code: str
    module: str
    segment_code: str
    month_index: int
    calendar_month: date
    patient_fg_demand_units: float
    launch_fill_component_units: float
    ex_factory_fg_demand_units: float
    bullwhip_amplification_factor: float
    notes: str

    @property
    def key(self) -> tuple[str, str, str, str, str, str, int]:
        return (
            self.scenario_name,
            self.geography_code,
            self.module,
            self.segment_code,
            self.month_index,
        )

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "Phase3SchedulingInputRecord":
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
            month_index=_parse_positive_int(row["month_index"], "month_index"),
            calendar_month=_parse_date(row["calendar_month"], "calendar_month"),
            patient_fg_demand_units=_parse_nonnegative_float(
                row["patient_fg_demand_units"], "patient_fg_demand_units"
            ),
            launch_fill_component_units=_parse_nonnegative_float(
                row["launch_fill_component_units"], "launch_fill_component_units"
            ),
            ex_factory_fg_demand_units=_parse_nonnegative_float(
                row["ex_factory_fg_demand_units"], "ex_factory_fg_demand_units"
            ),
            bullwhip_amplification_factor=_parse_nonnegative_float(
                row["bullwhip_amplification_factor"], "bullwhip_amplification_factor"
            ),
            notes=row.get("notes", "").strip(),
        )


@dataclass(frozen=True)
class SchedulingSignal:
    scenario_name: str
    geography_code: str
    module: str
    month_index: int
    calendar_month: date
    patient_fg_demand_units: float
    ex_factory_fg_demand_units: float
    launch_fill_component_units: float
    bullwhip_amplification_factor: float
    underlying_patient_consumption_units: float
    channel_inventory_build_units: float
    fg_release_units: float
    dp_release_units: float
    ds_release_quantity_mg: float
    ss_release_units: float
    stepdown_applied: bool
    notes: str

    @property
    def key(self) -> tuple[str, str, str, int]:
        return (self.scenario_name, self.geography_code, self.module, self.month_index)


@dataclass(frozen=True)
class ScheduleDetailRecord:
    scenario_name: str
    stage: str
    module: str
    geography_code: str
    batch_number: str
    demand_month_index: int
    demand_calendar_month: date
    support_start_month_index: int
    support_start_calendar_month: date
    support_end_month_index: int
    support_end_calendar_month: date
    month_index: int
    calendar_month: date
    planned_start_month_index: int
    planned_start_month: date
    planned_release_month_index: int
    planned_release_month: date
    batch_quantity: float
    allocated_support_quantity: float
    quantity_unit: str
    cumulative_released_quantity: float
    capacity_used: float
    capacity_limit: float
    capacity_metric: str
    capacity_flag: bool
    supply_gap_flag: bool
    excess_build_flag: bool
    bullwhip_review_flag: bool
    ss_fg_sync_flag: bool
    notes: str

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (
            self.scenario_name,
            self.stage,
            self.geography_code,
            self.batch_number,
        )

    def as_csv_row(self) -> dict[str, str]:
        return {
            "scenario_name": self.scenario_name,
            "stage": self.stage,
            "module": self.module,
            "geography_code": self.geography_code,
            "batch_number": self.batch_number,
            "demand_month_index": str(self.demand_month_index),
            "demand_calendar_month": self.demand_calendar_month.isoformat(),
            "support_start_month_index": str(self.support_start_month_index),
            "support_start_calendar_month": self.support_start_calendar_month.isoformat(),
            "support_end_month_index": str(self.support_end_month_index),
            "support_end_calendar_month": self.support_end_calendar_month.isoformat(),
            "month_index": str(self.month_index),
            "calendar_month": self.calendar_month.isoformat(),
            "planned_start_month_index": str(self.planned_start_month_index),
            "planned_start_month": self.planned_start_month.isoformat(),
            "planned_release_month_index": str(self.planned_release_month_index),
            "planned_release_month": self.planned_release_month.isoformat(),
            "batch_quantity": _format_numeric(self.batch_quantity),
            "allocated_support_quantity": _format_numeric(self.allocated_support_quantity),
            "quantity_unit": self.quantity_unit,
            "cumulative_released_quantity": _format_numeric(self.cumulative_released_quantity),
            "capacity_used": _format_numeric(self.capacity_used),
            "capacity_limit": _format_numeric(self.capacity_limit),
            "capacity_metric": self.capacity_metric,
            "capacity_flag": json.dumps(self.capacity_flag),
            "supply_gap_flag": json.dumps(self.supply_gap_flag),
            "excess_build_flag": json.dumps(self.excess_build_flag),
            "bullwhip_review_flag": json.dumps(self.bullwhip_review_flag),
            "ss_fg_sync_flag": json.dumps(self.ss_fg_sync_flag),
            "notes": self.notes,
        }


@dataclass(frozen=True)
class ScheduleAllocationRecord:
    scenario_name: str
    stage: str
    module: str
    geography_code: str
    allocated_module: str
    allocated_geography_code: str
    source_batch_number: str
    physical_batch_quantity: float
    quantity_unit: str
    planned_start_month_index: int
    planned_start_month: date
    planned_release_month_index: int
    planned_release_month: date
    allocated_to_demand_month_index: int
    allocated_to_demand_calendar_month: date
    allocated_support_quantity: float
    notes: str

    @property
    def key(self) -> tuple[str, str, str, str, int]:
        return (
            self.scenario_name,
            self.stage,
            self.geography_code,
            self.source_batch_number,
            self.allocated_geography_code,
            self.allocated_module,
            self.allocated_to_demand_month_index,
        )

    def as_csv_row(self) -> dict[str, str]:
        return {
            "scenario_name": self.scenario_name,
            "stage": self.stage,
            "module": self.module,
            "geography_code": self.geography_code,
            "allocated_module": self.allocated_module,
            "allocated_geography_code": self.allocated_geography_code,
            "source_batch_number": self.source_batch_number,
            "physical_batch_quantity": _format_numeric(self.physical_batch_quantity),
            "quantity_unit": self.quantity_unit,
            "planned_start_month_index": str(self.planned_start_month_index),
            "planned_start_month": self.planned_start_month.isoformat(),
            "planned_release_month_index": str(self.planned_release_month_index),
            "planned_release_month": self.planned_release_month.isoformat(),
            "allocated_to_demand_month_index": str(self.allocated_to_demand_month_index),
            "allocated_to_demand_calendar_month": self.allocated_to_demand_calendar_month.isoformat(),
            "allocated_support_quantity": _format_numeric(self.allocated_support_quantity),
            "notes": self.notes,
        }


@dataclass(frozen=True)
class ScheduleMonthlySummaryRecord:
    scenario_name: str
    geography_code: str
    module: str
    month_index: int
    calendar_month: date
    patient_fg_demand_units: float
    ex_factory_fg_demand_units: float
    underlying_patient_consumption_units: float
    channel_inventory_build_units: float
    launch_fill_component_units: float
    fg_release_units: float
    dp_release_units: float
    ds_release_quantity_mg: float
    ds_release_quantity_g: float
    ds_release_quantity_kg: float
    ss_release_units: float
    cumulative_fg_released: float
    cumulative_ss_released: float
    unmet_demand_units: float
    capacity_flag: bool
    supply_gap_flag: bool
    excess_build_flag: bool
    bullwhip_review_flag: bool
    ss_fg_sync_flag: bool
    stepdown_applied: bool
    notes: str

    @property
    def key(self) -> tuple[str, str, str, int]:
        return (self.scenario_name, self.geography_code, self.module, self.month_index)

    def as_csv_row(self) -> dict[str, str]:
        return {
            "scenario_name": self.scenario_name,
            "geography_code": self.geography_code,
            "module": self.module,
            "month_index": str(self.month_index),
            "calendar_month": self.calendar_month.isoformat(),
            "patient_fg_demand_units": _format_numeric(self.patient_fg_demand_units),
            "ex_factory_fg_demand_units": _format_numeric(self.ex_factory_fg_demand_units),
            "underlying_patient_consumption_units": _format_numeric(
                self.underlying_patient_consumption_units
            ),
            "channel_inventory_build_units": _format_numeric(self.channel_inventory_build_units),
            "launch_fill_component_units": _format_numeric(self.launch_fill_component_units),
            "fg_release_units": _format_numeric(self.fg_release_units),
            "dp_release_units": _format_numeric(self.dp_release_units),
            "ds_release_quantity_mg": _format_numeric(self.ds_release_quantity_mg),
            "ds_release_quantity_g": _format_numeric(self.ds_release_quantity_g),
            "ds_release_quantity_kg": _format_numeric(self.ds_release_quantity_kg),
            "ss_release_units": _format_numeric(self.ss_release_units),
            "cumulative_fg_released": _format_numeric(self.cumulative_fg_released),
            "cumulative_ss_released": _format_numeric(self.cumulative_ss_released),
            "unmet_demand_units": _format_numeric(self.unmet_demand_units),
            "capacity_flag": json.dumps(self.capacity_flag),
            "supply_gap_flag": json.dumps(self.supply_gap_flag),
            "excess_build_flag": json.dumps(self.excess_build_flag),
            "bullwhip_review_flag": json.dumps(self.bullwhip_review_flag),
            "ss_fg_sync_flag": json.dumps(self.ss_fg_sync_flag),
            "stepdown_applied": json.dumps(self.stepdown_applied),
            "notes": self.notes,
        }
