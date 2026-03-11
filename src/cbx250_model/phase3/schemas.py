"""Typed contracts for the deterministic Phase 3 trade layer."""

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


def _format_numeric(value: float) -> str:
    return format(value, ".15g")


@dataclass(frozen=True)
class Phase2TradeInputRecord:
    scenario_name: str
    geography_code: str
    module: str
    segment_code: str
    month_index: int
    calendar_month: date
    patient_fg_demand_units: float
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
    def from_row(cls, row: dict[str, str]) -> "Phase2TradeInputRecord":
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
            patient_fg_demand_units=_parse_nonnegative_float(
                row["fg_units_required"], "fg_units_required"
            ),
            notes=row.get("notes", "").strip(),
        )


@dataclass(frozen=True)
class Phase3TradeRecord:
    scenario_name: str
    geography_code: str
    module: str
    segment_code: str
    month_index: int
    calendar_month: date
    patient_fg_demand_units: float
    sublayer2_wastage_units: float
    sublayer2_inventory_target_units: float
    sublayer2_inventory_adjustment_units: float
    new_site_stocking_orders_units: float
    ss_site_stocking_units: float
    sublayer2_pull_units: float
    sublayer1_inventory_target_units: float
    sublayer1_inventory_adjustment_units: float
    ex_factory_fg_demand_units: float
    bullwhip_amplification_factor: float
    bullwhip_flag: bool
    launch_fill_component_units: float
    ongoing_replenishment_component_units: float
    active_certified_sites: float
    new_certified_sites: float
    raw_new_certified_sites: float
    site_stocking_units_before_segment_allocation: float
    ss_site_stocking_units_before_segment_allocation: float
    site_stocking_allocation_share: float
    allocated_new_site_stocking_orders_units: float
    allocated_ss_site_stocking_units: float
    sublayer2_inventory_on_hand_end_units: float
    sublayer1_inventory_on_hand_end_units: float
    january_softening_applied: bool
    trade_parameters_used: str
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
            "patient_fg_demand_units": _format_numeric(self.patient_fg_demand_units),
            "sublayer2_wastage_units": _format_numeric(self.sublayer2_wastage_units),
            "sublayer2_inventory_target_units": _format_numeric(
                self.sublayer2_inventory_target_units
            ),
            "sublayer2_inventory_adjustment_units": _format_numeric(
                self.sublayer2_inventory_adjustment_units
            ),
            "new_site_stocking_orders_units": _format_numeric(
                self.new_site_stocking_orders_units
            ),
            "ss_site_stocking_units": _format_numeric(self.ss_site_stocking_units),
            "sublayer2_pull_units": _format_numeric(self.sublayer2_pull_units),
            "sublayer1_inventory_target_units": _format_numeric(
                self.sublayer1_inventory_target_units
            ),
            "sublayer1_inventory_adjustment_units": _format_numeric(
                self.sublayer1_inventory_adjustment_units
            ),
            "ex_factory_fg_demand_units": _format_numeric(self.ex_factory_fg_demand_units),
            "bullwhip_amplification_factor": _format_numeric(
                self.bullwhip_amplification_factor
            ),
            "bullwhip_flag": json.dumps(self.bullwhip_flag),
            "launch_fill_component_units": _format_numeric(self.launch_fill_component_units),
            "ongoing_replenishment_component_units": _format_numeric(
                self.ongoing_replenishment_component_units
            ),
            "active_certified_sites": _format_numeric(self.active_certified_sites),
            "new_certified_sites": _format_numeric(self.new_certified_sites),
            "raw_new_certified_sites": _format_numeric(self.raw_new_certified_sites),
            "site_stocking_units_before_segment_allocation": _format_numeric(
                self.site_stocking_units_before_segment_allocation
            ),
            "ss_site_stocking_units_before_segment_allocation": _format_numeric(
                self.ss_site_stocking_units_before_segment_allocation
            ),
            "site_stocking_allocation_share": _format_numeric(
                self.site_stocking_allocation_share
            ),
            "allocated_new_site_stocking_orders_units": _format_numeric(
                self.allocated_new_site_stocking_orders_units
            ),
            "allocated_ss_site_stocking_units": _format_numeric(
                self.allocated_ss_site_stocking_units
            ),
            "sublayer2_inventory_on_hand_end_units": _format_numeric(
                self.sublayer2_inventory_on_hand_end_units
            ),
            "sublayer1_inventory_on_hand_end_units": _format_numeric(
                self.sublayer1_inventory_on_hand_end_units
            ),
            "january_softening_applied": json.dumps(self.january_softening_applied),
            "trade_parameters_used": self.trade_parameters_used,
            "notes": self.notes,
        }
