"""Typed contracts for deterministic Phase 5 inventory and shelf-life."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
import math

from ..constants import MODULE_TO_SEGMENTS, PHASE1_MODULES


INVENTORY_NODES = (
    "DS",
    "DP",
    "FG_Central",
    "SS_Central",
    "SubLayer1_FG",
    "SubLayer2_FG",
)


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


def _parse_int(value: str, field_name: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer, received {value!r}.") from exc


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
    if math.isinf(value):
        return "inf"
    return format(value, ".15g")


@dataclass(frozen=True)
class Phase3InventoryInputRecord:
    scenario_name: str
    geography_code: str
    module: str
    segment_code: str
    month_index: int
    calendar_month: date
    patient_fg_demand_units: float
    sublayer2_pull_units: float
    ex_factory_fg_demand_units: float
    sublayer2_wastage_units: float
    new_site_stocking_orders_units: float
    ss_site_stocking_units: float
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
    def from_row(cls, row: dict[str, str]) -> "Phase3InventoryInputRecord":
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
            sublayer2_pull_units=_parse_nonnegative_float(
                row["sublayer2_pull_units"], "sublayer2_pull_units"
            ),
            ex_factory_fg_demand_units=_parse_nonnegative_float(
                row["ex_factory_fg_demand_units"], "ex_factory_fg_demand_units"
            ),
            sublayer2_wastage_units=_parse_nonnegative_float(
                row["sublayer2_wastage_units"], "sublayer2_wastage_units"
            ),
            new_site_stocking_orders_units=_parse_nonnegative_float(
                row["new_site_stocking_orders_units"], "new_site_stocking_orders_units"
            ),
            ss_site_stocking_units=_parse_nonnegative_float(
                row["ss_site_stocking_units"], "ss_site_stocking_units"
            ),
            notes=row.get("notes", "").strip(),
        )


@dataclass(frozen=True)
class Phase4ScheduleDetailInputRecord:
    scenario_name: str
    stage: str
    module: str
    geography_code: str
    batch_number: str
    demand_month_index: int
    demand_calendar_month: date
    month_index: int
    calendar_month: date
    planned_start_month_index: int
    planned_start_month: date
    planned_release_month_index: int
    planned_release_month: date
    batch_quantity: float
    quantity_unit: str
    notes: str

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.scenario_name, self.stage, self.geography_code, self.batch_number)

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "Phase4ScheduleDetailInputRecord":
        stage = _require_nonempty(row["stage"], "stage")
        if stage not in ("DS", "DP", "FG", "SS"):
            raise ValueError(f"stage must be one of ('DS', 'DP', 'FG', 'SS'), received {stage!r}.")
        module = _require_nonempty(row["module"], "module")
        if module not in PHASE1_MODULES:
            raise ValueError(f"module must be one of {PHASE1_MODULES}, received {module!r}.")
        return cls(
            scenario_name=_require_nonempty(row["scenario_name"], "scenario_name"),
            stage=stage,
            module=module,
            geography_code=_require_nonempty(row["geography_code"], "geography_code"),
            batch_number=_require_nonempty(row["batch_number"], "batch_number"),
            demand_month_index=_parse_positive_int(row["demand_month_index"], "demand_month_index"),
            demand_calendar_month=_parse_date(row["demand_calendar_month"], "demand_calendar_month"),
            month_index=_parse_int(row["month_index"], "month_index"),
            calendar_month=_parse_date(row["calendar_month"], "calendar_month"),
            planned_start_month_index=_parse_int(
                row["planned_start_month_index"], "planned_start_month_index"
            ),
            planned_start_month=_parse_date(row["planned_start_month"], "planned_start_month"),
            planned_release_month_index=_parse_int(
                row["planned_release_month_index"], "planned_release_month_index"
            ),
            planned_release_month=_parse_date(
                row["planned_release_month"], "planned_release_month"
            ),
            batch_quantity=_parse_nonnegative_float(row["batch_quantity"], "batch_quantity"),
            quantity_unit=_require_nonempty(row["quantity_unit"], "quantity_unit"),
            notes=row.get("notes", "").strip(),
        )


@dataclass(frozen=True)
class Phase4MonthlySummaryInputRecord:
    scenario_name: str
    geography_code: str
    module: str
    month_index: int
    calendar_month: date
    fg_release_units: float
    dp_release_units: float
    ds_release_quantity_mg: float
    ss_release_units: float
    notes: str

    @property
    def key(self) -> tuple[str, str, str, int]:
        return (self.scenario_name, self.geography_code, self.module, self.month_index)

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "Phase4MonthlySummaryInputRecord":
        module = _require_nonempty(row["module"], "module")
        if module not in PHASE1_MODULES:
            raise ValueError(f"module must be one of {PHASE1_MODULES}, received {module!r}.")
        return cls(
            scenario_name=_require_nonempty(row["scenario_name"], "scenario_name"),
            geography_code=_require_nonempty(row["geography_code"], "geography_code"),
            module=module,
            month_index=_parse_positive_int(row["month_index"], "month_index"),
            calendar_month=_parse_date(row["calendar_month"], "calendar_month"),
            fg_release_units=_parse_nonnegative_float(row["fg_release_units"], "fg_release_units"),
            dp_release_units=_parse_nonnegative_float(row["dp_release_units"], "dp_release_units"),
            ds_release_quantity_mg=_parse_nonnegative_float(
                row["ds_release_quantity_mg"], "ds_release_quantity_mg"
            ),
            ss_release_units=_parse_nonnegative_float(row["ss_release_units"], "ss_release_units"),
            notes=row.get("notes", "").strip(),
        )


@dataclass(frozen=True)
class InventorySignal:
    scenario_name: str
    geography_code: str
    module: str
    month_index: int
    calendar_month: date
    patient_fg_demand_units: float
    sublayer2_pull_units: float
    ex_factory_fg_demand_units: float
    sublayer2_wastage_units: float
    new_site_stocking_orders_units: float
    ss_site_stocking_units: float
    fg_release_units: float
    dp_release_units: float
    ds_release_quantity_mg: float
    ss_release_units: float
    notes: str

    @property
    def key(self) -> tuple[str, str, str, int]:
        return (self.scenario_name, self.geography_code, self.module, self.month_index)


@dataclass(frozen=True)
class InventoryDetailRecord:
    scenario_name: str
    geography_code: str
    module: str
    month_index: int
    calendar_month: date
    material_node: str
    opening_inventory: float
    receipts: float
    issues: float
    expired_quantity: float
    ending_inventory: float
    available_nonexpired_inventory: float
    demand_signal_units: float
    shortfall_units: float
    months_of_cover: float
    stockout_flag: bool
    excess_inventory_flag: bool
    expiry_flag: bool
    fg_ss_mismatch_flag: bool
    matched_administrable_fg_units: float
    fg_ss_mismatch_units: float
    notes: str

    @property
    def key(self) -> tuple[str, str, str, int, str]:
        return (
            self.scenario_name,
            self.geography_code,
            self.module,
            self.month_index,
            self.material_node,
        )

    def as_csv_row(self) -> dict[str, str]:
        return {
            "scenario_name": self.scenario_name,
            "geography_code": self.geography_code,
            "module": self.module,
            "month_index": str(self.month_index),
            "calendar_month": self.calendar_month.isoformat(),
            "material_node": self.material_node,
            "opening_inventory": _format_numeric(self.opening_inventory),
            "receipts": _format_numeric(self.receipts),
            "issues": _format_numeric(self.issues),
            "expired_quantity": _format_numeric(self.expired_quantity),
            "ending_inventory": _format_numeric(self.ending_inventory),
            "available_nonexpired_inventory": _format_numeric(self.available_nonexpired_inventory),
            "demand_signal_units": _format_numeric(self.demand_signal_units),
            "shortfall_units": _format_numeric(self.shortfall_units),
            "months_of_cover": _format_numeric(self.months_of_cover),
            "stockout_flag": json.dumps(self.stockout_flag),
            "excess_inventory_flag": json.dumps(self.excess_inventory_flag),
            "expiry_flag": json.dumps(self.expiry_flag),
            "fg_ss_mismatch_flag": json.dumps(self.fg_ss_mismatch_flag),
            "matched_administrable_fg_units": _format_numeric(
                self.matched_administrable_fg_units
            ),
            "fg_ss_mismatch_units": _format_numeric(self.fg_ss_mismatch_units),
            "notes": self.notes,
        }


@dataclass(frozen=True)
class InventoryMonthlySummaryRecord:
    scenario_name: str
    geography_code: str
    module: str
    month_index: int
    calendar_month: date
    ds_inventory_mg: float
    dp_inventory_units: float
    fg_inventory_units: float
    ss_inventory_units: float
    sublayer1_fg_inventory_units: float
    sublayer2_fg_inventory_units: float
    expired_ds_mg: float
    expired_dp_units: float
    expired_fg_units: float
    expired_ss_units: float
    unmatched_fg_units: float
    matched_administrable_fg_units: float
    stockout_flag: bool
    excess_inventory_flag: bool
    expiry_flag: bool
    fg_ss_mismatch_flag: bool
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
            "ds_inventory_mg": _format_numeric(self.ds_inventory_mg),
            "dp_inventory_units": _format_numeric(self.dp_inventory_units),
            "fg_inventory_units": _format_numeric(self.fg_inventory_units),
            "ss_inventory_units": _format_numeric(self.ss_inventory_units),
            "sublayer1_fg_inventory_units": _format_numeric(self.sublayer1_fg_inventory_units),
            "sublayer2_fg_inventory_units": _format_numeric(self.sublayer2_fg_inventory_units),
            "expired_ds_mg": _format_numeric(self.expired_ds_mg),
            "expired_dp_units": _format_numeric(self.expired_dp_units),
            "expired_fg_units": _format_numeric(self.expired_fg_units),
            "expired_ss_units": _format_numeric(self.expired_ss_units),
            "unmatched_fg_units": _format_numeric(self.unmatched_fg_units),
            "matched_administrable_fg_units": _format_numeric(
                self.matched_administrable_fg_units
            ),
            "stockout_flag": json.dumps(self.stockout_flag),
            "excess_inventory_flag": json.dumps(self.excess_inventory_flag),
            "expiry_flag": json.dumps(self.expiry_flag),
            "fg_ss_mismatch_flag": json.dumps(self.fg_ss_mismatch_flag),
            "notes": self.notes,
        }


@dataclass(frozen=True)
class CohortAuditRecord:
    scenario_name: str
    geography_code: str
    module: str
    month_index: int
    calendar_month: date
    material_node: str
    cohort_id: str
    original_receipt_month_index: int
    receipt_month_index: int
    expiry_month_index: int
    ending_quantity: float
    notes: str

    @property
    def key(self) -> tuple[str, str, str, int, str, str]:
        return (
            self.scenario_name,
            self.geography_code,
            self.module,
            self.month_index,
            self.material_node,
            self.cohort_id,
        )

    def as_csv_row(self) -> dict[str, str]:
        return {
            "scenario_name": self.scenario_name,
            "geography_code": self.geography_code,
            "module": self.module,
            "month_index": str(self.month_index),
            "calendar_month": self.calendar_month.isoformat(),
            "material_node": self.material_node,
            "cohort_id": self.cohort_id,
            "original_receipt_month_index": str(self.original_receipt_month_index),
            "receipt_month_index": str(self.receipt_month_index),
            "expiry_month_index": str(self.expiry_month_index),
            "ending_quantity": _format_numeric(self.ending_quantity),
            "notes": self.notes,
        }
