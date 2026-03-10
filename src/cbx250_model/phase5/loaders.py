"""Input loading for deterministic Phase 5 inventory and shelf-life."""

from __future__ import annotations

from collections import defaultdict
from csv import DictReader
from dataclasses import dataclass
from pathlib import Path

from .config_schema import Phase5Config
from .schemas import (
    InventorySignal,
    Phase3InventoryInputRecord,
    Phase4MonthlySummaryInputRecord,
    Phase4ScheduleDetailInputRecord,
)


@dataclass(frozen=True)
class Phase5InputBundle:
    phase3_trade_layer: tuple[Phase3InventoryInputRecord, ...]
    phase4_schedule_detail: tuple[Phase4ScheduleDetailInputRecord, ...]
    phase4_monthly_summary: tuple[Phase4MonthlySummaryInputRecord, ...]
    inventory_signals: tuple[InventorySignal, ...]


def _load_csv_rows(path: Path, required_columns: tuple[str, ...]) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Required input file not found: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"Input file has no header row: {path}")
        missing_columns = [column for column in required_columns if column not in reader.fieldnames]
        if missing_columns:
            raise ValueError(f"Input file {path} is missing columns: {missing_columns}")
        return list(reader)


def load_phase3_trade_layer(path: Path) -> tuple[Phase3InventoryInputRecord, ...]:
    rows = _load_csv_rows(
        path,
        (
            "scenario_name",
            "geography_code",
            "module",
            "segment_code",
            "month_index",
            "calendar_month",
            "patient_fg_demand_units",
            "sublayer2_pull_units",
            "ex_factory_fg_demand_units",
            "sublayer2_wastage_units",
            "new_site_stocking_orders_units",
            "ss_site_stocking_units",
        ),
    )
    return tuple(Phase3InventoryInputRecord.from_row(row) for row in rows)


def load_phase4_schedule_detail(path: Path) -> tuple[Phase4ScheduleDetailInputRecord, ...]:
    rows = _load_csv_rows(
        path,
        (
            "scenario_name",
            "stage",
            "module",
            "geography_code",
            "batch_number",
            "demand_month_index",
            "demand_calendar_month",
            "month_index",
            "calendar_month",
            "planned_start_month_index",
            "planned_start_month",
            "planned_release_month_index",
            "planned_release_month",
            "batch_quantity",
            "quantity_unit",
        ),
    )
    return tuple(Phase4ScheduleDetailInputRecord.from_row(row) for row in rows)


def load_phase4_monthly_summary(path: Path) -> tuple[Phase4MonthlySummaryInputRecord, ...]:
    rows = _load_csv_rows(
        path,
        (
            "scenario_name",
            "geography_code",
            "module",
            "month_index",
            "calendar_month",
            "fg_release_units",
            "dp_release_units",
            "ds_release_quantity_mg",
            "ss_release_units",
        ),
    )
    return tuple(Phase4MonthlySummaryInputRecord.from_row(row) for row in rows)


def build_inventory_signals(
    phase3_rows: tuple[Phase3InventoryInputRecord, ...],
    phase4_rows: tuple[Phase4MonthlySummaryInputRecord, ...],
) -> tuple[InventorySignal, ...]:
    grouped: dict[tuple[str, str, str, int], dict[str, object]] = defaultdict(
        lambda: {
            "scenario_name": "",
            "geography_code": "",
            "module": "",
            "month_index": 0,
            "calendar_month": None,
            "patient_fg_demand_units": 0.0,
            "sublayer2_pull_units": 0.0,
            "ex_factory_fg_demand_units": 0.0,
            "sublayer2_wastage_units": 0.0,
            "new_site_stocking_orders_units": 0.0,
            "ss_site_stocking_units": 0.0,
            "fg_release_units": 0.0,
            "dp_release_units": 0.0,
            "ds_release_quantity_mg": 0.0,
            "ss_release_units": 0.0,
            "notes": [],
        }
    )

    for record in phase3_rows:
        key = (record.scenario_name, record.geography_code, record.module, record.month_index)
        bucket = grouped[key]
        bucket["scenario_name"] = record.scenario_name
        bucket["geography_code"] = record.geography_code
        bucket["module"] = record.module
        bucket["month_index"] = record.month_index
        bucket["calendar_month"] = record.calendar_month
        bucket["patient_fg_demand_units"] += record.patient_fg_demand_units
        bucket["sublayer2_pull_units"] += record.sublayer2_pull_units
        bucket["ex_factory_fg_demand_units"] += record.ex_factory_fg_demand_units
        bucket["sublayer2_wastage_units"] += record.sublayer2_wastage_units
        bucket["new_site_stocking_orders_units"] += record.new_site_stocking_orders_units
        bucket["ss_site_stocking_units"] += record.ss_site_stocking_units
        if record.notes:
            bucket["notes"].append(record.notes)

    for record in phase4_rows:
        key = (record.scenario_name, record.geography_code, record.module, record.month_index)
        bucket = grouped[key]
        bucket["scenario_name"] = record.scenario_name
        bucket["geography_code"] = record.geography_code
        bucket["module"] = record.module
        bucket["month_index"] = record.month_index
        bucket["calendar_month"] = record.calendar_month
        bucket["fg_release_units"] += record.fg_release_units
        bucket["dp_release_units"] += record.dp_release_units
        bucket["ds_release_quantity_mg"] += record.ds_release_quantity_mg
        bucket["ss_release_units"] += record.ss_release_units
        if record.notes:
            bucket["notes"].append(record.notes)

    signals: list[InventorySignal] = []
    for bucket in grouped.values():
        signals.append(
            InventorySignal(
                scenario_name=str(bucket["scenario_name"]),
                geography_code=str(bucket["geography_code"]),
                module=str(bucket["module"]),
                month_index=int(bucket["month_index"]),
                calendar_month=bucket["calendar_month"],  # type: ignore[arg-type]
                patient_fg_demand_units=float(bucket["patient_fg_demand_units"]),
                sublayer2_pull_units=float(bucket["sublayer2_pull_units"]),
                ex_factory_fg_demand_units=float(bucket["ex_factory_fg_demand_units"]),
                sublayer2_wastage_units=float(bucket["sublayer2_wastage_units"]),
                new_site_stocking_orders_units=float(bucket["new_site_stocking_orders_units"]),
                ss_site_stocking_units=float(bucket["ss_site_stocking_units"]),
                fg_release_units=float(bucket["fg_release_units"]),
                dp_release_units=float(bucket["dp_release_units"]),
                ds_release_quantity_mg=float(bucket["ds_release_quantity_mg"]),
                ss_release_units=float(bucket["ss_release_units"]),
                notes=" | ".join(bucket["notes"]),
            )
        )
    signals.sort(key=lambda item: (item.scenario_name, item.geography_code, item.module, item.month_index))
    return tuple(signals)


def load_phase5_inputs(config: Phase5Config) -> Phase5InputBundle:
    phase3_trade_layer = load_phase3_trade_layer(config.input_paths.phase3_trade_layer)
    phase4_schedule_detail = load_phase4_schedule_detail(config.input_paths.phase4_schedule_detail)
    phase4_monthly_summary = load_phase4_monthly_summary(config.input_paths.phase4_monthly_summary)
    return Phase5InputBundle(
        phase3_trade_layer=phase3_trade_layer,
        phase4_schedule_detail=phase4_schedule_detail,
        phase4_monthly_summary=phase4_monthly_summary,
        inventory_signals=build_inventory_signals(phase3_trade_layer, phase4_monthly_summary),
    )
