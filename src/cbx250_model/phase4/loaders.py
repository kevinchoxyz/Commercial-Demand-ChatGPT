"""Input loading for deterministic Phase 4 production scheduling.

Phase 4 reads the accepted upstream Phase 3 trade-layer contract only. It does
not assume anything about how upstream treated demand was originally generated.
"""

from __future__ import annotations

from collections import defaultdict
from csv import DictReader
from dataclasses import dataclass
from pathlib import Path

from .config_schema import Phase4Config
from .schemas import Phase3SchedulingInputRecord, SchedulingSignal


@dataclass(frozen=True)
class Phase4InputBundle:
    phase3_trade_layer: tuple[Phase3SchedulingInputRecord, ...]
    scheduling_signals: tuple[SchedulingSignal, ...]


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


def load_phase3_trade_layer(path: Path) -> tuple[Phase3SchedulingInputRecord, ...]:
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
            "launch_fill_component_units",
            "ex_factory_fg_demand_units",
            "bullwhip_amplification_factor",
        ),
    )
    return tuple(Phase3SchedulingInputRecord.from_row(row) for row in rows)


def build_scheduling_signals(
    records: tuple[Phase3SchedulingInputRecord, ...],
) -> tuple[SchedulingSignal, ...]:
    grouped: dict[tuple[str, str, str, int], dict[str, object]] = defaultdict(
        lambda: {
            "scenario_name": "",
            "geography_code": "",
            "module": "",
            "month_index": 0,
            "calendar_month": None,
            "patient_fg_demand_units": 0.0,
            "ex_factory_fg_demand_units": 0.0,
            "launch_fill_component_units": 0.0,
            "bullwhip_amplification_factor": 0.0,
            "notes": [],
        }
    )
    for record in records:
        key = (record.scenario_name, record.geography_code, record.module, record.month_index)
        bucket = grouped[key]
        bucket["scenario_name"] = record.scenario_name
        bucket["geography_code"] = record.geography_code
        bucket["module"] = record.module
        bucket["month_index"] = record.month_index
        bucket["calendar_month"] = record.calendar_month
        bucket["patient_fg_demand_units"] += record.patient_fg_demand_units
        bucket["ex_factory_fg_demand_units"] += record.ex_factory_fg_demand_units
        bucket["launch_fill_component_units"] += record.launch_fill_component_units
        bucket["bullwhip_amplification_factor"] = max(
            float(bucket["bullwhip_amplification_factor"]),
            record.bullwhip_amplification_factor,
        )
        if record.notes:
            bucket["notes"].append(record.notes)

    signals: list[SchedulingSignal] = []
    for bucket in grouped.values():
        patient_fg_demand_units = float(bucket["patient_fg_demand_units"])
        ex_factory_fg_demand_units = float(bucket["ex_factory_fg_demand_units"])
        channel_inventory_build_units = max(ex_factory_fg_demand_units - patient_fg_demand_units, 0.0)
        signals.append(
            SchedulingSignal(
                scenario_name=str(bucket["scenario_name"]),
                geography_code=str(bucket["geography_code"]),
                module=str(bucket["module"]),
                month_index=int(bucket["month_index"]),
                calendar_month=bucket["calendar_month"],  # type: ignore[arg-type]
                patient_fg_demand_units=patient_fg_demand_units,
                ex_factory_fg_demand_units=ex_factory_fg_demand_units,
                launch_fill_component_units=float(bucket["launch_fill_component_units"]),
                bullwhip_amplification_factor=float(bucket["bullwhip_amplification_factor"]),
                underlying_patient_consumption_units=patient_fg_demand_units,
                channel_inventory_build_units=channel_inventory_build_units,
                fg_release_units=patient_fg_demand_units,
                dp_release_units=0.0,
                ds_release_quantity_mg=0.0,
                ss_release_units=0.0,
                stepdown_applied=False,
                notes=" | ".join(bucket["notes"]),
            )
        )
    signals.sort(key=lambda item: (item.scenario_name, item.geography_code, item.module, item.month_index))
    return tuple(signals)


def load_phase4_inputs(config: Phase4Config) -> Phase4InputBundle:
    phase3_trade_layer = load_phase3_trade_layer(config.input_paths.phase3_trade_layer)
    return Phase4InputBundle(
        phase3_trade_layer=phase3_trade_layer,
        scheduling_signals=build_scheduling_signals(phase3_trade_layer),
    )
