"""CSV output writer for the deterministic Phase 3 trade layer."""

from __future__ import annotations

from csv import DictWriter
from pathlib import Path

from .schemas import Phase3TradeRecord

PHASE3_OUTPUT_HEADERS = (
    "scenario_name",
    "geography_code",
    "module",
    "segment_code",
    "month_index",
    "calendar_month",
    "patient_fg_demand_units",
    "sublayer2_wastage_units",
    "sublayer2_inventory_target_units",
    "sublayer2_inventory_adjustment_units",
    "new_site_stocking_orders_units",
    "ss_site_stocking_units",
    "sublayer2_pull_units",
    "sublayer1_inventory_target_units",
    "sublayer1_inventory_adjustment_units",
    "ex_factory_fg_demand_units",
    "bullwhip_amplification_factor",
    "bullwhip_flag",
    "launch_fill_component_units",
    "ongoing_replenishment_component_units",
    "active_certified_sites",
    "new_certified_sites",
    "raw_new_certified_sites",
    "site_stocking_units_before_segment_allocation",
    "ss_site_stocking_units_before_segment_allocation",
    "site_stocking_allocation_share",
    "allocated_new_site_stocking_orders_units",
    "allocated_ss_site_stocking_units",
    "sublayer2_inventory_on_hand_end_units",
    "sublayer1_inventory_on_hand_end_units",
    "january_softening_applied",
    "trade_parameters_used",
    "notes",
)


def write_phase3_outputs(path: Path, outputs: tuple[Phase3TradeRecord, ...]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = DictWriter(handle, fieldnames=PHASE3_OUTPUT_HEADERS)
        writer.writeheader()
        for record in outputs:
            writer.writerow(record.as_csv_row())
    return path
