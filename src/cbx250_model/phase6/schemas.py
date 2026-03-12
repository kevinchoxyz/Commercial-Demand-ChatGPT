"""Schemas for deterministic Phase 6 financial analytics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Phase4FinancialInputRecord:
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
    def from_row(cls, row: dict[str, str]) -> "Phase4FinancialInputRecord":
        return cls(
            scenario_name=row["scenario_name"],
            geography_code=row["geography_code"],
            module=row["module"],
            month_index=int(row["month_index"]),
            calendar_month=date.fromisoformat(row["calendar_month"]),
            fg_release_units=float(row["fg_release_units"]),
            dp_release_units=float(row["dp_release_units"]),
            ds_release_quantity_mg=float(row["ds_release_quantity_mg"]),
            ss_release_units=float(row["ss_release_units"]),
            notes=row.get("notes", ""),
        )


@dataclass(frozen=True)
class Phase5FinancialDetailInputRecord:
    scenario_name: str
    geography_code: str
    module: str
    month_index: int
    calendar_month: date
    material_node: str
    issues: float
    available_nonexpired_inventory: float
    expired_quantity: float
    matched_administrable_fg_units: float
    fg_ss_mismatch_units: float
    stockout_flag: bool
    excess_inventory_flag: bool
    expiry_flag: bool
    fg_ss_mismatch_flag: bool
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

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "Phase5FinancialDetailInputRecord":
        return cls(
            scenario_name=row["scenario_name"],
            geography_code=row["geography_code"],
            module=row["module"],
            month_index=int(row["month_index"]),
            calendar_month=date.fromisoformat(row["calendar_month"]),
            material_node=row["material_node"],
            issues=float(row.get("issues", 0.0)),
            available_nonexpired_inventory=float(row["available_nonexpired_inventory"]),
            expired_quantity=float(row["expired_quantity"]),
            matched_administrable_fg_units=float(row.get("matched_administrable_fg_units", 0.0)),
            fg_ss_mismatch_units=float(row.get("fg_ss_mismatch_units", 0.0)),
            stockout_flag=row.get("stockout_flag", "false").lower() == "true",
            excess_inventory_flag=row.get("excess_inventory_flag", "false").lower() == "true",
            expiry_flag=row.get("expiry_flag", "false").lower() == "true",
            fg_ss_mismatch_flag=row.get("fg_ss_mismatch_flag", "false").lower() == "true",
            notes=row.get("notes", ""),
        )


@dataclass(frozen=True)
class Phase5FinancialSummaryInputRecord:
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

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "Phase5FinancialSummaryInputRecord":
        return cls(
            scenario_name=row["scenario_name"],
            geography_code=row["geography_code"],
            module=row["module"],
            month_index=int(row["month_index"]),
            calendar_month=date.fromisoformat(row["calendar_month"]),
            ds_inventory_mg=float(row["ds_inventory_mg"]),
            dp_inventory_units=float(row["dp_inventory_units"]),
            fg_inventory_units=float(row["fg_inventory_units"]),
            ss_inventory_units=float(row["ss_inventory_units"]),
            sublayer1_fg_inventory_units=float(row["sublayer1_fg_inventory_units"]),
            sublayer2_fg_inventory_units=float(row["sublayer2_fg_inventory_units"]),
            expired_ds_mg=float(row["expired_ds_mg"]),
            expired_dp_units=float(row["expired_dp_units"]),
            expired_fg_units=float(row["expired_fg_units"]),
            expired_ss_units=float(row["expired_ss_units"]),
            unmatched_fg_units=float(row["unmatched_fg_units"]),
            matched_administrable_fg_units=float(row["matched_administrable_fg_units"]),
            stockout_flag=row.get("stockout_flag", "false").lower() == "true",
            excess_inventory_flag=row.get("excess_inventory_flag", "false").lower() == "true",
            expiry_flag=row.get("expiry_flag", "false").lower() == "true",
            fg_ss_mismatch_flag=row.get("fg_ss_mismatch_flag", "false").lower() == "true",
            notes=row.get("notes", ""),
        )


@dataclass(frozen=True)
class FinancialDetailRecord:
    scenario_name: str
    geography_code: str
    module: str
    month_index: int
    calendar_month: date
    financial_node_or_stage: str
    quantity_basis: str
    quantity_value: float
    standard_cost_rate: float
    shipment_quantity_basis_units: float
    shipping_cold_chain_cost_rate: float
    shipping_cold_chain_cost_value: float
    inventory_value: float
    release_value: float
    expired_value: float
    carrying_cost_value: float
    matched_administrable_fg_value: float
    unmatched_fg_value_at_risk: float
    notes: str

    @property
    def key(self) -> tuple[str, str, str, int, str]:
        return (
            self.scenario_name,
            self.geography_code,
            self.module,
            self.month_index,
            self.financial_node_or_stage,
        )

    def as_csv_row(self) -> dict[str, object]:
        return {
            "scenario_name": self.scenario_name,
            "geography_code": self.geography_code,
            "module": self.module,
            "month_index": self.month_index,
            "calendar_month": self.calendar_month.isoformat(),
            "financial_node_or_stage": self.financial_node_or_stage,
            "quantity_basis": self.quantity_basis,
            "quantity_value": self.quantity_value,
            "standard_cost_rate": self.standard_cost_rate,
            "shipment_quantity_basis_units": self.shipment_quantity_basis_units,
            "shipping_cold_chain_cost_rate": self.shipping_cold_chain_cost_rate,
            "shipping_cold_chain_cost_value": self.shipping_cold_chain_cost_value,
            "inventory_value": self.inventory_value,
            "release_value": self.release_value,
            "expired_value": self.expired_value,
            "carrying_cost_value": self.carrying_cost_value,
            "matched_administrable_fg_value": self.matched_administrable_fg_value,
            "unmatched_fg_value_at_risk": self.unmatched_fg_value_at_risk,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class FinancialMonthlySummaryRecord:
    scenario_name: str
    geography_code: str
    module: str
    month_index: int
    calendar_month: date
    ds_inventory_value: float
    dp_inventory_value: float
    fg_inventory_value: float
    ss_inventory_value: float
    sublayer1_fg_inventory_value: float
    sublayer2_fg_inventory_value: float
    total_inventory_value: float
    ds_release_value: float
    dp_release_value: float
    fg_release_value: float
    ss_release_value: float
    total_release_value: float
    fg_shipping_cold_chain_cost: float
    ss_shipping_cold_chain_cost: float
    total_shipping_cold_chain_cost: float
    expired_ds_value: float
    expired_dp_value: float
    expired_fg_value: float
    expired_ss_value: float
    expired_value_total: float
    ds_carrying_cost: float
    dp_carrying_cost: float
    fg_carrying_cost: float
    ss_carrying_cost: float
    trade_node_fg_carrying_cost: float
    carrying_cost_total: float
    matched_administrable_fg_value: float
    unmatched_fg_value_at_risk: float
    stockout_flag: bool
    excess_inventory_flag: bool
    expiry_flag: bool
    fg_ss_mismatch_flag: bool
    notes: str

    @property
    def key(self) -> tuple[str, str, str, int]:
        return (self.scenario_name, self.geography_code, self.module, self.month_index)

    def as_csv_row(self) -> dict[str, object]:
        return {
            "scenario_name": self.scenario_name,
            "geography_code": self.geography_code,
            "module": self.module,
            "month_index": self.month_index,
            "calendar_month": self.calendar_month.isoformat(),
            "ds_inventory_value": self.ds_inventory_value,
            "dp_inventory_value": self.dp_inventory_value,
            "fg_inventory_value": self.fg_inventory_value,
            "ss_inventory_value": self.ss_inventory_value,
            "sublayer1_fg_inventory_value": self.sublayer1_fg_inventory_value,
            "sublayer2_fg_inventory_value": self.sublayer2_fg_inventory_value,
            "total_inventory_value": self.total_inventory_value,
            "ds_release_value": self.ds_release_value,
            "dp_release_value": self.dp_release_value,
            "fg_release_value": self.fg_release_value,
            "ss_release_value": self.ss_release_value,
            "total_release_value": self.total_release_value,
            "fg_shipping_cold_chain_cost": self.fg_shipping_cold_chain_cost,
            "ss_shipping_cold_chain_cost": self.ss_shipping_cold_chain_cost,
            "total_shipping_cold_chain_cost": self.total_shipping_cold_chain_cost,
            "expired_ds_value": self.expired_ds_value,
            "expired_dp_value": self.expired_dp_value,
            "expired_fg_value": self.expired_fg_value,
            "expired_ss_value": self.expired_ss_value,
            "expired_value_total": self.expired_value_total,
            "ds_carrying_cost": self.ds_carrying_cost,
            "dp_carrying_cost": self.dp_carrying_cost,
            "fg_carrying_cost": self.fg_carrying_cost,
            "ss_carrying_cost": self.ss_carrying_cost,
            "trade_node_fg_carrying_cost": self.trade_node_fg_carrying_cost,
            "carrying_cost_total": self.carrying_cost_total,
            "matched_administrable_fg_value": self.matched_administrable_fg_value,
            "unmatched_fg_value_at_risk": self.unmatched_fg_value_at_risk,
            "stockout_flag": self.stockout_flag,
            "excess_inventory_flag": self.excess_inventory_flag,
            "expiry_flag": self.expiry_flag,
            "fg_ss_mismatch_flag": self.fg_ss_mismatch_flag,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class FinancialAnnualSummaryRecord:
    scenario_name: str
    geography_code: str
    module: str
    calendar_year: int
    ending_total_inventory_value: float
    total_release_value: float
    total_shipping_cold_chain_cost: float
    total_expired_value: float
    total_carrying_cost: float
    ending_matched_administrable_fg_value: float
    ending_unmatched_fg_value_at_risk: float
    stockout_month_count: int
    excess_inventory_month_count: int
    expiry_month_count: int
    fg_ss_mismatch_month_count: int
    notes: str

    @property
    def key(self) -> tuple[str, str, str, int]:
        return (self.scenario_name, self.geography_code, self.module, self.calendar_year)

    def as_csv_row(self) -> dict[str, object]:
        return {
            "scenario_name": self.scenario_name,
            "geography_code": self.geography_code,
            "module": self.module,
            "calendar_year": self.calendar_year,
            "ending_total_inventory_value": self.ending_total_inventory_value,
            "total_release_value": self.total_release_value,
            "total_shipping_cold_chain_cost": self.total_shipping_cold_chain_cost,
            "total_expired_value": self.total_expired_value,
            "total_carrying_cost": self.total_carrying_cost,
            "ending_matched_administrable_fg_value": self.ending_matched_administrable_fg_value,
            "ending_unmatched_fg_value_at_risk": self.ending_unmatched_fg_value_at_risk,
            "stockout_month_count": self.stockout_month_count,
            "excess_inventory_month_count": self.excess_inventory_month_count,
            "expiry_month_count": self.expiry_month_count,
            "fg_ss_mismatch_month_count": self.fg_ss_mismatch_month_count,
            "notes": self.notes,
        }
