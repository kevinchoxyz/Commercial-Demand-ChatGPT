"""Deterministic Phase 6 financial/value logic."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from .config_schema import Phase6Config
from .schemas import (
    FinancialAnnualSummaryRecord,
    FinancialDetailRecord,
    FinancialMonthlySummaryRecord,
    Phase4FinancialInputRecord,
    Phase5FinancialDetailInputRecord,
    Phase5FinancialSummaryInputRecord,
)


DS_NODE = "DS"
DP_NODE = "DP"
FG_CENTRAL_NODE = "FG_Central"
SS_CENTRAL_NODE = "SS_Central"
SUBLAYER1_NODE = "SubLayer1_FG"
SUBLAYER2_NODE = "SubLayer2_FG"


@dataclass(frozen=True)
class StandardCostModel:
    config: Phase6Config

    @property
    def ds_standard_cost_per_mg(self) -> float:
        return self.config.cost_basis.ds_standard_cost_per_mg

    @property
    def dp_standard_cost_per_unit(self) -> float:
        ds_input_cost = (
            self.config.conversion.ds_qty_per_dp_unit_mg
            / self.config.conversion.ds_to_dp_yield
            * (1.0 + self.config.conversion.ds_overage_factor)
            * self.ds_standard_cost_per_mg
        )
        return ds_input_cost + self.config.cost_basis.dp_conversion_cost_per_unit

    def fg_packaging_labeling_cost_per_unit(self, geography_code: str) -> float:
        return self.config.cost_basis.geography_fg_packaging_labeling_cost_overrides.get(
            geography_code,
            self.config.cost_basis.fg_packaging_labeling_cost_per_unit,
        )

    def fg_standard_cost_per_unit(self, geography_code: str) -> float:
        return (
            self.dp_standard_cost_per_unit / self.config.conversion.dp_to_fg_yield
            + self.fg_packaging_labeling_cost_per_unit(geography_code)
        )

    @property
    def ss_standard_cost_per_unit(self) -> float:
        return self.config.cost_basis.ss_standard_cost_per_unit

    @property
    def monthly_carry_rate(self) -> float:
        return self.config.carrying_cost.monthly_inventory_carry_rate

    @property
    def expiry_writeoff_multiplier(self) -> float:
        return (
            self.config.expiry_writeoff.expired_inventory_writeoff_rate
            * (1.0 - self.config.expiry_writeoff.expired_inventory_salvage_rate)
        )


def build_phase6_outputs(
    config: Phase6Config,
    phase4_monthly_summary: tuple[Phase4FinancialInputRecord, ...],
    phase5_inventory_detail: tuple[Phase5FinancialDetailInputRecord, ...],
    phase5_monthly_summary: tuple[Phase5FinancialSummaryInputRecord, ...],
) -> tuple[
    tuple[FinancialDetailRecord, ...],
    tuple[FinancialMonthlySummaryRecord, ...],
    tuple[FinancialAnnualSummaryRecord, ...],
]:
    cost_model = StandardCostModel(config)
    detail_records = _build_detail_records(cost_model, phase4_monthly_summary, phase5_inventory_detail)
    monthly_summary = _build_monthly_summary(cost_model, phase4_monthly_summary, phase5_monthly_summary)
    annual_summary = _build_annual_summary(monthly_summary)
    return tuple(detail_records), tuple(monthly_summary), tuple(annual_summary)


def _build_detail_records(
    cost_model: StandardCostModel,
    phase4_monthly_summary: tuple[Phase4FinancialInputRecord, ...],
    phase5_inventory_detail: tuple[Phase5FinancialDetailInputRecord, ...],
) -> list[FinancialDetailRecord]:
    records: list[FinancialDetailRecord] = []
    for row in phase5_inventory_detail:
        standard_cost_rate = _inventory_standard_cost_rate(cost_model, row.material_node, row.geography_code)
        inventory_value = _inventory_value(
            cost_model,
            row.material_node,
            row.geography_code,
            row.available_nonexpired_inventory,
        )
        expired_value = _inventory_value(
            cost_model,
            row.material_node,
            row.geography_code,
            row.expired_quantity,
        ) * cost_model.expiry_writeoff_multiplier
        carrying_cost_value = inventory_value * cost_model.monthly_carry_rate
        fg_cost_rate = cost_model.fg_standard_cost_per_unit(row.geography_code)
        matched_value = (
            row.matched_administrable_fg_units * fg_cost_rate
            if row.material_node == FG_CENTRAL_NODE and cost_model.config.valuation_policy.use_matched_administrable_fg_value
            else 0.0
        )
        unmatched_value = (
            row.fg_ss_mismatch_units * fg_cost_rate
            if row.material_node == FG_CENTRAL_NODE and cost_model.config.valuation_policy.value_unmatched_fg_at_fg_standard_cost
            else 0.0
        )
        records.append(
            FinancialDetailRecord(
                scenario_name=row.scenario_name,
                geography_code=row.geography_code,
                module=row.module,
                month_index=row.month_index,
                calendar_month=row.calendar_month,
                financial_node_or_stage=row.material_node,
                quantity_basis="mg" if row.material_node == DS_NODE else "units",
                quantity_value=row.available_nonexpired_inventory,
                standard_cost_rate=standard_cost_rate,
                inventory_value=inventory_value,
                release_value=0.0,
                expired_value=expired_value,
                carrying_cost_value=carrying_cost_value,
                matched_administrable_fg_value=matched_value,
                unmatched_fg_value_at_risk=unmatched_value,
                notes=_join_notes(
                    row.notes,
                    "Inventory value is deterministic ending non-expired inventory value at stage/node standard cost.",
                ),
            )
        )

    for row in phase4_monthly_summary:
        stage_rows = (
            ("DS_Release", "mg", row.ds_release_quantity_mg, cost_model.ds_standard_cost_per_mg),
            ("DP_Release", "units", row.dp_release_units, cost_model.dp_standard_cost_per_unit),
            ("FG_Release", "units", row.fg_release_units, cost_model.fg_standard_cost_per_unit(row.geography_code)),
            ("SS_Release", "units", row.ss_release_units, cost_model.ss_standard_cost_per_unit),
        )
        for stage_name, quantity_basis, quantity_value, standard_cost_rate in stage_rows:
            records.append(
                FinancialDetailRecord(
                    scenario_name=row.scenario_name,
                    geography_code=row.geography_code,
                    module=row.module,
                    month_index=row.month_index,
                    calendar_month=row.calendar_month,
                    financial_node_or_stage=stage_name,
                    quantity_basis=quantity_basis,
                    quantity_value=quantity_value,
                    standard_cost_rate=standard_cost_rate,
                    inventory_value=0.0,
                    release_value=quantity_value * standard_cost_rate,
                    expired_value=0.0,
                    carrying_cost_value=0.0,
                    matched_administrable_fg_value=0.0,
                    unmatched_fg_value_at_risk=0.0,
                    notes=_join_notes(
                        row.notes,
                        "Release value is deterministic stage release quantity valued at the configured stage standard cost.",
                    ),
                )
            )
    records.sort(
        key=lambda item: (
            item.scenario_name,
            item.geography_code,
            item.module,
            item.month_index,
            item.financial_node_or_stage,
        )
    )
    return records


def _build_monthly_summary(
    cost_model: StandardCostModel,
    phase4_monthly_summary: tuple[Phase4FinancialInputRecord, ...],
    phase5_monthly_summary: tuple[Phase5FinancialSummaryInputRecord, ...],
) -> list[FinancialMonthlySummaryRecord]:
    phase4_by_key = {row.key: row for row in phase4_monthly_summary}
    phase5_by_key = {row.key: row for row in phase5_monthly_summary}
    keys = sorted(set(phase4_by_key) | set(phase5_by_key))

    summaries: list[FinancialMonthlySummaryRecord] = []
    for key in keys:
        phase4_row = phase4_by_key.get(key)
        phase5_row = phase5_by_key.get(key)
        geography_code = phase5_row.geography_code if phase5_row is not None else phase4_row.geography_code
        scenario_name = phase5_row.scenario_name if phase5_row is not None else phase4_row.scenario_name
        module = phase5_row.module if phase5_row is not None else phase4_row.module
        month_index = phase5_row.month_index if phase5_row is not None else phase4_row.month_index
        calendar_month = phase5_row.calendar_month if phase5_row is not None else phase4_row.calendar_month

        fg_cost_rate = cost_model.fg_standard_cost_per_unit(geography_code)
        ds_inventory_value = (
            phase5_row.ds_inventory_mg * cost_model.ds_standard_cost_per_mg if phase5_row is not None else 0.0
        )
        dp_inventory_value = (
            phase5_row.dp_inventory_units * cost_model.dp_standard_cost_per_unit if phase5_row is not None else 0.0
        )
        fg_inventory_value = phase5_row.fg_inventory_units * fg_cost_rate if phase5_row is not None else 0.0
        ss_inventory_value = (
            phase5_row.ss_inventory_units * cost_model.ss_standard_cost_per_unit if phase5_row is not None else 0.0
        )
        if phase5_row is None or not cost_model.config.valuation_policy.include_trade_node_fg_value:
            sublayer1_fg_inventory_value = 0.0
            sublayer2_fg_inventory_value = 0.0
        else:
            sublayer1_fg_inventory_value = phase5_row.sublayer1_fg_inventory_units * fg_cost_rate
            sublayer2_fg_inventory_value = phase5_row.sublayer2_fg_inventory_units * fg_cost_rate
        total_inventory_value = (
            ds_inventory_value
            + dp_inventory_value
            + fg_inventory_value
            + ss_inventory_value
            + sublayer1_fg_inventory_value
            + sublayer2_fg_inventory_value
        )

        ds_release_value = (
            phase4_row.ds_release_quantity_mg * cost_model.ds_standard_cost_per_mg if phase4_row is not None else 0.0
        )
        dp_release_value = (
            phase4_row.dp_release_units * cost_model.dp_standard_cost_per_unit if phase4_row is not None else 0.0
        )
        fg_release_value = phase4_row.fg_release_units * fg_cost_rate if phase4_row is not None else 0.0
        ss_release_value = (
            phase4_row.ss_release_units * cost_model.ss_standard_cost_per_unit if phase4_row is not None else 0.0
        )
        total_release_value = fg_release_value + ss_release_value

        expired_ds_value = (
            phase5_row.expired_ds_mg
            * cost_model.ds_standard_cost_per_mg
            * cost_model.expiry_writeoff_multiplier
            if phase5_row is not None
            else 0.0
        )
        expired_dp_value = (
            phase5_row.expired_dp_units
            * cost_model.dp_standard_cost_per_unit
            * cost_model.expiry_writeoff_multiplier
            if phase5_row is not None
            else 0.0
        )
        expired_fg_value = (
            phase5_row.expired_fg_units * fg_cost_rate * cost_model.expiry_writeoff_multiplier
            if phase5_row is not None
            else 0.0
        )
        expired_ss_value = (
            phase5_row.expired_ss_units
            * cost_model.ss_standard_cost_per_unit
            * cost_model.expiry_writeoff_multiplier
            if phase5_row is not None
            else 0.0
        )
        expired_value_total = expired_ds_value + expired_dp_value + expired_fg_value + expired_ss_value

        ds_carrying_cost = ds_inventory_value * cost_model.monthly_carry_rate
        dp_carrying_cost = dp_inventory_value * cost_model.monthly_carry_rate
        fg_carrying_cost = fg_inventory_value * cost_model.monthly_carry_rate
        ss_carrying_cost = ss_inventory_value * cost_model.monthly_carry_rate
        trade_node_fg_carrying_cost = (
            (sublayer1_fg_inventory_value + sublayer2_fg_inventory_value) * cost_model.monthly_carry_rate
        )
        carrying_cost_total = (
            ds_carrying_cost
            + dp_carrying_cost
            + fg_carrying_cost
            + ss_carrying_cost
            + trade_node_fg_carrying_cost
        )

        if phase5_row is None:
            matched_administrable_fg_value = 0.0
            unmatched_fg_value_at_risk = 0.0
            stockout_flag = False
            excess_inventory_flag = False
            expiry_flag = False
            fg_ss_mismatch_flag = False
            notes = ""
        else:
            matched_quantity = (
                phase5_row.matched_administrable_fg_units
                if cost_model.config.valuation_policy.use_matched_administrable_fg_value
                else phase5_row.fg_inventory_units
            )
            matched_administrable_fg_value = matched_quantity * fg_cost_rate
            unmatched_fg_value_at_risk = (
                phase5_row.unmatched_fg_units * fg_cost_rate
                if cost_model.config.valuation_policy.value_unmatched_fg_at_fg_standard_cost
                else 0.0
            )
            stockout_flag = phase5_row.stockout_flag
            excess_inventory_flag = phase5_row.excess_inventory_flag
            expiry_flag = phase5_row.expiry_flag
            fg_ss_mismatch_flag = phase5_row.fg_ss_mismatch_flag
            notes = phase5_row.notes

        summaries.append(
            FinancialMonthlySummaryRecord(
                scenario_name=scenario_name,
                geography_code=geography_code,
                module=module,
                month_index=month_index,
                calendar_month=calendar_month,
                ds_inventory_value=ds_inventory_value,
                dp_inventory_value=dp_inventory_value,
                fg_inventory_value=fg_inventory_value,
                ss_inventory_value=ss_inventory_value,
                sublayer1_fg_inventory_value=sublayer1_fg_inventory_value,
                sublayer2_fg_inventory_value=sublayer2_fg_inventory_value,
                total_inventory_value=total_inventory_value,
                ds_release_value=ds_release_value,
                dp_release_value=dp_release_value,
                fg_release_value=fg_release_value,
                ss_release_value=ss_release_value,
                total_release_value=total_release_value,
                expired_ds_value=expired_ds_value,
                expired_dp_value=expired_dp_value,
                expired_fg_value=expired_fg_value,
                expired_ss_value=expired_ss_value,
                expired_value_total=expired_value_total,
                ds_carrying_cost=ds_carrying_cost,
                dp_carrying_cost=dp_carrying_cost,
                fg_carrying_cost=fg_carrying_cost,
                ss_carrying_cost=ss_carrying_cost,
                trade_node_fg_carrying_cost=trade_node_fg_carrying_cost,
                carrying_cost_total=carrying_cost_total,
                matched_administrable_fg_value=matched_administrable_fg_value,
                unmatched_fg_value_at_risk=unmatched_fg_value_at_risk,
                stockout_flag=stockout_flag,
                excess_inventory_flag=excess_inventory_flag,
                expiry_flag=expiry_flag,
                fg_ss_mismatch_flag=fg_ss_mismatch_flag,
                notes=_join_notes(
                    notes,
                    "total_release_value includes FG and SS release value only to avoid DS/DP stage double counting.",
                ),
            )
        )

    summaries.sort(key=lambda item: (item.scenario_name, item.geography_code, item.module, item.month_index))
    return summaries


def _build_annual_summary(
    monthly_summary: list[FinancialMonthlySummaryRecord],
) -> list[FinancialAnnualSummaryRecord]:
    grouped: dict[tuple[str, str, str, int], list[FinancialMonthlySummaryRecord]] = defaultdict(list)
    for row in monthly_summary:
        grouped[(row.scenario_name, row.geography_code, row.module, row.calendar_month.year)].append(row)

    annual_rows: list[FinancialAnnualSummaryRecord] = []
    for key, rows in sorted(grouped.items()):
        ordered_rows = sorted(rows, key=lambda item: item.month_index)
        latest_row = ordered_rows[-1]
        annual_rows.append(
            FinancialAnnualSummaryRecord(
                scenario_name=key[0],
                geography_code=key[1],
                module=key[2],
                calendar_year=key[3],
                ending_total_inventory_value=latest_row.total_inventory_value,
                total_release_value=sum(item.total_release_value for item in ordered_rows),
                total_expired_value=sum(item.expired_value_total for item in ordered_rows),
                total_carrying_cost=sum(item.carrying_cost_total for item in ordered_rows),
                ending_matched_administrable_fg_value=latest_row.matched_administrable_fg_value,
                ending_unmatched_fg_value_at_risk=latest_row.unmatched_fg_value_at_risk,
                stockout_month_count=sum(1 for item in ordered_rows if item.stockout_flag),
                excess_inventory_month_count=sum(1 for item in ordered_rows if item.excess_inventory_flag),
                expiry_month_count=sum(1 for item in ordered_rows if item.expiry_flag),
                fg_ss_mismatch_month_count=sum(1 for item in ordered_rows if item.fg_ss_mismatch_flag),
                notes="Annual rollup uses end-of-year inventory value and summed monthly release/expiry/carrying values.",
            )
        )
    return annual_rows


def _inventory_standard_cost_rate(
    cost_model: StandardCostModel,
    material_node: str,
    geography_code: str,
) -> float:
    if material_node == DS_NODE:
        return cost_model.ds_standard_cost_per_mg
    if material_node == DP_NODE:
        return cost_model.dp_standard_cost_per_unit
    if material_node in (FG_CENTRAL_NODE, SUBLAYER1_NODE, SUBLAYER2_NODE):
        return cost_model.fg_standard_cost_per_unit(geography_code)
    if material_node == SS_CENTRAL_NODE:
        return cost_model.ss_standard_cost_per_unit
    raise ValueError(f"Unsupported material_node for Phase 6 valuation: {material_node}")


def _inventory_value(
    cost_model: StandardCostModel,
    material_node: str,
    geography_code: str,
    quantity: float,
) -> float:
    if material_node in (SUBLAYER1_NODE, SUBLAYER2_NODE) and not cost_model.config.valuation_policy.include_trade_node_fg_value:
        return 0.0
    return quantity * _inventory_standard_cost_rate(cost_model, material_node, geography_code)


def _join_notes(*parts: str) -> str:
    return " | ".join(part for part in parts if part)
