"""Deterministic Phase 3 trade layer."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from json import dumps
from math import ceil

from .config_schema import GeographyTradeConfig, LaunchEventConfig, Phase3Config
from .schemas import Phase2TradeInputRecord, Phase3TradeRecord


@dataclass(frozen=True)
class ModuleMonthTradeTotals:
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
    sublayer2_inventory_on_hand_end_units: float
    sublayer1_inventory_on_hand_end_units: float
    january_softening_applied: bool


def build_phase3_outputs(
    config: Phase3Config,
    phase2_rows: tuple[Phase2TradeInputRecord, ...],
) -> tuple[Phase3TradeRecord, ...]:
    grouped_rows: dict[tuple[str, str, str], list[Phase2TradeInputRecord]] = defaultdict(list)
    for row in phase2_rows:
        grouped_rows[(row.scenario_name, row.geography_code, row.module)].append(row)

    outputs: list[Phase3TradeRecord] = []
    trade_parameters_used = dumps(
        {
            "sublayer1_target_weeks_on_hand": config.trade.sublayer1_target_weeks_on_hand,
            "sublayer2_target_weeks_on_hand": config.trade.sublayer2_target_weeks_on_hand,
            "sublayer2_wastage_rate": config.trade.sublayer2_wastage_rate,
            "initial_stocking_units_per_new_site": config.trade.initial_stocking_units_per_new_site,
            "ss_units_per_new_site": config.trade.ss_units_per_new_site,
            "sublayer1_launch_fill_months_of_demand": config.trade.sublayer1_launch_fill_months_of_demand,
            "rems_certification_lag_weeks": config.trade.rems_certification_lag_weeks,
            "january_softening_enabled": config.trade.january_softening_enabled,
            "january_softening_factor": config.trade.january_softening_factor,
            "bullwhip_flag_threshold": config.trade.bullwhip_flag_threshold,
            "channel_fill_start_prelaunch_weeks": config.trade.channel_fill_start_prelaunch_weeks,
            "sublayer2_fill_distribution_weeks": config.trade.sublayer2_fill_distribution_weeks,
        },
        sort_keys=True,
    )

    for (scenario_name, geography_code, module), rows in sorted(grouped_rows.items()):
        geography_defaults = config.get_geography_defaults(geography_code)
        launch_event = config.get_launch_event(module, geography_code)
        outputs.extend(
            _build_group_outputs(
                config=config,
                scenario_name=scenario_name,
                geography_code=geography_code,
                module=module,
                rows=tuple(sorted(rows, key=lambda item: (item.month_index, item.segment_code))),
                geography_defaults=geography_defaults,
                launch_event=launch_event,
                trade_parameters_used=trade_parameters_used,
            )
        )
    return tuple(outputs)


def _build_group_outputs(
    *,
    config: Phase3Config,
    scenario_name: str,
    geography_code: str,
    module: str,
    rows: tuple[Phase2TradeInputRecord, ...],
    geography_defaults: GeographyTradeConfig,
    launch_event: LaunchEventConfig,
    trade_parameters_used: str,
) -> list[Phase3TradeRecord]:
    rows_by_month = _index_rows_by_month(rows)
    segment_codes = tuple(sorted({row.segment_code for row in rows}))
    if not segment_codes:
        return []

    month_to_calendar = _build_calendar_map(rows)
    patient_fg_demand_by_month = {
        month_index: sum(row.patient_fg_demand_units for row in month_rows)
        for month_index, month_rows in rows_by_month.items()
    }
    fill_schedule = _build_launch_fill_schedule(
        config=config,
        patient_fg_demand_by_month=patient_fg_demand_by_month,
        launch_month_index=launch_event.launch_month_index,
    )
    start_month_index = min(min(rows_by_month), min(fill_schedule) if fill_schedule else min(rows_by_month))
    end_month_index = max(max(rows_by_month), max(fill_schedule) if fill_schedule else max(rows_by_month))
    months = range(start_month_index, end_month_index + 1)

    active_sites_by_month, new_sites_by_month = _build_site_activation_schedule(
        config=config,
        geography_defaults=geography_defaults,
        launch_event=launch_event,
        months=months,
    )
    allocation_weights = _build_allocation_weights(
        rows_by_month=rows_by_month,
        segment_codes=segment_codes,
        months=months,
        lookahead_months=max(
            1,
            int(
                ceil(
                    config.trade.sublayer2_fill_distribution_weeks
                    / config.trade.weeks_per_month
                )
            ),
        ),
    )

    state_sublayer2_inventory = 0.0
    state_sublayer1_inventory = 0.0
    group_outputs: list[Phase3TradeRecord] = []
    for month_index in months:
        calendar_month = month_to_calendar.get(month_index)
        if calendar_month is None:
            calendar_month = _derive_calendar_month(rows[0], month_index)
            month_to_calendar[month_index] = calendar_month
        patient_fg_demand_units = patient_fg_demand_by_month.get(month_index, 0.0)
        sublayer2_wastage_units = patient_fg_demand_units * config.trade.sublayer2_wastage_rate
        demand_consumption_units = patient_fg_demand_units + sublayer2_wastage_units
        sublayer2_inventory_target_units = _target_inventory_units(
            target_weeks_on_hand=config.trade.sublayer2_target_weeks_on_hand,
            weekly_demand_units=demand_consumption_units / config.trade.weeks_per_month,
            active_certified_sites=active_sites_by_month[month_index],
        )
        net_sublayer2_inventory_after_demand = (
            state_sublayer2_inventory - demand_consumption_units
        )
        sublayer2_inventory_adjustment_units = max(
            sublayer2_inventory_target_units - net_sublayer2_inventory_after_demand,
            0.0,
        )
        new_site_stocking_orders_units = (
            new_sites_by_month[month_index] * config.trade.initial_stocking_units_per_new_site
        )
        ss_site_stocking_units = (
            new_sites_by_month[month_index] * config.trade.ss_units_per_new_site
        )
        sublayer2_pull_units = (
            sublayer2_inventory_adjustment_units + new_site_stocking_orders_units
        )
        state_sublayer2_inventory = (
            state_sublayer2_inventory
            + sublayer2_pull_units
            - demand_consumption_units
        )

        sublayer1_inventory_target_units = _target_inventory_units(
            target_weeks_on_hand=config.trade.sublayer1_target_weeks_on_hand,
            weekly_demand_units=sublayer2_pull_units / config.trade.weeks_per_month,
            active_certified_sites=1.0 if sublayer2_pull_units > 0 else 0.0,
        )
        net_sublayer1_inventory_after_pull = state_sublayer1_inventory - sublayer2_pull_units
        base_sublayer1_inventory_adjustment_units = max(
            sublayer1_inventory_target_units - net_sublayer1_inventory_after_pull,
            0.0,
        )
        january_softening_applied = (
            config.trade.january_softening_enabled and calendar_month.month == 1
        )
        if january_softening_applied:
            sublayer1_inventory_adjustment_units = (
                base_sublayer1_inventory_adjustment_units * config.trade.january_softening_factor
            )
        else:
            sublayer1_inventory_adjustment_units = base_sublayer1_inventory_adjustment_units
        launch_fill_component_units = fill_schedule.get(month_index, 0.0)
        ongoing_replenishment_component_units = sublayer1_inventory_adjustment_units
        ex_factory_fg_demand_units = (
            launch_fill_component_units + ongoing_replenishment_component_units
        )
        state_sublayer1_inventory = (
            state_sublayer1_inventory
            + ex_factory_fg_demand_units
            - sublayer2_pull_units
        )
        bullwhip_amplification_factor = _bullwhip_factor(
            patient_fg_demand_units=patient_fg_demand_units,
            ex_factory_fg_demand_units=ex_factory_fg_demand_units,
        )
        bullwhip_flag = _bullwhip_flag(
            patient_fg_demand_units=patient_fg_demand_units,
            ex_factory_fg_demand_units=ex_factory_fg_demand_units,
            threshold=config.trade.bullwhip_flag_threshold,
        )
        totals = ModuleMonthTradeTotals(
            calendar_month=calendar_month,
            patient_fg_demand_units=patient_fg_demand_units,
            sublayer2_wastage_units=sublayer2_wastage_units,
            sublayer2_inventory_target_units=sublayer2_inventory_target_units,
            sublayer2_inventory_adjustment_units=sublayer2_inventory_adjustment_units,
            new_site_stocking_orders_units=new_site_stocking_orders_units,
            ss_site_stocking_units=ss_site_stocking_units,
            sublayer2_pull_units=sublayer2_pull_units,
            sublayer1_inventory_target_units=sublayer1_inventory_target_units,
            sublayer1_inventory_adjustment_units=sublayer1_inventory_adjustment_units,
            ex_factory_fg_demand_units=ex_factory_fg_demand_units,
            bullwhip_amplification_factor=bullwhip_amplification_factor,
            bullwhip_flag=bullwhip_flag,
            launch_fill_component_units=launch_fill_component_units,
            ongoing_replenishment_component_units=ongoing_replenishment_component_units,
            active_certified_sites=active_sites_by_month[month_index],
            new_certified_sites=new_sites_by_month[month_index],
            sublayer2_inventory_on_hand_end_units=state_sublayer2_inventory,
            sublayer1_inventory_on_hand_end_units=state_sublayer1_inventory,
            january_softening_applied=january_softening_applied,
        )
        group_outputs.extend(
            _allocate_group_month_to_segments(
                totals=totals,
                allocation_weights=allocation_weights[month_index],
                scenario_name=scenario_name,
                geography_code=geography_code,
                module=module,
                month_index=month_index,
                segment_codes=segment_codes,
                rows_by_month=rows_by_month,
                trade_parameters_used=trade_parameters_used,
            )
        )
    return group_outputs


def _index_rows_by_month(
    rows: tuple[Phase2TradeInputRecord, ...]
) -> dict[int, tuple[Phase2TradeInputRecord, ...]]:
    indexed: dict[int, list[Phase2TradeInputRecord]] = defaultdict(list)
    for row in rows:
        indexed[row.month_index].append(row)
    return {
        month_index: tuple(sorted(month_rows, key=lambda item: item.segment_code))
        for month_index, month_rows in indexed.items()
    }


def _build_calendar_map(rows: tuple[Phase2TradeInputRecord, ...]) -> dict[int, date]:
    return {row.month_index: row.calendar_month for row in rows}


def _derive_calendar_month(anchor_row: Phase2TradeInputRecord, month_index: int) -> date:
    return _add_months(anchor_row.calendar_month, month_index - anchor_row.month_index)


def _add_months(value: date, month_delta: int) -> date:
    year = value.year + (value.month - 1 + month_delta) // 12
    month = (value.month - 1 + month_delta) % 12 + 1
    return date(year, month, 1)


def _build_site_activation_schedule(
    *,
    config: Phase3Config,
    geography_defaults: GeographyTradeConfig,
    launch_event: LaunchEventConfig,
    months: range,
) -> tuple[dict[int, float], dict[int, float]]:
    lag_months = int(ceil(config.trade.rems_certification_lag_weeks / config.trade.weeks_per_month))
    first_active_month = launch_event.launch_month_index + lag_months
    active_sites_by_month: dict[int, float] = {}
    new_sites_by_month: dict[int, float] = {}
    prior_active_sites = 0.0
    for month_index in months:
        if month_index < first_active_month:
            active_sites = prior_active_sites
        elif month_index == first_active_month:
            active_sites = geography_defaults.certified_sites_at_launch
        else:
            active_sites = min(
                geography_defaults.certified_sites_at_peak,
                prior_active_sites + geography_defaults.site_activation_rate,
            )
        new_sites = max(active_sites - prior_active_sites, 0.0)
        active_sites_by_month[month_index] = active_sites
        new_sites_by_month[month_index] = new_sites
        prior_active_sites = active_sites
    return active_sites_by_month, new_sites_by_month


def _build_launch_fill_schedule(
    *,
    config: Phase3Config,
    patient_fg_demand_by_month: dict[int, float],
    launch_month_index: int,
) -> dict[int, float]:
    if config.trade.sublayer1_launch_fill_months_of_demand == 0:
        return {}
    prelaunch_months = int(
        ceil(config.trade.channel_fill_start_prelaunch_weeks / config.trade.weeks_per_month)
    )
    distribution_months = max(
        1,
        int(ceil(config.trade.sublayer2_fill_distribution_weeks / config.trade.weeks_per_month)),
    )
    start_month_index = max(1, launch_month_index - prelaunch_months)
    fill_months = tuple(range(start_month_index, launch_month_index + distribution_months))
    reference_months = tuple(range(launch_month_index, launch_month_index + distribution_months))
    reference_demand_units = [
        patient_fg_demand_by_month.get(month_index, 0.0) for month_index in reference_months
    ]
    reference_average_units = (
        sum(reference_demand_units) / len(reference_demand_units) if reference_demand_units else 0.0
    )
    if reference_average_units == 0.0:
        future_nonzero = [
            units
            for month_index, units in sorted(patient_fg_demand_by_month.items())
            if month_index >= launch_month_index and units > 0
        ]
        reference_average_units = future_nonzero[0] if future_nonzero else 0.0
    if reference_average_units == 0.0:
        return {}
    total_launch_fill_units = (
        config.trade.sublayer1_launch_fill_months_of_demand * reference_average_units
    )
    distributed_units = total_launch_fill_units / len(fill_months)
    return {month_index: distributed_units for month_index in fill_months}


def _build_allocation_weights(
    *,
    rows_by_month: dict[int, tuple[Phase2TradeInputRecord, ...]],
    segment_codes: tuple[str, ...],
    months: range,
    lookahead_months: int,
) -> dict[int, dict[str, float]]:
    patient_by_month_segment: dict[int, dict[str, float]] = {}
    zero_template = {segment_code: 0.0 for segment_code in segment_codes}
    for month_index in months:
        patient_by_month_segment[month_index] = zero_template.copy()
        for row in rows_by_month.get(month_index, tuple()):
            patient_by_month_segment[month_index][row.segment_code] = row.patient_fg_demand_units

    weights_by_month: dict[int, dict[str, float]] = {}
    for month_index in months:
        current_weights = patient_by_month_segment[month_index]
        current_total = sum(current_weights.values())
        if current_total > 0:
            weights_by_month[month_index] = {
                segment_code: current_weights[segment_code] / current_total
                for segment_code in segment_codes
            }
            continue
        lookahead_totals = zero_template.copy()
        for candidate_month_index in range(month_index, month_index + lookahead_months):
            candidate_weights = patient_by_month_segment.get(candidate_month_index, zero_template)
            for segment_code in segment_codes:
                lookahead_totals[segment_code] += candidate_weights.get(segment_code, 0.0)
        lookahead_total = sum(lookahead_totals.values())
        if lookahead_total > 0:
            weights_by_month[month_index] = {
                segment_code: lookahead_totals[segment_code] / lookahead_total
                for segment_code in segment_codes
            }
            continue
        prior_months = [
            candidate_month_index
            for candidate_month_index in months
            if candidate_month_index < month_index
            and sum(patient_by_month_segment[candidate_month_index].values()) > 0
        ]
        if prior_months:
            weights_by_month[month_index] = weights_by_month[prior_months[-1]]
            continue
        equal_weight = 1.0 / len(segment_codes)
        weights_by_month[month_index] = {
            segment_code: equal_weight for segment_code in segment_codes
        }
    return weights_by_month


def _target_inventory_units(
    *,
    target_weeks_on_hand: float,
    weekly_demand_units: float,
    active_certified_sites: float,
) -> float:
    if active_certified_sites <= 0 and weekly_demand_units <= 0:
        return 0.0
    return target_weeks_on_hand * weekly_demand_units


def _bullwhip_factor(*, patient_fg_demand_units: float, ex_factory_fg_demand_units: float) -> float:
    if patient_fg_demand_units == 0:
        return float("inf") if ex_factory_fg_demand_units > 0 else 0.0
    return ex_factory_fg_demand_units / patient_fg_demand_units


def _bullwhip_flag(
    *,
    patient_fg_demand_units: float,
    ex_factory_fg_demand_units: float,
    threshold: float,
) -> bool:
    if patient_fg_demand_units == 0:
        return ex_factory_fg_demand_units > 0
    return ex_factory_fg_demand_units > patient_fg_demand_units * (1.0 + threshold)


def _allocate_group_month_to_segments(
    *,
    totals: ModuleMonthTradeTotals,
    allocation_weights: dict[str, float],
    scenario_name: str,
    geography_code: str,
    module: str,
    month_index: int,
    segment_codes: tuple[str, ...],
    rows_by_month: dict[int, tuple[Phase2TradeInputRecord, ...]],
    trade_parameters_used: str,
) -> list[Phase3TradeRecord]:
    patient_by_segment = {segment_code: 0.0 for segment_code in segment_codes}
    notes_by_segment = {segment_code: "" for segment_code in segment_codes}
    for row in rows_by_month.get(month_index, tuple()):
        patient_by_segment[row.segment_code] = row.patient_fg_demand_units
        notes_by_segment[row.segment_code] = row.notes

    outputs: list[Phase3TradeRecord] = []
    for segment_code in segment_codes:
        share = allocation_weights[segment_code]
        outputs.append(
            Phase3TradeRecord(
                scenario_name=scenario_name,
                geography_code=geography_code,
                module=module,
                segment_code=segment_code,
                month_index=month_index,
                calendar_month=totals.calendar_month,
                patient_fg_demand_units=patient_by_segment[segment_code],
                sublayer2_wastage_units=totals.sublayer2_wastage_units * share,
                sublayer2_inventory_target_units=totals.sublayer2_inventory_target_units * share,
                sublayer2_inventory_adjustment_units=totals.sublayer2_inventory_adjustment_units
                * share,
                new_site_stocking_orders_units=totals.new_site_stocking_orders_units * share,
                ss_site_stocking_units=totals.ss_site_stocking_units * share,
                sublayer2_pull_units=totals.sublayer2_pull_units * share,
                sublayer1_inventory_target_units=totals.sublayer1_inventory_target_units * share,
                sublayer1_inventory_adjustment_units=totals.sublayer1_inventory_adjustment_units
                * share,
                ex_factory_fg_demand_units=totals.ex_factory_fg_demand_units * share,
                bullwhip_amplification_factor=totals.bullwhip_amplification_factor,
                bullwhip_flag=totals.bullwhip_flag,
                launch_fill_component_units=totals.launch_fill_component_units * share,
                ongoing_replenishment_component_units=totals.ongoing_replenishment_component_units
                * share,
                active_certified_sites=totals.active_certified_sites,
                new_certified_sites=totals.new_certified_sites,
                sublayer2_inventory_on_hand_end_units=totals.sublayer2_inventory_on_hand_end_units
                * share,
                sublayer1_inventory_on_hand_end_units=totals.sublayer1_inventory_on_hand_end_units
                * share,
                january_softening_applied=totals.january_softening_applied,
                trade_parameters_used=trade_parameters_used,
                notes=notes_by_segment[segment_code],
            )
        )
    return outputs
