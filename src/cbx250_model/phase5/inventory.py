"""Deterministic Phase 5 rolling inventory and shelf-life logic."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
import math

from ..constants import PHYSICAL_SHARED_GEOGRAPHY, PHYSICAL_SHARED_MODULE
from .config_schema import Phase5Config
from .schemas import (
    CohortAuditRecord,
    INVENTORY_NODES,
    InventoryDetailRecord,
    InventoryMonthlySummaryRecord,
    InventorySignal,
    Phase4ScheduleDetailInputRecord,
)


DS_NODE = "DS"
DP_NODE = "DP"
FG_CENTRAL_NODE = "FG_Central"
SS_CENTRAL_NODE = "SS_Central"
SUBLAYER1_NODE = "SubLayer1_FG"
SUBLAYER2_NODE = "SubLayer2_FG"

SHARED_SUPPLY_GROUP = "shared_supply"
GEOGRAPHY_DISTRIBUTION_GROUP = "geography_distribution"


@dataclass
class Cohort:
    cohort_id: str
    original_receipt_month_index: int
    receipt_month_index: int
    expiry_month_index: int
    quantity: float
    source_stage: str
    source_reference: str
    notes: str = ""


@dataclass(frozen=True)
class ReceiptEvent:
    node: str
    receipt_month_index: int
    quantity: float
    expiry_month_index: int
    cohort_id: str
    source_stage: str
    source_reference: str
    notes: str


@dataclass(frozen=True)
class ConsumptionSlice:
    cohort_id: str
    original_receipt_month_index: int
    receipt_month_index: int
    expiry_month_index: int
    quantity: float
    source_stage: str
    source_reference: str
    notes: str


def build_phase5_outputs(
    config: Phase5Config,
    signals: tuple[InventorySignal, ...],
    phase4_schedule_detail: tuple[Phase4ScheduleDetailInputRecord, ...],
) -> tuple[
    tuple[InventoryDetailRecord, ...],
    tuple[InventoryMonthlySummaryRecord, ...],
    tuple[CohortAuditRecord, ...],
]:
    detail_records: list[InventoryDetailRecord] = []
    summary_records: list[InventoryMonthlySummaryRecord] = []
    cohort_records: list[CohortAuditRecord] = []

    signals_by_group = _group_signals(signals)
    detail_by_group = _group_schedule_detail(phase4_schedule_detail)
    shared_support_issue_requests = _build_support_issue_requests(config, phase4_schedule_detail)

    for group_key in sorted(signals_by_group):
        group_signals = signals_by_group[group_key]
        group_detail = detail_by_group.get(group_key, tuple())
        group_detail_records, group_summary_records, group_cohort_records = _build_group_inventory(
            config,
            group_signals,
            group_detail,
            shared_support_issue_requests,
        )
        detail_records.extend(group_detail_records)
        summary_records.extend(group_summary_records)
        cohort_records.extend(group_cohort_records)

    detail_records.sort(
        key=lambda item: (
            item.scenario_name,
            item.geography_code,
            item.module,
            item.month_index,
            item.material_node,
        )
    )
    summary_records.sort(
        key=lambda item: (
            item.scenario_name,
            item.geography_code,
            item.module,
            item.month_index,
        )
    )
    cohort_records.sort(
        key=lambda item: (
            item.scenario_name,
            item.geography_code,
            item.module,
            item.month_index,
            item.material_node,
            item.cohort_id,
        )
    )
    return tuple(detail_records), tuple(summary_records), tuple(cohort_records)


def _build_group_inventory(
    config: Phase5Config,
    signals: tuple[InventorySignal, ...],
    schedule_detail: tuple[Phase4ScheduleDetailInputRecord, ...],
    shared_support_issue_requests: tuple[dict[int, float], dict[int, float]],
) -> tuple[
    list[InventoryDetailRecord],
    list[InventoryMonthlySummaryRecord],
    list[CohortAuditRecord],
]:
    signal_by_month = {signal.month_index: signal for signal in signals}
    positive_months = sorted(signal_by_month)
    if not positive_months:
        return [], [], []

    scenario_name, geography_code, module = (
        signals[0].scenario_name,
        signals[0].geography_code,
        signals[0].module,
    )
    group_kind = _inventory_group_kind(geography_code, module)
    active_nodes = _active_nodes_for_group_kind(group_kind)

    min_event_month = min(
        [1]
        + [record.planned_release_month_index for record in schedule_detail]
        + [record.planned_start_month_index for record in schedule_detail]
        + (
            list(shared_support_issue_requests[0])
            + list(shared_support_issue_requests[1])
            if group_kind == SHARED_SUPPLY_GROUP
            else []
        )
    )
    receipt_events = _build_external_receipt_events(config, schedule_detail)
    ds_issue_requests, dp_issue_requests = (
        shared_support_issue_requests
        if group_kind == SHARED_SUPPLY_GROUP
        else ({}, {})
    )
    policy_context_by_month = {
        month_index: _build_policy_context(
            signal=signal,
            config=config,
            ds_requested=ds_issue_requests.get(month_index, 0.0),
            dp_requested=dp_issue_requests.get(month_index, 0.0),
        )
        for month_index, signal in signal_by_month.items()
    }
    cover_window_months = max(
        1,
        int(math.ceil(config.policy.excess_inventory_threshold_months_of_cover)),
    )

    node_states: dict[str, list[Cohort]] = {node: [] for node in INVENTORY_NODES}
    starting_injected = False

    group_detail_records: list[InventoryDetailRecord] = []
    group_summary_records: list[InventoryMonthlySummaryRecord] = []
    group_cohort_records: list[CohortAuditRecord] = []

    for month_index in range(min_event_month, positive_months[-1] + 1):
        signal = signal_by_month.get(month_index)
        if signal is None and month_index > 0:
            continue

        if month_index == 1 and not starting_injected:
            _inject_starting_inventory(config, node_states, active_nodes)
            starting_injected = True

        month_receipt_events = list(receipt_events.get(month_index, ()))
        prelaunch_positioned_by_node = {node: 0.0 for node in INVENTORY_NODES}
        if (
            month_index == 1
            and signal is not None
            and group_kind == GEOGRAPHY_DISTRIBUTION_GROUP
            and config.policy.allow_prelaunch_inventory_build
        ):
            (
                prelaunch_positioned_by_node,
                month_receipt_events,
            ) = _apply_month1_prelaunch_positioning(
                config=config,
                signal=signal,
                node_states=node_states,
                receipt_events=month_receipt_events,
            )

        opening_inventory = {node: _cohort_total(node_states[node]) for node in INVENTORY_NODES}

        expired_quantities = {
            node: _expire_cohorts(node_states[node], month_index) for node in INVENTORY_NODES
        }
        receipt_totals = {node: 0.0 for node in INVENTORY_NODES}
        for event in month_receipt_events:
            if month_index < 1 and not config.policy.allow_prelaunch_inventory_build:
                continue
            _add_cohort(
                node_states[event.node],
                Cohort(
                    cohort_id=event.cohort_id,
                    original_receipt_month_index=event.receipt_month_index,
                    receipt_month_index=event.receipt_month_index,
                    expiry_month_index=event.expiry_month_index,
                    quantity=event.quantity,
                    source_stage=event.source_stage,
                    source_reference=event.source_reference,
                    notes=event.notes,
                ),
            )
            receipt_totals[event.node] += event.quantity

        ds_requested = ds_issue_requests.get(month_index, 0.0)
        ds_actual, _, _ = _consume_cohorts(
            node_states[DS_NODE],
            ds_requested,
            config.policy.fefo_enabled,
        )

        dp_requested = dp_issue_requests.get(month_index, 0.0)
        dp_actual, _, _ = _consume_cohorts(
            node_states[DP_NODE],
            dp_requested,
            config.policy.fefo_enabled,
        )

        fg_requested = 0.0
        if signal is not None and group_kind == GEOGRAPHY_DISTRIBUTION_GROUP:
            fg_requested = max(
                signal.ex_factory_fg_demand_units - prelaunch_positioned_by_node[SUBLAYER1_NODE],
                0.0,
            )
        available_fg_before_issue = _cohort_total(node_states[FG_CENTRAL_NODE])
        available_ss_before_issue = _cohort_total(node_states[SS_CENTRAL_NODE])
        matched_available_before_issue = _matched_fg_units(
            available_fg_before_issue,
            available_ss_before_issue,
            config,
        )
        fg_target_issues = min(fg_requested, matched_available_before_issue)
        fg_actual, _, fg_slices = _consume_cohorts(
            node_states[FG_CENTRAL_NODE],
            fg_target_issues,
            config.policy.fefo_enabled,
        )
        actual_ss_required = fg_actual * config.conversion.ss_ratio_to_fg
        ss_actual, _, _ = _consume_cohorts(
            node_states[SS_CENTRAL_NODE],
            actual_ss_required,
            config.policy.fefo_enabled,
        )
        if config.policy.ss_fg_match_required and ss_actual + config.policy.stockout_tolerance_units < actual_ss_required:
            fg_actual = min(
                fg_actual,
                ss_actual / config.conversion.ss_ratio_to_fg,
            )

        sublayer1_receipts = _transfer_slices_to_cohorts(
            fg_slices,
            destination_node=SUBLAYER1_NODE,
            month_index=month_index,
        )
        for cohort in sublayer1_receipts:
            _add_cohort(node_states[SUBLAYER1_NODE], cohort)
            receipt_totals[SUBLAYER1_NODE] += cohort.quantity

        sublayer1_requested = 0.0
        if signal is not None and group_kind == GEOGRAPHY_DISTRIBUTION_GROUP:
            sublayer1_requested = max(
                signal.sublayer2_pull_units - prelaunch_positioned_by_node[SUBLAYER2_NODE],
                0.0,
            )
        sublayer1_actual, _, sublayer1_slices = _consume_cohorts(
            node_states[SUBLAYER1_NODE],
            sublayer1_requested,
            config.policy.fefo_enabled,
        )
        sublayer2_receipts = _transfer_slices_to_cohorts(
            sublayer1_slices,
            destination_node=SUBLAYER2_NODE,
            month_index=month_index,
        )
        for cohort in sublayer2_receipts:
            _add_cohort(node_states[SUBLAYER2_NODE], cohort)
            receipt_totals[SUBLAYER2_NODE] += cohort.quantity

        sublayer2_requested = 0.0
        if signal is not None and group_kind == GEOGRAPHY_DISTRIBUTION_GROUP:
            sublayer2_requested = (
                signal.patient_fg_demand_units
                + signal.sublayer2_wastage_units
                + signal.new_site_stocking_orders_units
            )
        sublayer2_actual, _, _ = _consume_cohorts(
            node_states[SUBLAYER2_NODE],
            sublayer2_requested,
            config.policy.fefo_enabled,
        )

        if month_index < 1 or signal is None:
            continue

        policy_context = policy_context_by_month[month_index]
        if (
            group_kind == GEOGRAPHY_DISTRIBUTION_GROUP
            and month_index == 1
            and any(
                prelaunch_positioned_by_node[node] > config.policy.stockout_tolerance_units
                for node in (SUBLAYER1_NODE, SUBLAYER2_NODE)
            )
        ):
            policy_context = _adjust_policy_context_for_prelaunch_positioning(
                policy_context=policy_context,
                prelaunch_positioned_by_node=prelaunch_positioned_by_node,
                config=config,
            )
        required_administrable_demand_by_node = {
            node: float(payload["required_administrable_demand_units"])
            for node, payload in policy_context.items()
        }
        policy_excluded_channel_build_by_node = {
            node: float(payload["policy_excluded_channel_build_units"])
            for node, payload in policy_context.items()
        }
        inventory_policy_gap_by_node = {
            node: float(payload["inventory_policy_gap_units"])
            for node, payload in policy_context.items()
        }
        cover_demand_by_node = _build_forward_cover_demand_by_node(
            policy_context_by_month,
            month_index=month_index,
            cover_window_months=cover_window_months,
        )
        ending_inventory = {node: _cohort_total(node_states[node]) for node in INVENTORY_NODES}
        available_fg_end = ending_inventory[FG_CENTRAL_NODE]
        available_ss_end = ending_inventory[SS_CENTRAL_NODE]
        matched_administrable_fg_units = _matched_fg_units(
            available_fg_end,
            available_ss_end,
            config,
        )
        fg_ss_mismatch_units = max(available_fg_end - matched_administrable_fg_units, 0.0)
        fg_ss_mismatch_flag = fg_ss_mismatch_units > config.policy.stockout_tolerance_units
        raw_demand_signal_by_node = _build_raw_demand_signal_by_node(
            signal=signal,
            policy_context=policy_context,
        )

        node_issue_quantities = {
            DS_NODE: ds_actual,
            DP_NODE: dp_actual,
            FG_CENTRAL_NODE: fg_actual,
            SS_CENTRAL_NODE: ss_actual,
            SUBLAYER1_NODE: sublayer1_actual,
            SUBLAYER2_NODE: sublayer2_actual,
        }
        detail_payloads = {
            node: _build_detail_payload(
                opening_inventory[node],
                receipt_totals[node],
                node_issue_quantities[node],
                expired_quantities[node],
                ending_inventory[node],
                raw_demand_signal_by_node[node],
                required_administrable_demand_by_node[node],
                policy_excluded_channel_build_by_node[node],
                inventory_policy_gap_by_node[node],
                cover_demand_by_node[node],
                config.policy.excess_inventory_threshold_months_of_cover,
                config.policy.stockout_tolerance_units,
            )
            for node in active_nodes
        }

        node_notes = _build_node_notes(
            month_index=month_index,
            signal=signal,
            config=config,
            expired_quantities=expired_quantities,
            detail_payloads=detail_payloads,
            starting_inventory=config.starting_inventory,
            prelaunch_positioned_by_node=prelaunch_positioned_by_node,
            active_nodes=active_nodes,
        )
        for node in active_nodes:
            payload = detail_payloads[node]
            group_detail_records.append(
                InventoryDetailRecord(
                    scenario_name=scenario_name,
                    geography_code=geography_code,
                    module=module,
                    month_index=month_index,
                    calendar_month=signal.calendar_month,
                    material_node=node,
                    opening_inventory=payload["opening_inventory"],
                    receipts=payload["receipts"],
                    issues=payload["issues"],
                    expired_quantity=payload["expired_quantity"],
                    ending_inventory=payload["ending_inventory"],
                    available_nonexpired_inventory=payload["available_nonexpired_inventory"],
                    demand_signal_units=payload["demand_signal_units"],
                    required_administrable_demand_units=payload[
                        "required_administrable_demand_units"
                    ],
                    policy_excluded_channel_build_units=payload[
                        "policy_excluded_channel_build_units"
                    ],
                    inventory_policy_gap_units=payload["inventory_policy_gap_units"],
                    cover_demand_units=payload["cover_demand_units"],
                    effective_cover_demand_units=payload["effective_cover_demand_units"],
                    shortfall_units=payload["shortfall_units"],
                    months_of_cover=payload["months_of_cover"],
                    stockout_flag=payload["stockout_flag"],
                    excess_inventory_flag=payload["excess_inventory_flag"],
                    expiry_flag=payload["expiry_flag"],
                    fg_ss_mismatch_flag=fg_ss_mismatch_flag if node in (FG_CENTRAL_NODE, SS_CENTRAL_NODE) else False,
                    matched_administrable_fg_units=matched_administrable_fg_units
                    if node == FG_CENTRAL_NODE
                    else 0.0,
                    fg_ss_mismatch_units=fg_ss_mismatch_units if node == FG_CENTRAL_NODE else 0.0,
                    notes=node_notes[node],
                )
            )

        group_summary_records.append(
            InventoryMonthlySummaryRecord(
                scenario_name=scenario_name,
                geography_code=geography_code,
                module=module,
                month_index=month_index,
                calendar_month=signal.calendar_month,
                ds_inventory_mg=ending_inventory[DS_NODE] if DS_NODE in active_nodes else 0.0,
                dp_inventory_units=ending_inventory[DP_NODE] if DP_NODE in active_nodes else 0.0,
                fg_inventory_units=ending_inventory[FG_CENTRAL_NODE] if FG_CENTRAL_NODE in active_nodes else 0.0,
                ss_inventory_units=ending_inventory[SS_CENTRAL_NODE] if SS_CENTRAL_NODE in active_nodes else 0.0,
                sublayer1_fg_inventory_units=ending_inventory[SUBLAYER1_NODE] if SUBLAYER1_NODE in active_nodes else 0.0,
                sublayer2_fg_inventory_units=ending_inventory[SUBLAYER2_NODE] if SUBLAYER2_NODE in active_nodes else 0.0,
                expired_ds_mg=expired_quantities[DS_NODE] if DS_NODE in active_nodes else 0.0,
                expired_dp_units=expired_quantities[DP_NODE] if DP_NODE in active_nodes else 0.0,
                expired_fg_units=(
                    (
                        expired_quantities[FG_CENTRAL_NODE]
                        + expired_quantities[SUBLAYER1_NODE]
                        + expired_quantities[SUBLAYER2_NODE]
                    )
                    if FG_CENTRAL_NODE in active_nodes
                    else 0.0
                ),
                expired_ss_units=expired_quantities[SS_CENTRAL_NODE] if SS_CENTRAL_NODE in active_nodes else 0.0,
                unmatched_fg_units=fg_ss_mismatch_units if FG_CENTRAL_NODE in active_nodes else 0.0,
                matched_administrable_fg_units=matched_administrable_fg_units if FG_CENTRAL_NODE in active_nodes else 0.0,
                stockout_flag=any(payload["stockout_flag"] for payload in detail_payloads.values()),
                excess_inventory_flag=any(payload["excess_inventory_flag"] for payload in detail_payloads.values()),
                expiry_flag=any(payload["expiry_flag"] for payload in detail_payloads.values()),
                fg_ss_mismatch_flag=fg_ss_mismatch_flag if FG_CENTRAL_NODE in active_nodes else False,
                notes=" | ".join(
                    note
                    for note in (
                        signal.notes,
                        "FG/SS mismatch risk present." if fg_ss_mismatch_flag else "",
                        "Pre-month-1 released inventory is carried into opening inventory."
                        if config.policy.allow_prelaunch_inventory_build and month_index == 1
                        else "",
                        "Month-1 launch-supporting FG / SS / trade inventory was prepositioned into opening inventory."
                        if any(
                            prelaunch_positioned_by_node[node] > config.policy.stockout_tolerance_units
                            for node in (FG_CENTRAL_NODE, SS_CENTRAL_NODE, SUBLAYER1_NODE, SUBLAYER2_NODE)
                        )
                        else "",
                    )
                    if note
                ),
            )
        )

        for node, cohorts in node_states.items():
            for cohort in cohorts:
                group_cohort_records.append(
                    CohortAuditRecord(
                        scenario_name=scenario_name,
                        geography_code=geography_code,
                        module=module,
                        month_index=month_index,
                        calendar_month=signal.calendar_month,
                        material_node=node,
                        cohort_id=cohort.cohort_id,
                        original_receipt_month_index=cohort.original_receipt_month_index,
                        receipt_month_index=cohort.receipt_month_index,
                        expiry_month_index=cohort.expiry_month_index,
                        ending_quantity=cohort.quantity,
                        notes=cohort.notes,
                    )
                )

    return group_detail_records, group_summary_records, group_cohort_records


def _group_signals(
    signals: tuple[InventorySignal, ...],
) -> dict[tuple[str, str, str], tuple[InventorySignal, ...]]:
    grouped: dict[tuple[str, str, str], list[InventorySignal]] = defaultdict(list)
    for signal in signals:
        grouped[(signal.scenario_name, signal.geography_code, signal.module)].append(signal)
    return {
        key: tuple(sorted(value, key=lambda item: item.month_index))
        for key, value in grouped.items()
    }


def _group_schedule_detail(
    records: tuple[Phase4ScheduleDetailInputRecord, ...],
) -> dict[tuple[str, str, str], tuple[Phase4ScheduleDetailInputRecord, ...]]:
    grouped: dict[tuple[str, str, str], list[Phase4ScheduleDetailInputRecord]] = defaultdict(list)
    for record in records:
        if record.stage in ("DS", "DP"):
            group_key = (
                record.scenario_name,
                PHYSICAL_SHARED_GEOGRAPHY,
                PHYSICAL_SHARED_MODULE,
            )
        else:
            group_key = (
                record.scenario_name,
                record.geography_code,
                PHYSICAL_SHARED_MODULE,
            )
        grouped[group_key].append(record)
    return {
        key: tuple(
            sorted(
                value,
                key=lambda item: (
                    item.stage,
                    item.planned_release_month_index,
                    item.planned_start_month_index,
                    item.batch_number,
                ),
            )
        )
        for key, value in grouped.items()
    }


def _inventory_group_kind(geography_code: str, module: str) -> str:
    if geography_code == PHYSICAL_SHARED_GEOGRAPHY and module == PHYSICAL_SHARED_MODULE:
        return SHARED_SUPPLY_GROUP
    return GEOGRAPHY_DISTRIBUTION_GROUP


def _active_nodes_for_group_kind(group_kind: str) -> tuple[str, ...]:
    if group_kind == SHARED_SUPPLY_GROUP:
        return (DS_NODE, DP_NODE)
    return (FG_CENTRAL_NODE, SS_CENTRAL_NODE, SUBLAYER1_NODE, SUBLAYER2_NODE)


def _build_external_receipt_events(
    config: Phase5Config,
    schedule_detail: tuple[Phase4ScheduleDetailInputRecord, ...],
) -> dict[int, list[ReceiptEvent]]:
    events: dict[int, list[ReceiptEvent]] = defaultdict(list)
    shelf_life_by_stage = {
        "DS": config.shelf_life.ds_months,
        "DP": config.shelf_life.dp_months,
        "FG": config.shelf_life.fg_months,
        "SS": config.shelf_life.ss_months,
    }
    node_by_stage = {
        "DS": DS_NODE,
        "DP": DP_NODE,
        "FG": FG_CENTRAL_NODE,
        "SS": SS_CENTRAL_NODE,
    }
    for record in schedule_detail:
        receipt_month_index = record.planned_release_month_index
        events[receipt_month_index].append(
            ReceiptEvent(
                node=node_by_stage[record.stage],
                receipt_month_index=receipt_month_index,
                quantity=record.batch_quantity,
                expiry_month_index=receipt_month_index + shelf_life_by_stage[record.stage],
                cohort_id=f"{record.batch_number}:{node_by_stage[record.stage]}",
                source_stage=record.stage,
                source_reference=record.batch_number,
                notes=record.notes,
            )
        )
    return events


def _build_support_issue_requests(
    config: Phase5Config,
    schedule_detail: tuple[Phase4ScheduleDetailInputRecord, ...],
) -> tuple[dict[int, float], dict[int, float]]:
    ds_issue_requests: dict[int, float] = defaultdict(float)
    dp_issue_requests: dict[int, float] = defaultdict(float)
    for record in schedule_detail:
        if record.stage == "DP":
            ds_issue_requests[record.planned_start_month_index] += (
                record.batch_quantity
                * config.conversion.ds_qty_per_dp_unit_mg
                / config.conversion.ds_to_dp_yield
                * (1.0 + config.conversion.ds_overage_factor)
            )
        elif record.stage == "FG":
            dp_issue_requests[record.planned_start_month_index] += (
                record.batch_quantity / config.conversion.dp_to_fg_yield
            )
    return dict(ds_issue_requests), dict(dp_issue_requests)


def _inject_starting_inventory(
    config: Phase5Config,
    node_states: dict[str, list[Cohort]],
    active_nodes: tuple[str, ...],
) -> None:
    starting_map = {
        DS_NODE: (config.starting_inventory.ds_mg, config.shelf_life.ds_months, "DS"),
        DP_NODE: (config.starting_inventory.dp_units, config.shelf_life.dp_months, "DP"),
        FG_CENTRAL_NODE: (config.starting_inventory.fg_units, config.shelf_life.fg_months, "FG"),
        SS_CENTRAL_NODE: (config.starting_inventory.ss_units, config.shelf_life.ss_months, "SS"),
        SUBLAYER1_NODE: (
            config.starting_inventory.sublayer1_fg_units,
            config.shelf_life.fg_months,
            "FG",
        ),
        SUBLAYER2_NODE: (
            config.starting_inventory.sublayer2_fg_units,
            config.shelf_life.fg_months,
            "FG",
        ),
    }
    for node, (quantity, shelf_life_months, source_stage) in starting_map.items():
        if node not in active_nodes:
            continue
        if quantity <= 0:
            continue
        _add_cohort(
            node_states[node],
            Cohort(
                cohort_id=f"STARTING:{node}",
                original_receipt_month_index=1,
                receipt_month_index=1,
                expiry_month_index=1 + shelf_life_months,
                quantity=quantity,
                source_stage=source_stage,
                source_reference="STARTING_INVENTORY",
                notes="Starting inventory is treated as a fresh month-1 opening cohort.",
            ),
        )


def _apply_month1_prelaunch_positioning(
    *,
    config: Phase5Config,
    signal: InventorySignal,
    node_states: dict[str, list[Cohort]],
    receipt_events: list[ReceiptEvent],
) -> tuple[dict[str, float], list[ReceiptEvent]]:
    positioned_by_node = {node: 0.0 for node in INVENTORY_NODES}
    remaining_events: list[ReceiptEvent] = []
    for event in receipt_events:
        if event.node not in (FG_CENTRAL_NODE, SS_CENTRAL_NODE):
            remaining_events.append(event)
            continue
        _add_cohort(
            node_states[event.node],
            Cohort(
                cohort_id=f"{event.cohort_id}:PRELAUNCH_OPENING",
                original_receipt_month_index=event.receipt_month_index,
                receipt_month_index=0,
                expiry_month_index=event.expiry_month_index,
                quantity=event.quantity,
                source_stage=event.source_stage,
                source_reference=event.source_reference,
                notes="Month-1 launch-supporting release was treated as prelaunch-positioned opening inventory.",
            ),
        )
        positioned_by_node[event.node] += event.quantity

    available_fg = _cohort_total(node_states[FG_CENTRAL_NODE])
    available_ss = _cohort_total(node_states[SS_CENTRAL_NODE])
    matched_fg = _matched_fg_units(available_fg, available_ss, config)
    fg_to_sublayer1 = min(signal.ex_factory_fg_demand_units, matched_fg)
    fg_transferred, _, fg_slices = _consume_cohorts(
        node_states[FG_CENTRAL_NODE],
        fg_to_sublayer1,
        config.policy.fefo_enabled,
    )
    ss_required_for_transfer = fg_transferred * config.conversion.ss_ratio_to_fg
    if ss_required_for_transfer > 0:
        _consume_cohorts(
            node_states[SS_CENTRAL_NODE],
            ss_required_for_transfer,
            config.policy.fefo_enabled,
        )
    prelaunch_sublayer1_receipts = _transfer_slices_to_cohorts(
        fg_slices,
        destination_node=SUBLAYER1_NODE,
        month_index=0,
    )
    for cohort in prelaunch_sublayer1_receipts:
        cohort.notes = (
            cohort.notes + " | " if cohort.notes else ""
        ) + "Prelaunch ex-factory positioning created month-1 opening inventory."
        _add_cohort(node_states[SUBLAYER1_NODE], cohort)
        positioned_by_node[SUBLAYER1_NODE] += cohort.quantity

    s1_to_s2 = min(signal.sublayer2_pull_units, _cohort_total(node_states[SUBLAYER1_NODE]))
    sublayer1_actual, _, sublayer1_slices = _consume_cohorts(
        node_states[SUBLAYER1_NODE],
        s1_to_s2,
        config.policy.fefo_enabled,
    )
    prelaunch_sublayer2_receipts = _transfer_slices_to_cohorts(
        sublayer1_slices,
        destination_node=SUBLAYER2_NODE,
        month_index=0,
    )
    for cohort in prelaunch_sublayer2_receipts:
        cohort.notes = (
            cohort.notes + " | " if cohort.notes else ""
        ) + "Prelaunch Sub-Layer positioning created month-1 opening inventory."
        _add_cohort(node_states[SUBLAYER2_NODE], cohort)
        positioned_by_node[SUBLAYER2_NODE] += cohort.quantity

    return positioned_by_node, remaining_events


def _expire_cohorts(cohorts: list[Cohort], month_index: int) -> float:
    expired_quantity = 0.0
    remaining: list[Cohort] = []
    for cohort in cohorts:
        if cohort.expiry_month_index <= month_index:
            expired_quantity += cohort.quantity
        else:
            remaining.append(cohort)
    cohorts[:] = remaining
    return expired_quantity


def _consume_cohorts(
    cohorts: list[Cohort],
    requested_quantity: float,
    fefo_enabled: bool,
) -> tuple[float, float, list[ConsumptionSlice]]:
    if requested_quantity <= 0:
        return 0.0, 0.0, []
    ordering = (
        lambda cohort: (
            cohort.expiry_month_index,
            cohort.original_receipt_month_index,
            cohort.receipt_month_index,
            cohort.cohort_id,
        )
        if fefo_enabled
        else (
            cohort.receipt_month_index,
            cohort.original_receipt_month_index,
            cohort.expiry_month_index,
            cohort.cohort_id,
        )
    )
    actual_issued = 0.0
    remaining_request = requested_quantity
    consumed: list[ConsumptionSlice] = []
    for cohort in sorted(cohorts, key=ordering):
        if remaining_request <= 0:
            break
        quantity_taken = min(cohort.quantity, remaining_request)
        if quantity_taken <= 0:
            continue
        cohort.quantity -= quantity_taken
        actual_issued += quantity_taken
        remaining_request -= quantity_taken
        consumed.append(
            ConsumptionSlice(
                cohort_id=cohort.cohort_id,
                original_receipt_month_index=cohort.original_receipt_month_index,
                receipt_month_index=cohort.receipt_month_index,
                expiry_month_index=cohort.expiry_month_index,
                quantity=quantity_taken,
                source_stage=cohort.source_stage,
                source_reference=cohort.source_reference,
                notes=cohort.notes,
            )
        )
    cohorts[:] = [cohort for cohort in cohorts if cohort.quantity > 1e-12]
    return actual_issued, max(requested_quantity - actual_issued, 0.0), consumed


def _transfer_slices_to_cohorts(
    slices: list[ConsumptionSlice],
    *,
    destination_node: str,
    month_index: int,
) -> list[Cohort]:
    transferred: list[Cohort] = []
    for index, item in enumerate(slices, start=1):
        transferred.append(
            Cohort(
                cohort_id=f"{item.cohort_id}->{destination_node}:{month_index:04d}:{index:03d}",
                original_receipt_month_index=item.original_receipt_month_index,
                receipt_month_index=month_index,
                expiry_month_index=item.expiry_month_index,
                quantity=item.quantity,
                source_stage=item.source_stage,
                source_reference=item.source_reference,
                notes=item.notes,
            )
        )
    return transferred


def _cohort_total(cohorts: list[Cohort]) -> float:
    return sum(cohort.quantity for cohort in cohorts)


def _add_cohort(cohorts: list[Cohort], cohort: Cohort) -> None:
    if cohort.quantity <= 0:
        return
    cohorts.append(cohort)


def _matched_fg_units(available_fg_units: float, available_ss_units: float, config: Phase5Config) -> float:
    if not config.policy.ss_fg_match_required:
        return available_fg_units
    return min(available_fg_units, available_ss_units / config.conversion.ss_ratio_to_fg)


def _build_policy_context(
    *,
    signal: InventorySignal,
    config: Phase5Config,
    ds_requested: float,
    dp_requested: float,
) -> dict[str, dict[str, float]]:
    patient_support_units = signal.underlying_patient_consumption_units
    fg_policy_supported_units = min(signal.ex_factory_fg_demand_units, patient_support_units)
    fg_cover_units = fg_policy_supported_units
    dp_cover_units = fg_cover_units / config.conversion.dp_to_fg_yield
    ds_cover_units = (
        dp_cover_units
        * config.conversion.ds_qty_per_dp_unit_mg
        / config.conversion.ds_to_dp_yield
        * (1.0 + config.conversion.ds_overage_factor)
    )
    ss_cover_units = fg_cover_units * config.conversion.ss_ratio_to_fg

    def payload(
        *,
        raw: float,
        reference: float,
        required: float,
        cover: float,
    ) -> dict[str, float]:
        return {
            "raw_demand_signal_units": raw,
            "reference_support_units": reference,
            "required_administrable_demand_units": required,
            "policy_excluded_channel_build_units": max(raw - reference, 0.0),
            "inventory_policy_gap_units": max(reference - required, 0.0),
            "cover_demand_units": cover,
        }

    return {
        DS_NODE: payload(
            raw=ds_requested,
            reference=ds_requested,
            required=ds_requested,
            cover=ds_cover_units,
        ),
        DP_NODE: payload(
            raw=dp_requested,
            reference=dp_requested,
            required=dp_requested,
            cover=dp_cover_units,
        ),
        FG_CENTRAL_NODE: payload(
            raw=signal.ex_factory_fg_demand_units,
            reference=patient_support_units,
            required=fg_policy_supported_units,
            cover=fg_cover_units,
        ),
        SS_CENTRAL_NODE: payload(
            raw=(
                signal.ex_factory_fg_demand_units * config.conversion.ss_ratio_to_fg
                + signal.ss_site_stocking_units
            ),
            reference=patient_support_units * config.conversion.ss_ratio_to_fg,
            required=fg_policy_supported_units * config.conversion.ss_ratio_to_fg,
            cover=ss_cover_units,
        ),
        SUBLAYER1_NODE: payload(
            raw=signal.sublayer2_pull_units,
            reference=patient_support_units,
            required=fg_policy_supported_units,
            cover=fg_policy_supported_units,
        ),
        SUBLAYER2_NODE: payload(
            raw=(
                signal.patient_fg_demand_units
                + signal.sublayer2_wastage_units
                + signal.new_site_stocking_orders_units
            ),
            reference=patient_support_units,
            required=fg_policy_supported_units,
            cover=fg_policy_supported_units,
        ),
    }


def _adjust_policy_context_for_prelaunch_positioning(
    *,
    policy_context: dict[str, dict[str, float]],
    prelaunch_positioned_by_node: dict[str, float],
    config: Phase5Config,
) -> dict[str, dict[str, float]]:
    adjusted = {node: dict(payload) for node, payload in policy_context.items()}
    fg_prelaunch_units = prelaunch_positioned_by_node.get(SUBLAYER1_NODE, 0.0)
    sublayer1_prelaunch_units = prelaunch_positioned_by_node.get(SUBLAYER2_NODE, 0.0)
    ss_prelaunch_units = fg_prelaunch_units * config.conversion.ss_ratio_to_fg

    for node, prelaunch_units in (
        (FG_CENTRAL_NODE, fg_prelaunch_units),
        (SS_CENTRAL_NODE, ss_prelaunch_units),
        (SUBLAYER1_NODE, sublayer1_prelaunch_units),
    ):
        payload = adjusted[node]
        payload["required_administrable_demand_units"] = max(
            float(payload["required_administrable_demand_units"]) - prelaunch_units,
            0.0,
        )
    return adjusted


def _build_raw_demand_signal_by_node(
    *,
    signal: InventorySignal,
    policy_context: dict[str, dict[str, float]],
) -> dict[str, float]:
    return {
        node: float(payload["raw_demand_signal_units"])
        for node, payload in policy_context.items()
    }


def _build_forward_cover_demand_by_node(
    policy_context_by_month: dict[int, dict[str, dict[str, float]]],
    *,
    month_index: int,
    cover_window_months: int,
) -> dict[str, float]:
    effective_window = max(1, cover_window_months)
    cover_demand_by_node: dict[str, float] = {}
    for node in INVENTORY_NODES:
        forward_cover_values: list[float] = []
        for offset in range(effective_window):
            forward_cover_values.append(
                float(
                policy_context_by_month.get(month_index + offset, {})
                .get(node, {})
                .get("cover_demand_units", 0.0)
                )
            )
        cover_demand_by_node[node] = max(forward_cover_values, default=0.0)
    return cover_demand_by_node


def _build_detail_payload(
    opening_inventory: float,
    receipts: float,
    issues: float,
    expired_quantity: float,
    ending_inventory: float,
    demand_signal_units: float,
    required_administrable_demand_units: float,
    policy_excluded_channel_build_units: float,
    inventory_policy_gap_units: float,
    cover_demand_units: float,
    excess_threshold_months_of_cover: float,
    stockout_tolerance_units: float,
) -> dict[str, float | bool]:
    shortfall_units = max(required_administrable_demand_units - issues, 0.0)
    available_nonexpired_inventory = ending_inventory
    effective_cover_demand_units = (
        required_administrable_demand_units + inventory_policy_gap_units
    )
    months_of_cover = _months_of_cover(
        available_nonexpired_inventory,
        effective_cover_demand_units,
        stockout_tolerance_units,
    )
    return {
        "opening_inventory": opening_inventory,
        "receipts": receipts,
        "issues": issues,
        "expired_quantity": expired_quantity,
        "ending_inventory": ending_inventory,
        "available_nonexpired_inventory": available_nonexpired_inventory,
        "demand_signal_units": demand_signal_units,
        "required_administrable_demand_units": required_administrable_demand_units,
        "policy_excluded_channel_build_units": policy_excluded_channel_build_units,
        "inventory_policy_gap_units": inventory_policy_gap_units,
        "cover_demand_units": cover_demand_units,
        "effective_cover_demand_units": effective_cover_demand_units,
        "shortfall_units": shortfall_units,
        "months_of_cover": months_of_cover,
        "stockout_flag": shortfall_units > stockout_tolerance_units,
        "excess_inventory_flag": (
            effective_cover_demand_units > stockout_tolerance_units
            and months_of_cover > excess_threshold_months_of_cover
        ),
        "expiry_flag": expired_quantity > stockout_tolerance_units,
    }


def _months_of_cover(
    ending_inventory: float,
    demand_signal_units: float,
    stockout_tolerance_units: float,
) -> float:
    if ending_inventory <= stockout_tolerance_units:
        return 0.0
    if demand_signal_units <= stockout_tolerance_units:
        return 0.0
    return ending_inventory / demand_signal_units


def _build_node_notes(
    *,
    month_index: int,
    signal: InventorySignal,
    config: Phase5Config,
    expired_quantities: dict[str, float],
    detail_payloads: dict[str, dict[str, float | bool]],
    starting_inventory,
    prelaunch_positioned_by_node: dict[str, float],
    active_nodes: tuple[str, ...],
) -> dict[str, str]:
    notes: dict[str, list[str]] = {node: [] for node in INVENTORY_NODES}
    if month_index == 1 and config.policy.allow_prelaunch_inventory_build:
        for node in active_nodes:
            notes[node].append("Pre-month-1 released inventory is carried into opening inventory.")
            if prelaunch_positioned_by_node.get(node, 0.0) > config.policy.stockout_tolerance_units:
                notes[node].append(
                    "Month-1 launch-supporting inventory was prepositioned into opening inventory instead of being treated only as a month-1 receipt."
                )
    if month_index == 1 and any(
        quantity > 0
        for quantity in (
            starting_inventory.ds_mg,
            starting_inventory.dp_units,
            starting_inventory.fg_units,
            starting_inventory.ss_units,
            starting_inventory.sublayer1_fg_units,
            starting_inventory.sublayer2_fg_units,
        )
    ):
        for node in active_nodes:
            notes[node].append("Configured starting inventory is treated as a fresh month-1 opening cohort.")

    notes[FG_CENTRAL_NODE].append(
        "Issues are driven by Phase 3 ex-factory FG demand, constrained by matched SS availability, and interpreted against policy-supported trade demand."
    )
    notes[SUBLAYER1_NODE].append("Receipts come from FG ex-factory shipments; issues follow Sub-Layer 2 pull.")
    notes[SUBLAYER2_NODE].append(
        "Issues reflect available Sub-Layer 2 replenishment; temporary site-stocking and channel-build underfill are audited separately from true shortage."
    )

    if signal.notes:
        notes[FG_CENTRAL_NODE].append(signal.notes)

    for node, expired_quantity in expired_quantities.items():
        if node not in detail_payloads:
            continue
        if expired_quantity > config.policy.stockout_tolerance_units:
            notes[node].append("Expired inventory was removed before consumption.")
        policy_excluded_units = float(
            detail_payloads[node]["policy_excluded_channel_build_units"]
        )
        if policy_excluded_units > config.policy.stockout_tolerance_units:
            notes[node].append(
                "Temporary channel-build demand was excluded from true shortage and excess flagging."
            )
        inventory_policy_gap_units = float(
            detail_payloads[node]["inventory_policy_gap_units"]
        )
        if inventory_policy_gap_units > config.policy.stockout_tolerance_units:
            notes[node].append(
                "Policy-intended channel drawdown or underfill was separated from true shortage flagging."
            )
        if detail_payloads[node]["stockout_flag"]:
            notes[node].append(
                "Required administrable demand exceeded available non-expired inventory for this node."
            )
        if detail_payloads[node]["excess_inventory_flag"]:
            notes[node].append(
                "Ending inventory exceeded the configured months-of-cover threshold relative to forward cover demand plus any policy drawdown requirement."
            )
    return {node: " | ".join(node_notes) for node, node_notes in notes.items()}
