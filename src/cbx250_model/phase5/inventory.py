"""Deterministic Phase 5 rolling inventory and shelf-life logic."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
import math

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

    for group_key in sorted(signals_by_group):
        group_signals = signals_by_group[group_key]
        group_detail = detail_by_group.get(group_key, tuple())
        group_detail_records, group_summary_records, group_cohort_records = _build_group_inventory(
            config,
            group_signals,
            group_detail,
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
) -> tuple[
    list[InventoryDetailRecord],
    list[InventoryMonthlySummaryRecord],
    list[CohortAuditRecord],
]:
    signal_by_month = {signal.month_index: signal for signal in signals}
    positive_months = sorted(signal_by_month)
    if not positive_months:
        return [], [], []

    min_event_month = min(
        [1]
        + [record.planned_release_month_index for record in schedule_detail]
        + [record.planned_start_month_index for record in schedule_detail]
    )
    receipt_events = _build_external_receipt_events(config, schedule_detail)
    ds_issue_requests, dp_issue_requests = _build_support_issue_requests(config, schedule_detail)

    scenario_name, geography_code, module = (
        signals[0].scenario_name,
        signals[0].geography_code,
        signals[0].module,
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
            _inject_starting_inventory(config, node_states)
            starting_injected = True

        opening_inventory = {node: _cohort_total(node_states[node]) for node in INVENTORY_NODES}

        expired_quantities = {
            node: _expire_cohorts(node_states[node], month_index) for node in INVENTORY_NODES
        }
        receipt_totals = {node: 0.0 for node in INVENTORY_NODES}
        for event in receipt_events.get(month_index, ()):
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

        fg_requested = signal.ex_factory_fg_demand_units if signal else 0.0
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

        sublayer1_requested = signal.sublayer2_pull_units if signal else 0.0
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

        sublayer2_requested = (
            signal.patient_fg_demand_units + signal.sublayer2_wastage_units + signal.new_site_stocking_orders_units
            if signal
            else 0.0
        )
        sublayer2_actual, _, _ = _consume_cohorts(
            node_states[SUBLAYER2_NODE],
            sublayer2_requested,
            config.policy.fefo_enabled,
        )

        if month_index < 1 or signal is None:
            continue

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

        detail_payloads = {
            DS_NODE: _build_detail_payload(
                opening_inventory[DS_NODE],
                receipt_totals[DS_NODE],
                ds_actual,
                expired_quantities[DS_NODE],
                ending_inventory[DS_NODE],
                ds_requested,
                config.policy.excess_inventory_threshold_months_of_cover,
                config.policy.stockout_tolerance_units,
            ),
            DP_NODE: _build_detail_payload(
                opening_inventory[DP_NODE],
                receipt_totals[DP_NODE],
                dp_actual,
                expired_quantities[DP_NODE],
                ending_inventory[DP_NODE],
                dp_requested,
                config.policy.excess_inventory_threshold_months_of_cover,
                config.policy.stockout_tolerance_units,
            ),
            FG_CENTRAL_NODE: _build_detail_payload(
                opening_inventory[FG_CENTRAL_NODE],
                receipt_totals[FG_CENTRAL_NODE],
                fg_actual,
                expired_quantities[FG_CENTRAL_NODE],
                ending_inventory[FG_CENTRAL_NODE],
                fg_requested,
                config.policy.excess_inventory_threshold_months_of_cover,
                config.policy.stockout_tolerance_units,
            ),
            SS_CENTRAL_NODE: _build_detail_payload(
                opening_inventory[SS_CENTRAL_NODE],
                receipt_totals[SS_CENTRAL_NODE],
                ss_actual,
                expired_quantities[SS_CENTRAL_NODE],
                ending_inventory[SS_CENTRAL_NODE],
                actual_ss_required,
                config.policy.excess_inventory_threshold_months_of_cover,
                config.policy.stockout_tolerance_units,
            ),
            SUBLAYER1_NODE: _build_detail_payload(
                opening_inventory[SUBLAYER1_NODE],
                receipt_totals[SUBLAYER1_NODE],
                sublayer1_actual,
                expired_quantities[SUBLAYER1_NODE],
                ending_inventory[SUBLAYER1_NODE],
                sublayer1_requested,
                config.policy.excess_inventory_threshold_months_of_cover,
                config.policy.stockout_tolerance_units,
            ),
            SUBLAYER2_NODE: _build_detail_payload(
                opening_inventory[SUBLAYER2_NODE],
                receipt_totals[SUBLAYER2_NODE],
                sublayer2_actual,
                expired_quantities[SUBLAYER2_NODE],
                ending_inventory[SUBLAYER2_NODE],
                sublayer2_requested,
                config.policy.excess_inventory_threshold_months_of_cover,
                config.policy.stockout_tolerance_units,
            ),
        }

        node_notes = _build_node_notes(
            month_index=month_index,
            signal=signal,
            config=config,
            expired_quantities=expired_quantities,
            detail_payloads=detail_payloads,
            starting_inventory=config.starting_inventory,
        )
        for node in INVENTORY_NODES:
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
                ds_inventory_mg=ending_inventory[DS_NODE],
                dp_inventory_units=ending_inventory[DP_NODE],
                fg_inventory_units=ending_inventory[FG_CENTRAL_NODE],
                ss_inventory_units=ending_inventory[SS_CENTRAL_NODE],
                sublayer1_fg_inventory_units=ending_inventory[SUBLAYER1_NODE],
                sublayer2_fg_inventory_units=ending_inventory[SUBLAYER2_NODE],
                expired_ds_mg=expired_quantities[DS_NODE],
                expired_dp_units=expired_quantities[DP_NODE],
                expired_fg_units=(
                    expired_quantities[FG_CENTRAL_NODE]
                    + expired_quantities[SUBLAYER1_NODE]
                    + expired_quantities[SUBLAYER2_NODE]
                ),
                expired_ss_units=expired_quantities[SS_CENTRAL_NODE],
                unmatched_fg_units=fg_ss_mismatch_units,
                matched_administrable_fg_units=matched_administrable_fg_units,
                stockout_flag=any(payload["stockout_flag"] for payload in detail_payloads.values()),
                excess_inventory_flag=any(
                    payload["excess_inventory_flag"] for payload in detail_payloads.values()
                ),
                expiry_flag=any(payload["expiry_flag"] for payload in detail_payloads.values()),
                fg_ss_mismatch_flag=fg_ss_mismatch_flag,
                notes=" | ".join(
                    note
                    for note in (
                        signal.notes,
                        "FG/SS mismatch risk present." if fg_ss_mismatch_flag else "",
                        "Pre-month-1 released inventory is carried into opening inventory."
                        if config.policy.allow_prelaunch_inventory_build and month_index == 1
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
        grouped[(record.scenario_name, record.geography_code, record.module)].append(record)
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


def _inject_starting_inventory(config: Phase5Config, node_states: dict[str, list[Cohort]]) -> None:
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


def _build_detail_payload(
    opening_inventory: float,
    receipts: float,
    issues: float,
    expired_quantity: float,
    ending_inventory: float,
    demand_signal_units: float,
    excess_threshold_months_of_cover: float,
    stockout_tolerance_units: float,
) -> dict[str, float | bool]:
    shortfall_units = max(demand_signal_units - issues, 0.0)
    available_nonexpired_inventory = ending_inventory
    months_of_cover = _months_of_cover(available_nonexpired_inventory, demand_signal_units, stockout_tolerance_units)
    return {
        "opening_inventory": opening_inventory,
        "receipts": receipts,
        "issues": issues,
        "expired_quantity": expired_quantity,
        "ending_inventory": ending_inventory,
        "available_nonexpired_inventory": available_nonexpired_inventory,
        "demand_signal_units": demand_signal_units,
        "shortfall_units": shortfall_units,
        "months_of_cover": months_of_cover,
        "stockout_flag": shortfall_units > stockout_tolerance_units,
        "excess_inventory_flag": months_of_cover > excess_threshold_months_of_cover,
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
        return math.inf
    return ending_inventory / demand_signal_units


def _build_node_notes(
    *,
    month_index: int,
    signal: InventorySignal,
    config: Phase5Config,
    expired_quantities: dict[str, float],
    detail_payloads: dict[str, dict[str, float | bool]],
    starting_inventory,
) -> dict[str, str]:
    notes: dict[str, list[str]] = {node: [] for node in INVENTORY_NODES}
    if month_index == 1 and config.policy.allow_prelaunch_inventory_build:
        for node in INVENTORY_NODES:
            notes[node].append("Pre-month-1 released inventory is carried into opening inventory.")
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
        for node in INVENTORY_NODES:
            notes[node].append("Configured starting inventory is treated as a fresh month-1 opening cohort.")

    notes[FG_CENTRAL_NODE].append(
        "Issues are driven by Phase 3 ex-factory FG demand and matched SS availability."
    )
    notes[SUBLAYER1_NODE].append("Receipts come from FG ex-factory shipments; issues follow Sub-Layer 2 pull.")
    notes[SUBLAYER2_NODE].append(
        "Issues include patient demand, Sub-Layer 2 wastage, and new-site stocking support."
    )

    if signal.notes:
        notes[FG_CENTRAL_NODE].append(signal.notes)

    for node, expired_quantity in expired_quantities.items():
        if expired_quantity > config.policy.stockout_tolerance_units:
            notes[node].append("Expired inventory was removed before consumption.")
        if detail_payloads[node]["stockout_flag"]:
            notes[node].append("Demand exceeded available non-expired inventory for this node.")
        if detail_payloads[node]["excess_inventory_flag"]:
            notes[node].append("Ending inventory exceeded the configured months-of-cover threshold.")
    return {node: " | ".join(node_notes) for node, node_notes in notes.items()}
