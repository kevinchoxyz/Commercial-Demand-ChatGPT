"""Deterministic Phase 4 production scheduling."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from math import ceil

from ..constants import PHYSICAL_SHARED_GEOGRAPHY, PHYSICAL_SHARED_MODULE
from .config_schema import Phase4Config
from .schemas import (
    ScheduleAllocationRecord,
    ScheduleDetailRecord,
    ScheduleMonthlySummaryRecord,
    SchedulingSignal,
)


@dataclass(frozen=True)
class StageBatchRequest:
    scenario_name: str
    stage: str
    module: str
    geography_code: str
    demand_month_index: int
    demand_calendar_month: date
    release_month_index: int
    release_calendar_month: date
    start_month_index: int
    start_calendar_month: date
    quantity: float
    quantity_unit: str
    bullwhip_amplification_factor: float
    excess_build_flag: bool
    stepdown_applied: bool


@dataclass(frozen=True)
class StagePlanConfig:
    stage: str
    quantity_unit: str
    min_batch_quantity: float | None
    max_batch_quantity: float
    min_campaign_batches: int
    annual_capacity_batches: int | None
    release_offset_months: int
    start_offset_months: int
    fixed_batch_quantity: float | None


@dataclass(frozen=True)
class PlannedStageBatch:
    scenario_name: str
    stage: str
    module: str
    geography_code: str
    support_start_month_index: int
    support_start_calendar_month: date
    support_end_month_index: int
    support_end_calendar_month: date
    release_month_index: int
    release_calendar_month: date
    start_month_index: int
    start_calendar_month: date
    quantity: float
    allocated_support_quantity: float
    quantity_unit: str
    stepdown_applied: bool
    excess_build_flag: bool
    batch_index_in_year: int
    capacity_limit: float
    capacity_metric: str
    capacity_flag: bool
    supported_signal_keys: tuple[tuple[str, str, str, int], ...]


@dataclass(frozen=True)
class PlannedStageAllocation:
    scenario_name: str
    stage: str
    module: str
    geography_code: str
    allocated_module: str
    allocated_geography_code: str
    batch_index_in_year: int
    quantity_unit: str
    physical_batch_quantity: float
    start_month_index: int
    start_calendar_month: date
    release_month_index: int
    release_calendar_month: date
    demand_month_index: int
    demand_calendar_month: date
    allocated_support_quantity: float
    stepdown_applied: bool
    excess_build_flag: bool


def build_phase4_outputs(
    config: Phase4Config,
    signals: tuple[SchedulingSignal, ...],
) -> tuple[
    tuple[ScheduleDetailRecord, ...],
    tuple[ScheduleMonthlySummaryRecord, ...],
    tuple[ScheduleAllocationRecord, ...],
]:
    anchor_date = _derive_anchor_date(signals)
    resolved_signals = _resolve_signals(config, signals)

    fg_config = StagePlanConfig(
        stage="FG",
        quantity_unit="units",
        min_batch_quantity=None,
        max_batch_quantity=config.fg.packaging_campaign_size_units,
        min_campaign_batches=0,
        annual_capacity_batches=None,
        release_offset_months=0,
        start_offset_months=_months_from_weeks(
            config.fg.packaging_cycle_weeks + config.fg.release_qa_weeks,
            config.conversion.weeks_per_month,
        ),
        fixed_batch_quantity=None,
    )
    dp_release_offset = _months_from_weeks(
        config.fg.total_order_to_release_weeks,
        config.conversion.weeks_per_month,
    )
    dp_config = StagePlanConfig(
        stage="DP",
        quantity_unit="units",
        min_batch_quantity=config.dp.min_batch_size_units,
        max_batch_quantity=config.dp.max_batch_size_units,
        min_campaign_batches=config.dp.min_campaign_batches,
        annual_capacity_batches=config.dp.annual_capacity_batches,
        release_offset_months=dp_release_offset,
        start_offset_months=dp_release_offset
        + _months_from_weeks(
            config.dp.manufacturing_cycle_weeks + config.dp.release_testing_weeks,
            config.conversion.weeks_per_month,
        ),
        fixed_batch_quantity=None,
    )
    ds_release_offset = dp_config.start_offset_months + _months_from_weeks(
        config.dp.lead_time_from_ds_release_weeks,
        config.conversion.weeks_per_month,
    )
    ds_config = StagePlanConfig(
        stage="DS",
        quantity_unit="mg",
        min_batch_quantity=config.ds.min_batch_size_mg,
        max_batch_quantity=config.ds.max_batch_size_mg,
        min_campaign_batches=config.ds.min_campaign_batches,
        annual_capacity_batches=config.ds.annual_capacity_batches,
        release_offset_months=ds_release_offset,
        start_offset_months=ds_release_offset
        + _months_from_weeks(
            config.ds.manufacturing_cycle_weeks + config.ds.release_testing_weeks,
            config.conversion.weeks_per_month,
        ),
        fixed_batch_quantity=None,
    )
    ss_config = StagePlanConfig(
        stage="SS",
        quantity_unit="units",
        min_batch_quantity=config.ss.batch_size_units,
        max_batch_quantity=config.ss.batch_size_units,
        min_campaign_batches=config.ss.min_campaign_batches,
        annual_capacity_batches=config.ss.annual_capacity_batches,
        release_offset_months=0,
        start_offset_months=_months_from_weeks(
            config.ss.order_to_release_lead_time_weeks,
            config.conversion.weeks_per_month,
        ),
        fixed_batch_quantity=config.ss.batch_size_units,
    )

    fg_requests = _build_stage_requests(anchor_date, resolved_signals, fg_config, "fg_release_units", config)
    dp_requests = _build_stage_requests(anchor_date, resolved_signals, dp_config, "dp_release_units", config)
    ds_requests = _build_stage_requests(
        anchor_date,
        resolved_signals,
        ds_config,
        "ds_release_quantity_mg",
        config,
    )
    ss_requests = _build_stage_requests(anchor_date, resolved_signals, ss_config, "ss_release_units", config)

    fg_batches, fg_allocations, fg_dropped = _plan_stage_batches(fg_requests, fg_config)
    dp_batches, dp_allocations, dp_dropped = _plan_stage_batches(dp_requests, dp_config)
    ds_batches, ds_allocations, ds_dropped = _plan_stage_batches(ds_requests, ds_config)
    ss_batches, ss_allocations, ss_dropped = _plan_stage_batches(ss_requests, ss_config)

    fg_physical_cumulative = _build_cumulative_release_lookup(fg_batches)
    ss_physical_cumulative = _build_cumulative_release_lookup(ss_batches)
    fg_support_cumulative: dict[tuple[str, str, str], float] = defaultdict(float)
    ss_support_cumulative: dict[tuple[str, str, str], float] = defaultdict(float)

    summary_records: list[ScheduleMonthlySummaryRecord] = []
    summary_lookup: dict[tuple[str, str, str, int], ScheduleMonthlySummaryRecord] = {}
    for signal in resolved_signals:
        signal_key = signal.key
        clipped_quantities_by_stage = {
            "FG": fg_dropped.get(signal_key, 0.0),
            "DP": dp_dropped.get(signal_key, 0.0),
            "DS": ds_dropped.get(signal_key, 0.0),
            "SS": ss_dropped.get(signal_key, 0.0),
        }
        fg_supported = signal.fg_release_units
        dp_supported = max(
            signal.fg_release_units - clipped_quantities_by_stage["DP"] * config.conversion.dp_to_fg_yield,
            0.0,
        )
        ds_supported = max(
            signal.fg_release_units - clipped_quantities_by_stage["DS"] / _ds_mg_per_fg_unit(config),
            0.0,
        )
        ss_supported = max(
            signal.fg_release_units - clipped_quantities_by_stage["SS"] / config.conversion.ss_ratio_to_fg,
            0.0,
        )
        actual_fg_supported = min(fg_supported, dp_supported, ds_supported, ss_supported)
        unmet_demand_units = max(signal.fg_release_units - actual_fg_supported, 0.0)
        capacity_flag = any(
            clipped_quantity > config.review.capacity_clip_tolerance_units
            for clipped_quantity in clipped_quantities_by_stage.values()
        )
        prehorizon_start_present = any(
            request.start_month_index < 1
            for request in (*fg_requests, *dp_requests, *ds_requests, *ss_requests)
            if request.scenario_name == signal.scenario_name
            and request.geography_code == signal.geography_code
            and request.module == signal.module
            and request.demand_month_index == signal.month_index
        )
        supply_gap_flag = unmet_demand_units > config.review.supply_gap_tolerance_units
        bullwhip_review_flag = _has_bullwhip_review_flag(
            signal=signal,
            requests=(*fg_requests, *dp_requests, *ds_requests, *ss_requests),
            threshold=config.review.bullwhip_amplification_threshold,
            review_window_months=config.review.bullwhip_review_window_months,
        )
        support_cumulative_key = (signal.scenario_name, signal.geography_code, signal.module)
        fg_support_cumulative[support_cumulative_key] += signal.fg_release_units
        ss_support_cumulative[support_cumulative_key] += signal.ss_release_units
        cumulative_fg_released = fg_support_cumulative[support_cumulative_key]
        cumulative_ss_released = ss_support_cumulative[support_cumulative_key]
        fg_physical_key = _physical_cumulative_lookup_key("FG", signal.geography_code)
        ss_physical_key = _physical_cumulative_lookup_key("SS", signal.geography_code)
        ss_fg_sync_flag = _cumulative_release_for_month(
            fg_physical_cumulative,
            signal.scenario_name,
            fg_physical_key[0],
            fg_physical_key[1],
            signal.month_index,
        ) <= _cumulative_release_for_month(
            ss_physical_cumulative,
            signal.scenario_name,
            ss_physical_key[0],
            ss_physical_key[1],
            signal.month_index,
        ) + 1e-9

        notes: list[str] = []
        if signal.channel_inventory_build_units > 0:
            notes.append("Channel-build inflation excluded from new FG production and unmet-demand math.")
        if signal.stepdown_applied:
            notes.append("CML_Prevalent step-down hook active.")
        if prehorizon_start_present:
            notes.append("At least one supporting batch start falls before month 1 by design under no-starting-inventory assumptions.")
        if capacity_flag:
            clipped_stage_notes = ", ".join(
                f"{stage}={_format_stage_quantity(quantity)}"
                for stage, quantity in clipped_quantities_by_stage.items()
                if quantity > config.review.capacity_clip_tolerance_units
            )
            notes.append(
                "Supporting stage annual capacity clipped required quantity for this demand month."
                + (f" ({clipped_stage_notes})" if clipped_stage_notes else "")
            )
        if supply_gap_flag:
            notes.append("Scheduled supporting releases did not fully cover underlying patient demand.")

        summary_record = ScheduleMonthlySummaryRecord(
            scenario_name=signal.scenario_name,
            geography_code=signal.geography_code,
            module=signal.module,
            month_index=signal.month_index,
            calendar_month=signal.calendar_month,
            patient_fg_demand_units=signal.patient_fg_demand_units,
            ex_factory_fg_demand_units=signal.ex_factory_fg_demand_units,
            underlying_patient_consumption_units=signal.underlying_patient_consumption_units,
            channel_inventory_build_units=signal.channel_inventory_build_units,
            launch_fill_component_units=signal.launch_fill_component_units,
            fg_release_units=signal.fg_release_units,
            dp_release_units=signal.dp_release_units,
            ds_release_quantity_mg=signal.ds_release_quantity_mg,
            ds_release_quantity_g=signal.ds_release_quantity_mg / 1_000.0,
            ds_release_quantity_kg=signal.ds_release_quantity_mg / 1_000_000.0,
            ss_release_units=signal.ss_release_units,
            cumulative_fg_released=cumulative_fg_released,
            cumulative_ss_released=cumulative_ss_released,
            unmet_demand_units=unmet_demand_units,
            capacity_flag=capacity_flag,
            supply_gap_flag=supply_gap_flag,
            excess_build_flag=_is_excess_build(
                signal.channel_inventory_build_units,
                signal.underlying_patient_consumption_units,
                config.review.excess_build_threshold_ratio,
            ),
            bullwhip_review_flag=bullwhip_review_flag,
            ss_fg_sync_flag=ss_fg_sync_flag,
            stepdown_applied=signal.stepdown_applied,
            notes=" | ".join(notes),
        )
        summary_lookup[signal_key] = summary_record
        summary_records.append(summary_record)

    detail_records = _build_detail_records(
        batches=(*fg_batches, *dp_batches, *ds_batches, *ss_batches),
        summary_lookup=summary_lookup,
    )
    allocation_records = _build_allocation_records(
        allocations=(*fg_allocations, *dp_allocations, *ds_allocations, *ss_allocations),
    )
    return (
        tuple(detail_records),
        tuple(sorted(summary_records, key=lambda item: (item.month_index, item.geography_code, item.module))),
        tuple(allocation_records),
    )


def _resolve_signals(config: Phase4Config, signals: tuple[SchedulingSignal, ...]) -> tuple[SchedulingSignal, ...]:
    resolved: list[SchedulingSignal] = []
    for signal in signals:
        stepdown_applied = (
            signal.module == "CML_Prevalent"
            and config.stepdown.projected_cml_prevalent_bolus_exhaustion_month_index > 0
            and signal.month_index
            >= config.stepdown.projected_cml_prevalent_bolus_exhaustion_month_index
            - config.stepdown.cml_prevalent_forward_window_months
            and signal.month_index
            <= config.stepdown.projected_cml_prevalent_bolus_exhaustion_month_index
        )
        fg_release_units = signal.underlying_patient_consumption_units
        dp_release_units = fg_release_units / config.conversion.dp_to_fg_yield
        ds_release_quantity_mg = dp_release_units * _ds_mg_per_dp_unit(config)
        ss_release_units = fg_release_units * config.conversion.ss_ratio_to_fg
        notes = signal.notes
        if stepdown_applied:
            notes = (notes + " | " if notes else "") + (
                "CML_Prevalent step-down window active; schedule remains tied to underlying patient demand."
            )
        resolved.append(
            SchedulingSignal(
                scenario_name=signal.scenario_name,
                geography_code=signal.geography_code,
                module=signal.module,
                month_index=signal.month_index,
                calendar_month=signal.calendar_month,
                patient_fg_demand_units=signal.patient_fg_demand_units,
                ex_factory_fg_demand_units=signal.ex_factory_fg_demand_units,
                launch_fill_component_units=signal.launch_fill_component_units,
                bullwhip_amplification_factor=signal.bullwhip_amplification_factor,
                underlying_patient_consumption_units=signal.underlying_patient_consumption_units,
                channel_inventory_build_units=signal.channel_inventory_build_units,
                fg_release_units=fg_release_units,
                dp_release_units=dp_release_units,
                ds_release_quantity_mg=ds_release_quantity_mg,
                ss_release_units=ss_release_units,
                stepdown_applied=stepdown_applied,
                notes=notes,
            )
        )
    return tuple(resolved)


def _build_stage_requests(
    anchor_date: date,
    signals: tuple[SchedulingSignal, ...],
    stage_config: StagePlanConfig,
    quantity_field: str,
    config: Phase4Config,
) -> tuple[StageBatchRequest, ...]:
    requests: list[StageBatchRequest] = []
    for signal in signals:
        quantity = float(getattr(signal, quantity_field))
        if quantity <= 0:
            continue
        release_month_index = signal.month_index - stage_config.release_offset_months
        start_month_index = signal.month_index - stage_config.start_offset_months
        requests.append(
            StageBatchRequest(
                scenario_name=signal.scenario_name,
                stage=stage_config.stage,
                module=signal.module,
                geography_code=signal.geography_code,
                demand_month_index=signal.month_index,
                demand_calendar_month=signal.calendar_month,
                release_month_index=release_month_index,
                release_calendar_month=_date_for_month_index(anchor_date, release_month_index),
                start_month_index=start_month_index,
                start_calendar_month=_date_for_month_index(anchor_date, start_month_index),
                quantity=quantity,
                quantity_unit=stage_config.quantity_unit,
                bullwhip_amplification_factor=signal.bullwhip_amplification_factor,
                excess_build_flag=_is_excess_build(
                    signal.channel_inventory_build_units,
                    signal.underlying_patient_consumption_units,
                    config.review.excess_build_threshold_ratio,
                ),
                stepdown_applied=signal.stepdown_applied,
            )
        )
    return tuple(requests)


def _plan_stage_batches(
    requests: tuple[StageBatchRequest, ...],
    stage_config: StagePlanConfig,
) -> tuple[
    tuple[PlannedStageBatch, ...],
    tuple[PlannedStageAllocation, ...],
    dict[tuple[str, str, str, int], float],
]:
    if not requests:
        return (), (), {}

    grouped_requests: dict[tuple[str, str, str, str, int], list[StageBatchRequest]] = defaultdict(list)
    for request in requests:
        physical_module, physical_geography_code = _physical_schedule_grain(
            stage=request.stage,
            geography_code=request.geography_code,
        )
        grouped_requests[
            (
                request.scenario_name,
                request.stage,
                physical_module,
                physical_geography_code,
                request.release_calendar_month.year,
            )
        ].append(request)

    planned_batches: list[PlannedStageBatch] = []
    planned_allocations: list[PlannedStageAllocation] = []
    dropped_by_signal: dict[tuple[str, str, str, int], float] = defaultdict(float)
    for (
        scenario_name,
        stage,
        physical_module,
        physical_geography_code,
        _release_year,
    ), year_requests in grouped_requests.items():
        sorted_requests = sorted(year_requests, key=lambda item: (item.release_month_index, item.demand_month_index))
        total_required_quantity = sum(request.quantity for request in sorted_requests)
        if total_required_quantity <= 0:
            continue

        preferred_batch_count = _preferred_physical_batch_count(total_required_quantity, stage_config)
        physical_batch_count = preferred_batch_count
        effective_campaign_batch_count = max(preferred_batch_count, stage_config.min_campaign_batches)
        capacity_flag = False
        if (
            stage_config.annual_capacity_batches is not None
            and physical_batch_count > stage_config.annual_capacity_batches
        ):
            physical_batch_count = stage_config.annual_capacity_batches
            effective_campaign_batch_count = max(
                physical_batch_count,
                min(stage_config.min_campaign_batches, stage_config.annual_capacity_batches),
            )
            capacity_flag = True

        supported_total_quantity = min(
            total_required_quantity,
            _maximum_supported_quantity(physical_batch_count, stage_config),
        )
        supported_requests = _allocate_supported_request_quantities(
            requests=sorted_requests,
            supported_total_quantity=supported_total_quantity,
            dropped_by_signal=dropped_by_signal,
        )
        if not supported_requests:
            continue

        batch_quantities = _build_physical_batch_quantities(
            total_quantity=supported_total_quantity,
            physical_batch_count=physical_batch_count,
            min_batch_quantity=stage_config.min_batch_quantity,
            max_batch_quantity=stage_config.max_batch_quantity,
            fixed_batch_quantity=stage_config.fixed_batch_quantity,
        )
        batch_assignments = _assign_supported_requests_to_batches(
            supported_requests=supported_requests,
            batch_quantities=batch_quantities,
        )

        for batch_index, (batch_quantity, batch_allocations) in enumerate(
            zip(batch_quantities, batch_assignments, strict=True),
            start=1,
        ):
            first_allocation = batch_allocations[0]
            last_allocation = batch_allocations[-1]
            planned_batches.append(
                PlannedStageBatch(
                    scenario_name=scenario_name,
                    stage=stage,
                    module=physical_module,
                    geography_code=physical_geography_code,
                    support_start_month_index=first_allocation.demand_month_index,
                    support_start_calendar_month=first_allocation.demand_calendar_month,
                    support_end_month_index=last_allocation.demand_month_index,
                    support_end_calendar_month=last_allocation.demand_calendar_month,
                    release_month_index=first_allocation.release_month_index,
                    release_calendar_month=first_allocation.release_calendar_month,
                    start_month_index=first_allocation.start_month_index,
                    start_calendar_month=first_allocation.start_calendar_month,
                    quantity=batch_quantity,
                    allocated_support_quantity=sum(
                        allocation.quantity for allocation in batch_allocations
                    ),
                    quantity_unit=first_allocation.quantity_unit,
                    stepdown_applied=any(
                        allocation.stepdown_applied for allocation in batch_allocations
                    ),
                    excess_build_flag=any(
                        allocation.excess_build_flag for allocation in batch_allocations
                    ),
                    batch_index_in_year=batch_index,
                    capacity_limit=float(stage_config.annual_capacity_batches or stage_config.max_batch_quantity),
                    capacity_metric=(
                        "batches_per_year"
                        if stage_config.annual_capacity_batches is not None
                        else "units_per_campaign"
                    ),
                    capacity_flag=capacity_flag,
                    supported_signal_keys=tuple(
                        sorted(
                            {
                                (
                                    allocation.scenario_name,
                                    allocation.geography_code,
                                    allocation.module,
                                    allocation.demand_month_index,
                                )
                                for allocation in batch_allocations
                            }
                        )
                    ),
                )
            )
            for allocation in batch_allocations:
                planned_allocations.append(
                    PlannedStageAllocation(
                        scenario_name=scenario_name,
                        stage=stage,
                        module=physical_module,
                        geography_code=physical_geography_code,
                        allocated_module=allocation.module,
                        allocated_geography_code=allocation.geography_code,
                        batch_index_in_year=batch_index,
                        quantity_unit=first_allocation.quantity_unit,
                        physical_batch_quantity=batch_quantity,
                        start_month_index=first_allocation.start_month_index,
                        start_calendar_month=first_allocation.start_calendar_month,
                        release_month_index=first_allocation.release_month_index,
                        release_calendar_month=first_allocation.release_calendar_month,
                        demand_month_index=allocation.demand_month_index,
                        demand_calendar_month=allocation.demand_calendar_month,
                        allocated_support_quantity=allocation.quantity,
                        stepdown_applied=allocation.stepdown_applied,
                        excess_build_flag=allocation.excess_build_flag,
                    )
                )

    planned_batches.sort(
        key=lambda item: (
            item.release_month_index,
            item.stage,
            item.geography_code,
            item.module,
            item.batch_index_in_year,
        )
    )
    planned_allocations.sort(
        key=lambda item: (
            item.release_month_index,
            item.stage,
            item.geography_code,
            item.module,
            item.batch_index_in_year,
            item.allocated_geography_code,
            item.allocated_module,
            item.demand_month_index,
        )
    )
    return tuple(planned_batches), tuple(planned_allocations), dropped_by_signal


def _allocate_supported_request_quantities(
    *,
    requests: list[StageBatchRequest],
    supported_total_quantity: float,
    dropped_by_signal: dict[tuple[str, str, str, int], float],
) -> list[StageBatchRequest]:
    remaining_supported_quantity = supported_total_quantity
    supported_requests: list[StageBatchRequest] = []
    for request in requests:
        supported_quantity = min(request.quantity, max(remaining_supported_quantity, 0.0))
        unsupported_quantity = max(request.quantity - supported_quantity, 0.0)
        if supported_quantity > 0:
            supported_requests.append(_copy_request_with_quantity(request, supported_quantity))
        if unsupported_quantity > 0:
            dropped_by_signal[
                (
                    request.scenario_name,
                    request.geography_code,
                    request.module,
                    request.demand_month_index,
                )
            ] += unsupported_quantity
        remaining_supported_quantity -= supported_quantity
    return supported_requests


def _preferred_physical_batch_count(
    total_quantity: float,
    stage_config: StagePlanConfig,
) -> int:
    if total_quantity <= 0:
        return 0
    if stage_config.fixed_batch_quantity is not None:
        return max(1, ceil(total_quantity / stage_config.fixed_batch_quantity))
    return max(1, ceil(total_quantity / stage_config.max_batch_quantity))


def _maximum_supported_quantity(
    physical_batch_count: int,
    stage_config: StagePlanConfig,
) -> float:
    batch_capacity = stage_config.fixed_batch_quantity or stage_config.max_batch_quantity
    return physical_batch_count * batch_capacity


def _build_physical_batch_quantities(
    *,
    total_quantity: float,
    physical_batch_count: int,
    min_batch_quantity: float | None,
    max_batch_quantity: float,
    fixed_batch_quantity: float | None,
) -> list[float]:
    if total_quantity <= 0 or physical_batch_count <= 0:
        return []
    if fixed_batch_quantity is not None:
        return [fixed_batch_quantity for _ in range(physical_batch_count)]

    minimum_quantity = min_batch_quantity or 0.0
    if physical_batch_count == 1:
        return [min(max(total_quantity, minimum_quantity), max_batch_quantity)]

    remaining_quantity = total_quantity
    remaining_batches = physical_batch_count
    quantities: list[float] = []
    for _ in range(physical_batch_count):
        minimum_remaining = minimum_quantity * (remaining_batches - 1)
        target_quantity = remaining_quantity / remaining_batches
        quantity = min(
            max_batch_quantity,
            max(target_quantity, minimum_quantity, remaining_quantity - minimum_remaining),
        )
        quantities.append(quantity)
        remaining_quantity -= quantity
        remaining_batches -= 1
    return quantities


def _assign_supported_requests_to_batches(
    *,
    supported_requests: list[StageBatchRequest],
    batch_quantities: list[float],
) -> list[list[StageBatchRequest]]:
    if not supported_requests:
        return []
    assignments: list[list[StageBatchRequest]] = []
    request_index = 0
    request_consumed = 0.0
    for batch_quantity in batch_quantities:
        remaining_batch_quantity = batch_quantity
        batch_assignments: list[StageBatchRequest] = []
        while remaining_batch_quantity > 1e-9 and request_index < len(supported_requests):
            request = supported_requests[request_index]
            remaining_request_quantity = request.quantity - request_consumed
            if remaining_request_quantity <= 1e-9:
                request_index += 1
                request_consumed = 0.0
                continue
            allocation_quantity = min(remaining_batch_quantity, remaining_request_quantity)
            batch_assignments.append(_copy_request_with_quantity(request, allocation_quantity))
            remaining_batch_quantity -= allocation_quantity
            request_consumed += allocation_quantity
            if request_consumed + 1e-9 >= request.quantity:
                request_index += 1
                request_consumed = 0.0
        assignments.append(batch_assignments)
    return assignments


def _copy_request_with_quantity(request: StageBatchRequest, quantity: float) -> StageBatchRequest:
    return StageBatchRequest(
        scenario_name=request.scenario_name,
        stage=request.stage,
        module=request.module,
        geography_code=request.geography_code,
        demand_month_index=request.demand_month_index,
        demand_calendar_month=request.demand_calendar_month,
        release_month_index=request.release_month_index,
        release_calendar_month=request.release_calendar_month,
        start_month_index=request.start_month_index,
        start_calendar_month=request.start_calendar_month,
        quantity=quantity,
        quantity_unit=request.quantity_unit,
        bullwhip_amplification_factor=request.bullwhip_amplification_factor,
        excess_build_flag=request.excess_build_flag,
        stepdown_applied=request.stepdown_applied,
    )


def _build_detail_records(
    *,
    batches: tuple[PlannedStageBatch, ...],
    summary_lookup: dict[tuple[str, str, str, int], ScheduleMonthlySummaryRecord],
) -> list[ScheduleDetailRecord]:
    stage_running_totals: dict[tuple[str, str, str, str], float] = defaultdict(float)
    detail_records: list[ScheduleDetailRecord] = []
    for batch in sorted(
        batches,
        key=lambda item: (
            item.release_month_index,
            item.stage,
            item.geography_code,
            item.module,
            item.batch_index_in_year,
        ),
    ):
        running_key = (batch.scenario_name, batch.stage, batch.geography_code, batch.module)
        stage_running_totals[running_key] += batch.quantity
        supported_summary_records = [
            summary_lookup[supported_signal_key]
            for supported_signal_key in batch.supported_signal_keys
            if supported_signal_key in summary_lookup
        ]
        detail_records.append(
            ScheduleDetailRecord(
                scenario_name=batch.scenario_name,
                stage=batch.stage,
                module=batch.module,
                geography_code=batch.geography_code,
                batch_number=_batch_number(batch),
                demand_month_index=batch.support_start_month_index,
                demand_calendar_month=batch.support_start_calendar_month,
                support_start_month_index=batch.support_start_month_index,
                support_start_calendar_month=batch.support_start_calendar_month,
                support_end_month_index=batch.support_end_month_index,
                support_end_calendar_month=batch.support_end_calendar_month,
                month_index=batch.release_month_index,
                calendar_month=batch.release_calendar_month,
                planned_start_month_index=batch.start_month_index,
                planned_start_month=batch.start_calendar_month,
                planned_release_month_index=batch.release_month_index,
                planned_release_month=batch.release_calendar_month,
                batch_quantity=batch.quantity,
                allocated_support_quantity=batch.allocated_support_quantity,
                quantity_unit=batch.quantity_unit,
                cumulative_released_quantity=stage_running_totals[running_key],
                capacity_used=(
                    float(batch.batch_index_in_year)
                    if batch.capacity_metric == "batches_per_year"
                    else batch.quantity
                ),
                capacity_limit=batch.capacity_limit,
                capacity_metric=batch.capacity_metric,
                capacity_flag=any(record.capacity_flag for record in supported_summary_records) or batch.capacity_flag,
                supply_gap_flag=any(record.supply_gap_flag for record in supported_summary_records),
                excess_build_flag=batch.excess_build_flag,
                bullwhip_review_flag=any(
                    record.bullwhip_review_flag for record in supported_summary_records
                ),
                ss_fg_sync_flag=all(record.ss_fg_sync_flag for record in supported_summary_records)
                if supported_summary_records
                else True,
                notes=_detail_notes(batch, supported_summary_records),
            )
        )
    return detail_records


def _build_allocation_records(
    *,
    allocations: tuple[PlannedStageAllocation, ...],
) -> list[ScheduleAllocationRecord]:
    records: list[ScheduleAllocationRecord] = []
    for allocation in allocations:
        source_batch_number = (
            f"{allocation.stage}-{allocation.module}-{allocation.geography_code}-"
            f"{allocation.release_calendar_month.year}-{allocation.batch_index_in_year:03d}"
        )
        notes: list[str] = []
        if allocation.stepdown_applied:
            notes.append("Step-down window active.")
        if allocation.excess_build_flag:
            notes.append("Allocated demand month exceeded excess-build threshold.")
        records.append(
            ScheduleAllocationRecord(
                scenario_name=allocation.scenario_name,
                stage=allocation.stage,
                module=allocation.module,
                geography_code=allocation.geography_code,
                allocated_module=allocation.allocated_module,
                allocated_geography_code=allocation.allocated_geography_code,
                source_batch_number=source_batch_number,
                physical_batch_quantity=allocation.physical_batch_quantity,
                quantity_unit=allocation.quantity_unit,
                planned_start_month_index=allocation.start_month_index,
                planned_start_month=allocation.start_calendar_month,
                planned_release_month_index=allocation.release_month_index,
                planned_release_month=allocation.release_calendar_month,
                allocated_to_demand_month_index=allocation.demand_month_index,
                allocated_to_demand_calendar_month=allocation.demand_calendar_month,
                allocated_support_quantity=allocation.allocated_support_quantity,
                notes=" | ".join(notes),
            )
        )
    return records


def _derive_anchor_date(signals: tuple[SchedulingSignal, ...]) -> date:
    if not signals:
        return date(2029, 1, 1)
    first_signal = min(signals, key=lambda item: item.month_index)
    return _add_months(first_signal.calendar_month, -(first_signal.month_index - 1))


def _months_from_weeks(weeks: float, weeks_per_month: float) -> int:
    return int(ceil(weeks / weeks_per_month))


def _date_for_month_index(anchor_date: date, month_index: int) -> date:
    return _add_months(anchor_date, month_index - 1)


def _add_months(value: date, offset: int) -> date:
    month_number = (value.year * 12 + (value.month - 1)) + offset
    year, month_zero_based = divmod(month_number, 12)
    return date(year, month_zero_based + 1, 1)


def _ds_mg_per_dp_unit(config: Phase4Config) -> float:
    return (
        config.conversion.ds_qty_per_dp_unit_mg
        / config.conversion.ds_to_dp_yield
        * (1.0 + config.conversion.ds_overage_factor)
    )


def _ds_mg_per_fg_unit(config: Phase4Config) -> float:
    return _ds_mg_per_dp_unit(config) / config.conversion.dp_to_fg_yield


def _is_excess_build(
    channel_inventory_build_units: float,
    underlying_patient_consumption_units: float,
    threshold_ratio: float,
) -> bool:
    if channel_inventory_build_units <= 0:
        return False
    if underlying_patient_consumption_units <= 0:
        return True
    return channel_inventory_build_units > underlying_patient_consumption_units * threshold_ratio


def _physical_schedule_grain(*, stage: str, geography_code: str) -> tuple[str, str]:
    if stage in ("DS", "DP"):
        return PHYSICAL_SHARED_MODULE, PHYSICAL_SHARED_GEOGRAPHY
    return PHYSICAL_SHARED_MODULE, geography_code


def _physical_cumulative_lookup_key(stage: str, geography_code: str) -> tuple[str, str]:
    physical_module, physical_geography_code = _physical_schedule_grain(
        stage=stage,
        geography_code=geography_code,
    )
    return physical_geography_code, physical_module


def _build_cumulative_release_lookup(
    batches: tuple[PlannedStageBatch, ...],
) -> dict[tuple[str, str, str], dict[int, float]]:
    grouped: dict[tuple[str, str, str], dict[int, float]] = defaultdict(dict)
    for batch in batches:
        group_key = (batch.scenario_name, batch.geography_code, batch.module)
        grouped[group_key].setdefault(batch.release_month_index, 0.0)
        grouped[group_key][batch.release_month_index] += batch.quantity
    for by_month in grouped.values():
        cumulative = 0.0
        for month_index in sorted(by_month):
            cumulative += by_month[month_index]
            by_month[month_index] = cumulative
    return grouped


def _cumulative_release_for_month(
    lookup: dict[tuple[str, str, str], dict[int, float]],
    scenario_name: str,
    geography_code: str,
    module: str,
    month_index: int,
) -> float:
    grouped = lookup.get((scenario_name, geography_code, module), {})
    cumulative = 0.0
    for release_month_index in sorted(grouped):
        if release_month_index > month_index:
            break
        cumulative = grouped[release_month_index]
    return cumulative


def _has_bullwhip_review_flag(
    *,
    signal: SchedulingSignal,
    requests: tuple[StageBatchRequest, ...],
    threshold: float,
    review_window_months: int,
) -> bool:
    if signal.bullwhip_amplification_factor <= threshold:
        return False
    for request in requests:
        if request.scenario_name != signal.scenario_name:
            continue
        if request.geography_code != signal.geography_code or request.module != signal.module:
            continue
        if request.demand_month_index != signal.month_index:
            continue
        if request.start_month_index >= signal.month_index - review_window_months:
            return True
    return False


def _batch_number(batch: PlannedStageBatch) -> str:
    return (
        f"{batch.stage}-{batch.module}-{batch.geography_code}-"
        f"{batch.release_calendar_month.year}-{batch.batch_index_in_year:03d}"
    )


def _detail_notes(
    batch: PlannedStageBatch,
    supported_summary_records: list[ScheduleMonthlySummaryRecord],
) -> str:
    notes: list[str] = []
    if batch.stage in ("DS", "DP") and batch.geography_code == PHYSICAL_SHARED_GEOGRAPHY:
        notes.append(
            "Physical batch is scheduled in the shared manufacturing pool across all modules and geographies; see phase4_allocation_detail.csv for downstream allocation traceability."
        )
    elif batch.stage in ("FG", "SS") and batch.module == PHYSICAL_SHARED_MODULE:
        notes.append(
            "Physical batch is scheduled at geography-level finished-goods packaging grain across all modules in the geography; see phase4_allocation_detail.csv for downstream allocation traceability."
        )
    if batch.support_end_month_index > batch.support_start_month_index:
        notes.append(
            "Physical batch supports multiple demand months; see phase4_allocation_detail.csv for the per-demand allocation trace."
        )
    if batch.allocated_support_quantity + 1e-9 < batch.quantity:
        notes.append(
            "Physical batch quantity exceeds currently allocated support due to deterministic batch-size rounding or fixed-batch assumptions."
        )
    if batch.stepdown_applied:
        notes.append("Step-down window active.")
    if any(record.capacity_flag for record in supported_summary_records) or batch.capacity_flag:
        notes.append("Supporting stage annual capacity clipped required quantity for this demand month.")
    if any(record.supply_gap_flag for record in supported_summary_records):
        notes.append("Supporting stage releases did not fully cover underlying patient demand.")
    if batch.start_month_index < 1:
        notes.append("Planned start precedes month 1 by design under no-starting-inventory assumptions.")
    if batch.excess_build_flag:
        notes.append("Associated demand month exceeded excess-build threshold.")
    return " | ".join(notes)


def _format_stage_quantity(value: float) -> str:
    return f"{value:.6f}"
