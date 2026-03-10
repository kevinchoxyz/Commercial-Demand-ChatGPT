"""Deterministic Phase 4 production scheduling."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from math import ceil

from .config_schema import Phase4Config
from .schemas import ScheduleDetailRecord, ScheduleMonthlySummaryRecord, SchedulingSignal


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
    demand_month_index: int
    demand_calendar_month: date
    release_month_index: int
    release_calendar_month: date
    start_month_index: int
    start_calendar_month: date
    quantity: float
    quantity_unit: str
    stepdown_applied: bool
    excess_build_flag: bool
    batch_index_in_year: int
    capacity_limit: float
    capacity_metric: str
    capacity_flag: bool


def build_phase4_outputs(
    config: Phase4Config,
    signals: tuple[SchedulingSignal, ...],
) -> tuple[tuple[ScheduleDetailRecord, ...], tuple[ScheduleMonthlySummaryRecord, ...]]:
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

    fg_batches, fg_dropped = _plan_stage_batches(fg_requests, fg_config)
    dp_batches, dp_dropped = _plan_stage_batches(dp_requests, dp_config)
    ds_batches, ds_dropped = _plan_stage_batches(ds_requests, ds_config)
    ss_batches, ss_dropped = _plan_stage_batches(ss_requests, ss_config)

    fg_cumulative = _build_cumulative_release_lookup(fg_batches)
    ss_cumulative = _build_cumulative_release_lookup(ss_batches)

    summary_records: list[ScheduleMonthlySummaryRecord] = []
    summary_lookup: dict[tuple[str, str, str, int], ScheduleMonthlySummaryRecord] = {}
    for signal in resolved_signals:
        signal_key = signal.key
        fg_supported = signal.fg_release_units
        dp_supported = max(
            signal.fg_release_units - dp_dropped.get(signal_key, 0.0) * config.conversion.dp_to_fg_yield,
            0.0,
        )
        ds_supported = max(
            signal.fg_release_units - ds_dropped.get(signal_key, 0.0) / _ds_mg_per_fg_unit(config),
            0.0,
        )
        ss_supported = max(
            signal.fg_release_units - ss_dropped.get(signal_key, 0.0) / config.conversion.ss_ratio_to_fg,
            0.0,
        )
        actual_fg_supported = min(fg_supported, dp_supported, ds_supported, ss_supported)
        unmet_demand_units = max(signal.ex_factory_fg_demand_units - actual_fg_supported, 0.0)
        capacity_flag = any(
            dropped_map.get(signal_key, 0.0) > 0.0
            for dropped_map in (fg_dropped, dp_dropped, ds_dropped, ss_dropped)
        )
        prehorizon_supply_gap = any(
            request.start_month_index < 1
            for request in (*fg_requests, *dp_requests, *ds_requests, *ss_requests)
            if request.scenario_name == signal.scenario_name
            and request.geography_code == signal.geography_code
            and request.module == signal.module
            and request.demand_month_index == signal.month_index
        )
        supply_gap_flag = unmet_demand_units > 0 or capacity_flag or prehorizon_supply_gap
        bullwhip_review_flag = _has_bullwhip_review_flag(
            signal=signal,
            requests=(*fg_requests, *dp_requests, *ds_requests, *ss_requests),
            threshold=config.review.bullwhip_amplification_threshold,
            review_window_months=config.review.bullwhip_review_window_months,
        )
        cumulative_fg_released = _cumulative_release_for_month(
            fg_cumulative,
            signal.scenario_name,
            signal.geography_code,
            signal.module,
            signal.month_index,
        )
        cumulative_ss_released = _cumulative_release_for_month(
            ss_cumulative,
            signal.scenario_name,
            signal.geography_code,
            signal.module,
            signal.month_index,
        )
        ss_fg_sync_flag = cumulative_fg_released <= cumulative_ss_released + 1e-9

        notes: list[str] = []
        if signal.channel_inventory_build_units > 0:
            notes.append("Channel-build inflation excluded from new FG production.")
        if signal.stepdown_applied:
            notes.append("CML_Prevalent step-down hook active.")
        if prehorizon_supply_gap:
            notes.append("At least one supporting batch start falls before month 1.")
        if capacity_flag:
            notes.append("Supporting stage annual capacity clipped at least one batch.")

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
    return tuple(detail_records), tuple(sorted(summary_records, key=lambda item: (item.month_index, item.geography_code, item.module)))


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
) -> tuple[tuple[PlannedStageBatch, ...], dict[tuple[str, str, str, int], float]]:
    grouped_requests: dict[tuple[str, str, str, str, int], list[StageBatchRequest]] = defaultdict(list)
    for request in requests:
        grouped_requests[
            (
                request.scenario_name,
                request.stage,
                request.module,
                request.geography_code,
                request.release_calendar_month.year,
            )
        ].append(request)

    planned_batches: list[PlannedStageBatch] = []
    dropped_by_signal: dict[tuple[str, str, str, int], float] = defaultdict(float)
    for (scenario_name, stage, module, geography_code, _release_year), year_requests in grouped_requests.items():
        all_batches: list[StageBatchRequest] = []
        for request in sorted(year_requests, key=lambda item: (item.release_month_index, item.demand_month_index)):
            all_batches.extend(_split_request_into_batches(request, stage_config))

        if all_batches and stage_config.min_campaign_batches > 0 and len(all_batches) < stage_config.min_campaign_batches:
            all_batches.extend(
                _build_min_campaign_batches(
                    template_request=all_batches[-1],
                    additional_batch_count=stage_config.min_campaign_batches - len(all_batches),
                    stage_config=stage_config,
                )
            )

        capacity_flag = (
            stage_config.annual_capacity_batches is not None
            and len(all_batches) > stage_config.annual_capacity_batches
        )
        kept_batches = all_batches
        if stage_config.annual_capacity_batches is not None and len(all_batches) > stage_config.annual_capacity_batches:
            kept_batches = all_batches[: stage_config.annual_capacity_batches]
            for dropped_batch in all_batches[stage_config.annual_capacity_batches :]:
                dropped_by_signal[
                    (
                        dropped_batch.scenario_name,
                        dropped_batch.geography_code,
                        dropped_batch.module,
                        dropped_batch.demand_month_index,
                    )
                ] += dropped_batch.quantity

        for batch_index, batch in enumerate(kept_batches, start=1):
            planned_batches.append(
                PlannedStageBatch(
                    scenario_name=scenario_name,
                    stage=stage,
                    module=module,
                    geography_code=geography_code,
                    demand_month_index=batch.demand_month_index,
                    demand_calendar_month=batch.demand_calendar_month,
                    release_month_index=batch.release_month_index,
                    release_calendar_month=batch.release_calendar_month,
                    start_month_index=batch.start_month_index,
                    start_calendar_month=batch.start_calendar_month,
                    quantity=batch.quantity,
                    quantity_unit=batch.quantity_unit,
                    stepdown_applied=batch.stepdown_applied,
                    excess_build_flag=batch.excess_build_flag,
                    batch_index_in_year=batch_index,
                    capacity_limit=float(stage_config.annual_capacity_batches or stage_config.max_batch_quantity),
                    capacity_metric=(
                        "batches_per_year"
                        if stage_config.annual_capacity_batches is not None
                        else "units_per_campaign"
                    ),
                    capacity_flag=capacity_flag,
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
    return tuple(planned_batches), dropped_by_signal


def _split_request_into_batches(
    request: StageBatchRequest,
    stage_config: StagePlanConfig,
) -> list[StageBatchRequest]:
    if stage_config.fixed_batch_quantity is not None:
        batch_count = max(1, ceil(request.quantity / stage_config.fixed_batch_quantity))
        return [
            StageBatchRequest(
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
                quantity=stage_config.fixed_batch_quantity,
                quantity_unit=request.quantity_unit,
                bullwhip_amplification_factor=request.bullwhip_amplification_factor,
                excess_build_flag=request.excess_build_flag,
                stepdown_applied=request.stepdown_applied,
            )
            for _ in range(batch_count)
        ]

    batch_count = max(1, ceil(request.quantity / stage_config.max_batch_quantity))
    remaining_quantity = request.quantity
    min_batch_quantity = stage_config.min_batch_quantity or 0.0
    quantities: list[float] = []
    for batch_number in range(batch_count):
        remaining_batches = batch_count - batch_number
        minimum_remaining = min_batch_quantity * (remaining_batches - 1)
        quantity = min(
            stage_config.max_batch_quantity,
            max(remaining_quantity - minimum_remaining, min_batch_quantity),
        )
        quantities.append(quantity)
        remaining_quantity -= quantity
    return [
        StageBatchRequest(
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
        for quantity in quantities
    ]


def _build_min_campaign_batches(
    *,
    template_request: StageBatchRequest,
    additional_batch_count: int,
    stage_config: StagePlanConfig,
) -> list[StageBatchRequest]:
    extra_quantity = stage_config.fixed_batch_quantity or stage_config.min_batch_quantity or stage_config.max_batch_quantity
    return [
        StageBatchRequest(
            scenario_name=template_request.scenario_name,
            stage=template_request.stage,
            module=template_request.module,
            geography_code=template_request.geography_code,
            demand_month_index=template_request.demand_month_index,
            demand_calendar_month=template_request.demand_calendar_month,
            release_month_index=template_request.release_month_index,
            release_calendar_month=template_request.release_calendar_month,
            start_month_index=template_request.start_month_index,
            start_calendar_month=template_request.start_calendar_month,
            quantity=extra_quantity,
            quantity_unit=template_request.quantity_unit,
            bullwhip_amplification_factor=template_request.bullwhip_amplification_factor,
            excess_build_flag=template_request.excess_build_flag,
            stepdown_applied=template_request.stepdown_applied,
        )
        for _ in range(additional_batch_count)
    ]


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
        summary_record = summary_lookup[
            (
                batch.scenario_name,
                batch.geography_code,
                batch.module,
                batch.demand_month_index,
            )
        ]
        detail_records.append(
            ScheduleDetailRecord(
                scenario_name=batch.scenario_name,
                stage=batch.stage,
                module=batch.module,
                geography_code=batch.geography_code,
                batch_number=_batch_number(batch),
                demand_month_index=batch.demand_month_index,
                demand_calendar_month=batch.demand_calendar_month,
                month_index=batch.release_month_index,
                calendar_month=batch.release_calendar_month,
                planned_start_month_index=batch.start_month_index,
                planned_start_month=batch.start_calendar_month,
                planned_release_month_index=batch.release_month_index,
                planned_release_month=batch.release_calendar_month,
                batch_quantity=batch.quantity,
                quantity_unit=batch.quantity_unit,
                cumulative_released_quantity=stage_running_totals[running_key],
                capacity_used=(
                    float(batch.batch_index_in_year)
                    if batch.capacity_metric == "batches_per_year"
                    else batch.quantity
                ),
                capacity_limit=batch.capacity_limit,
                capacity_metric=batch.capacity_metric,
                capacity_flag=batch.capacity_flag,
                supply_gap_flag=summary_record.supply_gap_flag or batch.start_month_index < 1,
                excess_build_flag=summary_record.excess_build_flag,
                bullwhip_review_flag=summary_record.bullwhip_review_flag,
                ss_fg_sync_flag=summary_record.ss_fg_sync_flag,
                notes=_detail_notes(batch),
            )
        )
    return detail_records


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


def _detail_notes(batch: PlannedStageBatch) -> str:
    notes: list[str] = []
    if batch.stepdown_applied:
        notes.append("Step-down window active.")
    if batch.capacity_flag:
        notes.append("Annual capacity limit applied.")
    if batch.start_month_index < 1:
        notes.append("Planned start precedes month 1.")
    if batch.excess_build_flag:
        notes.append("Associated demand month exceeded excess-build threshold.")
    return " | ".join(notes)
