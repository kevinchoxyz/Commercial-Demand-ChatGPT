"""Base demand module behavior."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date

from ..calendar.monthly_calendar import MonthlyCalendar
from ..constants import DEMAND_BASIS_TREATED_CENSUS, MODULE_TO_INDICATION, MODULE_TO_SEGMENTS
from ..inputs.config_schema import Phase1Config
from ..inputs.schemas import ModuleLevelForecastRecord, SegmentLevelForecastRecord, SegmentMixRecord


@dataclass(frozen=True)
class DemandOutputRecord:
    scenario_name: str
    geography_code: str
    indication_code: str
    module: str
    segment_code: str
    month_index: int
    month_start: date
    patients_treated: float
    demand_basis_used: str = DEMAND_BASIS_TREATED_CENSUS
    patient_starts: float = 0.0
    patients_continuing: float = 0.0
    patients_rolloff: float = 0.0
    treatment_duration_months_used: int | None = None

    @property
    def key(self) -> tuple[str, str, str, str, int]:
        return (
            self.scenario_name,
            self.geography_code,
            self.module,
            self.segment_code,
            self.month_index,
        )

    @property
    def patients_active(self) -> float:
        return self.patients_treated

    @property
    def starts_input(self) -> float:
        return self.patient_starts

    @property
    def continuing_patients(self) -> float:
        return self.patients_continuing

    @property
    def rolloff_patients(self) -> float:
        return self.patients_rolloff


class DeterministicDemandModule:
    module_name: str

    def build(self, config: Phase1Config, calendar: MonthlyCalendar, **_: object) -> tuple[DemandOutputRecord, ...]:
        raise NotImplementedError

    def _build_output(
        self,
        scenario_name: str,
        geography_code: str,
        segment_code: str,
        month_index: int,
        patients_treated: float,
        calendar: MonthlyCalendar,
        demand_basis_used: str = DEMAND_BASIS_TREATED_CENSUS,
        patient_starts: float = 0.0,
        patients_continuing: float = 0.0,
        patients_rolloff: float = 0.0,
        treatment_duration_months_used: int | None = None,
    ) -> DemandOutputRecord:
        month = calendar.get_month(month_index)
        return DemandOutputRecord(
            scenario_name=scenario_name,
            geography_code=geography_code,
            indication_code=MODULE_TO_INDICATION[self.module_name],
            module=self.module_name,
            segment_code=segment_code,
            month_index=month_index,
            month_start=month.month_start,
            patients_treated=patients_treated,
            demand_basis_used=demand_basis_used,
            patient_starts=patient_starts,
            patients_continuing=patients_continuing,
            patients_rolloff=patients_rolloff,
            treatment_duration_months_used=treatment_duration_months_used,
        )

    def _pass_through_segment_level(
        self,
        config: Phase1Config,
        forecast_rows: tuple[SegmentLevelForecastRecord, ...],
        calendar: MonthlyCalendar,
    ) -> tuple[DemandOutputRecord, ...]:
        outputs: list[DemandOutputRecord] = []
        for record in forecast_rows:
            if record.module != self.module_name:
                continue
            outputs.append(
                self._build_output(
                    scenario_name=config.scenario_name,
                    geography_code=record.geography_code,
                    segment_code=record.segment_code,
                    month_index=record.month_index,
                    patients_treated=record.patients_treated,
                    calendar=calendar,
                )
            )
        return tuple(outputs)

    def _pass_through_module_level(
        self,
        config: Phase1Config,
        forecast_rows: tuple[ModuleLevelForecastRecord, ...],
        calendar: MonthlyCalendar,
        default_segment_code: str | None = None,
    ) -> tuple[DemandOutputRecord, ...]:
        segment_code = default_segment_code or MODULE_TO_SEGMENTS[self.module_name][0]
        outputs: list[DemandOutputRecord] = []
        for record in forecast_rows:
            if record.module != self.module_name:
                continue
            outputs.append(
                self._build_output(
                    scenario_name=config.scenario_name,
                    geography_code=record.geography_code,
                    segment_code=segment_code,
                    month_index=record.month_index,
                    patients_treated=record.patients_treated,
                    calendar=calendar,
                )
            )
        return tuple(outputs)

    @staticmethod
    def _build_allocatable_mix_lookup(
        mix_rows: tuple[SegmentMixRecord, ...],
        expected_segments: tuple[str, ...],
    ) -> dict[tuple[str, int], tuple[SegmentMixRecord, ...]]:
        grouped: dict[tuple[str, int], list[SegmentMixRecord]] = defaultdict(list)
        for record in mix_rows:
            grouped[(record.geography_code, record.month_index)].append(record)

        order = {segment_code: index for index, segment_code in enumerate(expected_segments)}
        lookup: dict[tuple[str, int], tuple[SegmentMixRecord, ...]] = {}
        for key, group in grouped.items():
            seen_segments = {record.segment_code for record in group}
            total_share = sum(record.segment_share for record in group)
            if (
                len(group) == len(expected_segments)
                and seen_segments == set(expected_segments)
                and abs(total_share - 1.0) <= 1e-9
            ):
                lookup[key] = tuple(sorted(group, key=lambda record: order[record.segment_code]))
        return lookup
