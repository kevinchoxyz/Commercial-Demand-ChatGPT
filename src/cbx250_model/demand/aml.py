"""AML demand module scaffold."""

from ..constants import AML_SEGMENTS, FORECAST_GRAIN_MODULE_LEVEL
from ..inputs.config_schema import Phase1Config
from ..inputs.loaders import InputBundle
from .base import DemandOutputRecord, DeterministicDemandModule
from ..calendar.monthly_calendar import MonthlyCalendar


class AMLDemandModule(DeterministicDemandModule):
    module_name = "AML"

    def build(
        self,
        config: Phase1Config,
        calendar: MonthlyCalendar,
        *,
        inputs: InputBundle,
    ) -> tuple[DemandOutputRecord, ...]:
        if config.model.forecast_grain != FORECAST_GRAIN_MODULE_LEVEL:
            return self._pass_through_segment_level(config, inputs.segment_level_forecast, calendar)

        mix_lookup = self._build_allocatable_mix_lookup(inputs.aml_segment_mix, AML_SEGMENTS)
        outputs: list[DemandOutputRecord] = []
        for record in inputs.module_level_forecast:
            if record.module != self.module_name:
                continue
            for mix_record in mix_lookup.get((record.geography_code, record.month_index), tuple()):
                outputs.append(
                    self._build_output(
                        scenario_name=config.scenario_name,
                        geography_code=record.geography_code,
                        segment_code=mix_record.segment_code,
                        month_index=record.month_index,
                        patients_treated=record.patients_treated * mix_record.segment_share,
                        calendar=calendar,
                    )
                )
        return tuple(outputs)
