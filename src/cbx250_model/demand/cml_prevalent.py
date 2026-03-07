"""CML prevalent demand module scaffold."""

from ..calendar.monthly_calendar import MonthlyCalendar
from ..constants import FORECAST_GRAIN_MODULE_LEVEL
from ..inputs.config_schema import Phase1Config
from ..inputs.loaders import InputBundle
from .base import DemandOutputRecord, DeterministicDemandModule


class CMLPrevalentDemandModule(DeterministicDemandModule):
    module_name = "CML_Prevalent"

    def build(
        self,
        config: Phase1Config,
        calendar: MonthlyCalendar,
        *,
        inputs: InputBundle,
    ) -> tuple[DemandOutputRecord, ...]:
        if config.model.forecast_grain == FORECAST_GRAIN_MODULE_LEVEL:
            return self._pass_through_module_level(config, inputs.module_level_forecast, calendar)
        return self._pass_through_segment_level(config, inputs.segment_level_forecast, calendar)
