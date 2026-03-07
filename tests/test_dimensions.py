from __future__ import annotations

from datetime import date

from cbx250_model.calendar.monthly_calendar import build_monthly_calendar
from cbx250_model.dimensions.tables import build_dimensions
from cbx250_model.inputs.loaders import InputBundle
from cbx250_model.inputs.schemas import ModuleLevelForecastRecord


def test_dimensions_include_modules_segments_and_input_geographies() -> None:
    inputs = InputBundle(
        module_level_forecast=(
            ModuleLevelForecastRecord(
                geography_code="US",
                module="AML",
                month_index=1,
                patients_treated=10.0,
            ),
        ),
        segment_level_forecast=tuple(),
        epi_crosscheck=tuple(),
        aml_segment_mix=tuple(),
        mds_segment_mix=tuple(),
        cml_prevalent=tuple(),
    )
    calendar = build_monthly_calendar(date(2029, 1, 1), 240)

    dimensions = build_dimensions(calendar, inputs)

    assert {row["module"] for row in dimensions["dim_module"]} == {
        "AML",
        "MDS",
        "CML_Incident",
        "CML_Prevalent",
    }
    assert any(row["segment_code"] == "1L_fit" for row in dimensions["dim_segment"])
    assert dimensions["dim_geography"] == [{"geography_code": "US"}]
