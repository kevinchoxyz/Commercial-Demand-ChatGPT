from __future__ import annotations

from datetime import date

from cbx250_model.calendar.monthly_calendar import build_monthly_calendar


def test_calendar_builds_240_months_from_anchor_month() -> None:
    calendar = build_monthly_calendar(date(2029, 5, 17), 240)

    assert len(calendar.months) == 240
    assert calendar.months[0].month_start == date(2029, 5, 1)
    assert calendar.months[-1].month_start == date(2049, 4, 1)

