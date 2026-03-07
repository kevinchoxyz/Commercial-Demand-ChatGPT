"""Monthly calendar generation for the Phase 1 scaffold."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _add_months(value: date, month_offset: int) -> date:
    year_offset, zero_based_month = divmod((value.month - 1) + month_offset, 12)
    return date(value.year + year_offset, zero_based_month + 1, 1)


@dataclass(frozen=True)
class CalendarMonth:
    month_index: int
    month_start: date
    month_id: str
    calendar_year: int
    calendar_month: int
    calendar_quarter: str


@dataclass(frozen=True)
class MonthlyCalendar:
    anchor_date: date
    months: tuple[CalendarMonth, ...]

    def get_month(self, month_index: int) -> CalendarMonth:
        if month_index < 1 or month_index > len(self.months):
            raise ValueError(
                f"month_index must be between 1 and {len(self.months)}, received {month_index}."
            )
        return self.months[month_index - 1]

    def month_starts(self) -> set[date]:
        return {month.month_start for month in self.months}

    def month_indices(self) -> set[int]:
        return {month.month_index for month in self.months}


def build_monthly_calendar(anchor_date: date, forecast_horizon_months: int) -> MonthlyCalendar:
    calendar_start = _month_start(anchor_date)
    months: list[CalendarMonth] = []
    for index in range(forecast_horizon_months):
        month_start = _add_months(calendar_start, index)
        quarter = ((month_start.month - 1) // 3) + 1
        months.append(
            CalendarMonth(
                month_index=index + 1,
                month_start=month_start,
                month_id=month_start.strftime("%Y-%m"),
                calendar_year=month_start.year,
                calendar_month=month_start.month,
                calendar_quarter=f"{month_start.year}-Q{quarter}",
            )
        )
    return MonthlyCalendar(anchor_date=anchor_date, months=tuple(months))
