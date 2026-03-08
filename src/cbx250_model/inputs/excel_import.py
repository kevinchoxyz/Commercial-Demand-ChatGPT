"""Import the Commercial forecast workbook into normalized Phase 1 CSV inputs."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import csv
import json
import re
import xml.etree.ElementTree as ET
import zipfile

from ..calendar.monthly_calendar import MonthlyCalendar, build_monthly_calendar
from ..constants import (
    AML_SEGMENTS,
    FORECAST_FREQUENCY_ANNUAL,
    FORECAST_FREQUENCY_MONTHLY,
    FORECAST_GRAIN_MODULE_LEVEL,
    FORECAST_GRAIN_SEGMENT_LEVEL,
    MDS_SEGMENTS,
    PHASE1_HORIZON_MONTHS,
    PHASE1_MODULES,
    SUPPORTED_FORECAST_FREQUENCIES,
    SUPPORTED_FORECAST_GRAINS,
)
from .schemas import CMLPrevalentPoolRecord, ModuleLevelForecastRecord, SegmentLevelForecastRecord

XML_NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
CELL_REF_PATTERN = re.compile(r"^(?P<column>[A-Z]+)(?P<row>\d+)$")
EXCEL_EPOCH = date(1899, 12, 30)
MODULE_LEVEL_HEADERS = (
    "scenario_name",
    "geography_code",
    "module",
    "month_index",
    "calendar_month",
    "patients_treated",
    "notes",
)
SEGMENT_LEVEL_HEADERS = (
    "scenario_name",
    "geography_code",
    "module",
    "segment_code",
    "month_index",
    "calendar_month",
    "patients_treated",
    "notes",
)
ANNUAL_MODULE_LEVEL_HEADERS = (
    "scenario_name",
    "geography_code",
    "module",
    "year_index",
    "calendar_year",
    "patients_treated_annual",
    "monthlyization_profile_id",
    "notes",
)
ANNUAL_SEGMENT_LEVEL_HEADERS = (
    "scenario_name",
    "geography_code",
    "module",
    "segment_code",
    "year_index",
    "calendar_year",
    "patients_treated_annual",
    "monthlyization_profile_id",
    "notes",
)
GEOGRAPHY_MASTER_HEADERS = (
    "geography_code",
    "market_group",
    "currency_code",
    "launch_sequence_rank",
    "active_flag",
)
AML_MIX_HEADERS = (
    "scenario_name",
    "geography_code",
    "year_index",
    "month_index",
    "1L_fit_share",
    "1L_unfit_share",
    "RR_share",
    "sum_check",
    "status",
)
MDS_MIX_HEADERS = (
    "scenario_name",
    "geography_code",
    "year_index",
    "month_index",
    "HR_MDS_share",
    "LR_MDS_share",
    "sum_check",
    "status",
)
PROFILE_HEADERS = (
    "profile_id",
    "module",
    "geography_code",
    "segment_code",
    "profile_type",
    "month_1_weight",
    "month_2_weight",
    "month_3_weight",
    "month_4_weight",
    "month_5_weight",
    "month_6_weight",
    "month_7_weight",
    "month_8_weight",
    "month_9_weight",
    "month_10_weight",
    "month_11_weight",
    "month_12_weight",
    "sum_check",
    "status",
    "notes",
)
CML_PREVALENT_ASSUMPTION_HEADERS = (
    "scenario_name",
    "geography_code",
    "year_index",
    "calendar_year",
    "addressable_prevalent_pool_annual",
    "fallback_patients_treated_annual",
    "monthlyization_profile_id",
    "launch_month_index",
    "duration_months",
    "exhaustion_rule",
    "notes",
)
MONTHLYIZED_OUTPUT_HEADERS = (
    "scenario_name",
    "geography_code",
    "module",
    "segment_code",
    "month_index",
    "calendar_month",
    "patients_treated_monthly",
    "source_frequency",
    "source_grain",
    "source_sheet",
    "profile_id_used",
    "notes",
)
INPUT_SHEET_ROWS = {
    "scenario_name": "B2",
    "forecast_grain": "B3",
    "forecast_frequency": "B4",
    "us_aml_mds_initial_approval_date": "B5",
    "real_geography_list_confirmed": "B6",
}
ALLOWED_PROFILE_TYPES = {"FLAT_12", "LAUNCH_RAMP", "STEADY_STATE", "CML_PREVALENT_BOLUS"}
ALLOWED_EXHAUSTION_RULES = {"track_vs_pool", "placeholder_metadata_only", "validate_only"}


@dataclass(frozen=True)
class WorkbookSubmissionContext:
    scenario_name: str
    forecast_grain: str
    forecast_frequency: str
    us_aml_mds_initial_approval_date: date
    real_geography_list_confirmed: bool


@dataclass(frozen=True)
class WorkbookImportResult:
    workbook_path: Path
    output_dir: Path
    context: WorkbookSubmissionContext
    file_paths: dict[str, Path]
    row_counts: dict[str, int]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class ForecastInputRow:
    geography_code: str
    module: str
    month_index: int
    patients_treated: float
    source_frequency: str
    source_grain: str
    source_sheet: str
    profile_id_used: str
    notes: str
    segment_code: str | None = None


@dataclass(frozen=True)
class AnnualModuleForecastInput:
    geography_code: str
    module: str
    year_index: int
    patients_treated_annual: float
    monthlyization_profile_id: str
    notes: str


@dataclass(frozen=True)
class AnnualSegmentForecastInput:
    geography_code: str
    module: str
    segment_code: str
    year_index: int
    patients_treated_annual: float
    monthlyization_profile_id: str
    notes: str


@dataclass(frozen=True)
class AnnualMonthlyizationProfile:
    profile_id: str
    module: str
    geography_code: str
    segment_code: str
    profile_type: str
    weights: tuple[float, ...]
    notes: str


@dataclass(frozen=True)
class CMLPrevalentAssumption:
    geography_code: str
    year_index: int
    addressable_prevalent_pool_annual: float
    fallback_patients_treated_annual: float | None
    monthlyization_profile_id: str
    launch_month_index: int
    duration_months: int
    exhaustion_rule: str
    notes: str


class WorkbookReader:
    """Minimal XLSX reader for workbook imports."""

    def __init__(self, workbook_path: Path) -> None:
        self.workbook_path = workbook_path.resolve()
        self._shared_strings: list[str] | None = None
        self._sheet_map: dict[str, str] | None = None

    def read_sheet_values(self, sheet_name: str) -> dict[str, str]:
        root = self._read_sheet_root(sheet_name)
        values: dict[str, str] = {}
        for cell in root.findall(".//a:sheetData/a:row/a:c", XML_NS):
            ref = cell.attrib["r"]
            values[ref] = _extract_cell_text(cell, self._shared_strings_list())
        return values

    def read_table(self, sheet_name: str, expected_headers: tuple[str, ...]) -> list[dict[str, str]]:
        root = self._read_sheet_root(sheet_name)
        rows = root.findall(".//a:sheetData/a:row", XML_NS)
        if not rows:
            raise ValueError(f"Worksheet {sheet_name!r} has no rows.")

        header_row = _row_to_cell_map(rows[0], self._shared_strings_list())
        headers = [header_row.get(index, "").strip() for index in range(1, len(expected_headers) + 1)]
        if tuple(headers) != expected_headers:
            raise ValueError(
                f"Worksheet {sheet_name!r} headers do not match the expected contract. "
                f"Expected {expected_headers}, received {tuple(headers)}."
            )

        records: list[dict[str, str]] = []
        for row in rows[1:]:
            cell_map = _row_to_cell_map(row, self._shared_strings_list())
            record = {
                header: cell_map.get(index, "").strip()
                for index, header in enumerate(expected_headers, start=1)
            }
            records.append(record)
        return records

    def _shared_strings_list(self) -> list[str]:
        if self._shared_strings is not None:
            return self._shared_strings
        with zipfile.ZipFile(self.workbook_path) as zf:
            try:
                root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            except KeyError:
                self._shared_strings = []
                return self._shared_strings

        self._shared_strings = [
            "".join(text.text or "" for text in node.iterfind(".//a:t", XML_NS))
            for node in root.findall("a:si", XML_NS)
        ]
        return self._shared_strings

    def _sheet_lookup(self) -> dict[str, str]:
        if self._sheet_map is not None:
            return self._sheet_map

        with zipfile.ZipFile(self.workbook_path) as zf:
            workbook_root = ET.fromstring(zf.read("xl/workbook.xml"))
            rel_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))

        rel_lookup = {
            relationship.attrib["Id"]: relationship.attrib["Target"]
            for relationship in rel_root.findall(f"{{{PKG_REL_NS}}}Relationship")
        }
        self._sheet_map = {}
        for sheet in workbook_root.findall("a:sheets/a:sheet", XML_NS):
            rel_id = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
            self._sheet_map[sheet.attrib["name"]] = f"xl/{rel_lookup[rel_id]}"
        return self._sheet_map

    def _read_sheet_root(self, sheet_name: str) -> ET.Element:
        sheet_map = self._sheet_lookup()
        if sheet_name not in sheet_map:
            raise ValueError(f"Workbook is missing required worksheet {sheet_name!r}.")
        with zipfile.ZipFile(self.workbook_path) as zf:
            return ET.fromstring(zf.read(sheet_map[sheet_name]))


def import_commercial_forecast_workbook(
    workbook_path: Path,
    output_dir: Path | None = None,
    scenario_name_override: str | None = None,
) -> WorkbookImportResult:
    reader = WorkbookReader(workbook_path)
    context, allowed_scenario_names = _load_submission_context(
        reader,
        scenario_name_override=scenario_name_override,
    )
    calendar = build_monthly_calendar(context.us_aml_mds_initial_approval_date, PHASE1_HORIZON_MONTHS)

    geography_rows = _normalize_geography_master(reader.read_table("Geography_Master", GEOGRAPHY_MASTER_HEADERS))
    geography_codes = {row["geography_code"] for row in geography_rows}
    profiles = _normalize_monthlyization_profiles(
        reader.read_table("Annual_to_Monthly_Profiles", PROFILE_HEADERS),
        geography_codes=geography_codes,
    )

    monthly_module_rows: list[ForecastInputRow] = []
    monthly_segment_rows: list[ForecastInputRow] = []
    annual_module_rows: list[ForecastInputRow] = []
    annual_segment_rows: list[ForecastInputRow] = []

    if context.forecast_frequency == FORECAST_FREQUENCY_MONTHLY:
        monthly_module_rows = _normalize_module_level_forecast(
            reader.read_table("ModuleLevel_Forecast", MODULE_LEVEL_HEADERS),
            allowed_scenario_names=allowed_scenario_names,
            geography_codes=geography_codes,
        )
        monthly_segment_rows = _normalize_segment_level_forecast(
            reader.read_table("SegmentLevel_Forecast", SEGMENT_LEVEL_HEADERS),
            allowed_scenario_names=allowed_scenario_names,
            geography_codes=geography_codes,
        )
    else:
        annual_module_rows = _monthlyize_annual_module_level_forecast(
            _normalize_annual_module_level_forecast(
                reader.read_table("Annual_ModuleLevel_Forecast", ANNUAL_MODULE_LEVEL_HEADERS),
                allowed_scenario_names=allowed_scenario_names,
                geography_codes=geography_codes,
            ),
            profiles=profiles,
            context=context,
        )
        annual_segment_rows = _monthlyize_annual_segment_level_forecast(
            _normalize_annual_segment_level_forecast(
                reader.read_table("Annual_SegmentLevel_Forecast", ANNUAL_SEGMENT_LEVEL_HEADERS),
                allowed_scenario_names=allowed_scenario_names,
                geography_codes=geography_codes,
            ),
            profiles=profiles,
            context=context,
        )

    module_rows = list(
        monthly_module_rows if context.forecast_frequency == FORECAST_FREQUENCY_MONTHLY else annual_module_rows
    )
    segment_rows = list(
        monthly_segment_rows if context.forecast_frequency == FORECAST_FREQUENCY_MONTHLY else annual_segment_rows
    )

    aml_mix_rows = _normalize_aml_mix(
        reader.read_table("AML_Mix", AML_MIX_HEADERS),
        allowed_scenario_names=allowed_scenario_names,
        geography_codes=geography_codes,
        approval_date=context.us_aml_mds_initial_approval_date,
    )
    mds_mix_rows = _normalize_mds_mix(
        reader.read_table("MDS_Mix", MDS_MIX_HEADERS),
        allowed_scenario_names=allowed_scenario_names,
        geography_codes=geography_codes,
        approval_date=context.us_aml_mds_initial_approval_date,
    )
    cml_assumptions = _normalize_cml_prevalent_assumptions(
        reader.read_table("CML_Prevalent_Assumptions", CML_PREVALENT_ASSUMPTION_HEADERS),
        allowed_scenario_names=allowed_scenario_names,
        geography_codes=geography_codes,
        profiles=profiles,
    )

    if context.forecast_grain == FORECAST_GRAIN_MODULE_LEVEL and not module_rows:
        raise ValueError(
            f"forecast_grain is {FORECAST_GRAIN_MODULE_LEVEL}, but the active {context.forecast_frequency} module-level source contains no data rows."
        )
    if context.forecast_grain == FORECAST_GRAIN_SEGMENT_LEVEL and not segment_rows:
        raise ValueError(
            f"forecast_grain is {FORECAST_GRAIN_SEGMENT_LEVEL}, but the active {context.forecast_frequency} segment-level source contains no data rows."
        )
    if context.forecast_grain == FORECAST_GRAIN_MODULE_LEVEL:
        if any(row.module == "AML" for row in module_rows) and not aml_mix_rows:
            raise ValueError("AML module-level demand requires AML_Mix rows, but none were provided.")
        if any(row.module == "MDS" for row in module_rows) and not mds_mix_rows:
            raise ValueError("MDS module-level demand requires MDS_Mix rows, but none were provided.")

    cml_pool_rows, fallback_module_rows, fallback_segment_rows, cml_warnings = _build_cml_prevalent_outputs(
        assumptions=cml_assumptions,
        context=context,
        profiles=profiles,
    )
    explicit_cml_prevalent_present = _has_explicit_cml_prevalent_rows(
        forecast_grain=context.forecast_grain,
        module_rows=module_rows,
        segment_rows=segment_rows,
    )

    cml_prevalent_primary_source = "none"
    if explicit_cml_prevalent_present:
        cml_prevalent_primary_source = "explicit_forecast"
    else:
        fallback_rows = (
            fallback_module_rows if context.forecast_grain == FORECAST_GRAIN_MODULE_LEVEL else fallback_segment_rows
        )
        if fallback_rows:
            cml_prevalent_primary_source = "assumption_fallback"
            if context.forecast_grain == FORECAST_GRAIN_MODULE_LEVEL:
                module_rows.extend(fallback_module_rows)
            else:
                segment_rows.extend(fallback_segment_rows)

    module_rows = _sort_forecast_rows(module_rows)
    segment_rows = _sort_forecast_rows(segment_rows)

    if context.forecast_grain == FORECAST_GRAIN_MODULE_LEVEL and any(row.module == "AML" for row in module_rows):
        _validate_required_mix_rows(
            module_rows=module_rows,
            mix_rows=aml_mix_rows,
            module="AML",
            approval_date=context.us_aml_mds_initial_approval_date,
        )
    if context.forecast_grain == FORECAST_GRAIN_MODULE_LEVEL and any(row.module == "MDS" for row in module_rows):
        _validate_required_mix_rows(
            module_rows=module_rows,
            mix_rows=mds_mix_rows,
            module="MDS",
            approval_date=context.us_aml_mds_initial_approval_date,
        )

    monthlyized_output_rows = _build_monthlyized_output_rows(
        context=context,
        calendar=calendar,
        module_rows=module_rows,
        segment_rows=segment_rows,
        aml_mix_rows=aml_mix_rows,
        mds_mix_rows=mds_mix_rows,
    )
    warnings = list(
        _build_warnings(
            context=context,
            module_rows=module_rows,
            segment_rows=segment_rows,
            aml_mix_rows=aml_mix_rows,
            mds_mix_rows=mds_mix_rows,
            cml_pool_rows=cml_pool_rows,
            cml_prevalent_primary_source=cml_prevalent_primary_source,
        )
    )
    warnings.extend(cml_warnings)

    resolved_output_dir = _resolve_output_dir(
        workbook_path=workbook_path,
        output_dir=output_dir,
        scenario_name=context.scenario_name,
    )
    file_paths = _write_import_outputs(
        output_dir=resolved_output_dir,
        geography_rows=geography_rows,
        module_rows=module_rows,
        segment_rows=segment_rows,
        aml_mix_rows=aml_mix_rows,
        mds_mix_rows=mds_mix_rows,
        cml_pool_rows=cml_pool_rows,
        monthlyized_output_rows=monthlyized_output_rows,
        warnings=tuple(warnings),
        context=context,
        workbook_path=workbook_path.resolve(),
        cml_prevalent_primary_source=cml_prevalent_primary_source,
    )

    row_counts = {
        "geography_master": len(geography_rows),
        "commercial_forecast_module_level": len(module_rows),
        "commercial_forecast_segment_level": len(segment_rows),
        "aml_segment_mix": len(aml_mix_rows),
        "mds_segment_mix": len(mds_mix_rows),
        "inp_cml_prevalent": len(cml_pool_rows),
        "monthlyized_output": len(monthlyized_output_rows),
    }
    return WorkbookImportResult(
        workbook_path=workbook_path.resolve(),
        output_dir=resolved_output_dir,
        context=context,
        file_paths=file_paths,
        row_counts=row_counts,
        warnings=tuple(warnings),
    )


def _load_submission_context(
    reader: WorkbookReader,
    *,
    scenario_name_override: str | None = None,
) -> tuple[WorkbookSubmissionContext, tuple[str, ...]]:
    values = reader.read_sheet_values("Inputs")
    workbook_scenario_name = values.get(INPUT_SHEET_ROWS["scenario_name"], "").strip()
    if scenario_name_override is not None and scenario_name_override.strip():
        scenario_name = scenario_name_override.strip()
    else:
        scenario_name = _require_nonempty_value(workbook_scenario_name, "scenario_name")
    forecast_grain = _require_nonempty_value(values.get(INPUT_SHEET_ROWS["forecast_grain"], ""), "forecast_grain")
    if forecast_grain not in SUPPORTED_FORECAST_GRAINS:
        raise ValueError(
            f"forecast_grain must be one of {SUPPORTED_FORECAST_GRAINS}, received {forecast_grain!r}."
        )
    forecast_frequency = _require_nonempty_value(
        values.get(INPUT_SHEET_ROWS["forecast_frequency"], ""),
        "forecast_frequency",
    )
    if forecast_frequency not in SUPPORTED_FORECAST_FREQUENCIES:
        raise ValueError(
            f"forecast_frequency must be one of {SUPPORTED_FORECAST_FREQUENCIES}, received {forecast_frequency!r}."
        )
    approval_date = _parse_workbook_date(
        values.get(INPUT_SHEET_ROWS["us_aml_mds_initial_approval_date"], ""),
        "us_aml_mds_initial_approval_date",
    )
    real_geography_list_confirmed = _parse_boolish(
        values.get(INPUT_SHEET_ROWS["real_geography_list_confirmed"], ""),
        "real_geography_list_confirmed",
    )
    context = WorkbookSubmissionContext(
        scenario_name=scenario_name,
        forecast_grain=forecast_grain,
        forecast_frequency=forecast_frequency,
        us_aml_mds_initial_approval_date=approval_date,
        real_geography_list_confirmed=real_geography_list_confirmed,
    )
    allowed_scenario_names = tuple(
        dict.fromkeys(name for name in (scenario_name, workbook_scenario_name) if name)
    )
    return context, allowed_scenario_names


def _normalize_geography_master(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    normalized_rows: list[dict[str, str]] = []
    seen_geographies: set[str] = set()
    for row_number, row in enumerate(rows, start=2):
        if _is_blank_row(row, GEOGRAPHY_MASTER_HEADERS):
            continue
        geography_code = _require_nonempty_row_value(row, "geography_code", "Geography_Master", row_number)
        if geography_code in seen_geographies:
            raise ValueError(f"Geography_Master row {row_number} duplicates geography_code {geography_code!r}.")
        seen_geographies.add(geography_code)
        normalized_rows.append(
            {
                "geography_code": geography_code,
                "market_group": _require_nonempty_row_value(row, "market_group", "Geography_Master", row_number),
                "currency_code": _require_nonempty_row_value(row, "currency_code", "Geography_Master", row_number),
                "launch_sequence_rank": str(
                    _parse_positive_int(
                        row.get("launch_sequence_rank", ""),
                        "launch_sequence_rank",
                        "Geography_Master",
                        row_number,
                    )
                ),
                "active_flag": _format_yes_no(
                    _parse_boolish(row.get("active_flag", ""), "active_flag", "Geography_Master", row_number)
                ),
            }
        )
    return normalized_rows


def _normalize_module_level_forecast(
    rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    geography_codes: set[str],
) -> list[ForecastInputRow]:
    normalized_rows: list[ForecastInputRow] = []
    required_fields = ("geography_code", "module", "month_index", "patients_treated", "notes")
    for row_number, row in enumerate(rows, start=2):
        if _is_blank_row(row, required_fields):
            continue
        _validate_optional_scenario_name(
            row,
            allowed_scenario_names,
            "ModuleLevel_Forecast",
            row_number,
        )
        geography_code = _require_known_geography(row, geography_codes, "ModuleLevel_Forecast", row_number)
        record = ModuleLevelForecastRecord.from_row(
            {
                "geography_code": geography_code,
                "module": _require_nonempty_row_value(row, "module", "ModuleLevel_Forecast", row_number),
                "month_index": _require_nonempty_row_value(
                    row, "month_index", "ModuleLevel_Forecast", row_number
                ),
                "patients_treated": _require_nonempty_row_value(
                    row, "patients_treated", "ModuleLevel_Forecast", row_number
                ),
            }
        )
        normalized_rows.append(
            ForecastInputRow(
                geography_code=record.geography_code,
                module=record.module,
                month_index=record.month_index,
                patients_treated=record.patients_treated,
                source_frequency=FORECAST_FREQUENCY_MONTHLY,
                source_grain=FORECAST_GRAIN_MODULE_LEVEL,
                source_sheet="ModuleLevel_Forecast",
                profile_id_used="",
                notes=row.get("notes", "").strip(),
            )
        )
    return normalized_rows


def _normalize_segment_level_forecast(
    rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    geography_codes: set[str],
) -> list[ForecastInputRow]:
    normalized_rows: list[ForecastInputRow] = []
    required_fields = (
        "geography_code",
        "module",
        "segment_code",
        "month_index",
        "patients_treated",
        "notes",
    )
    for row_number, row in enumerate(rows, start=2):
        if _is_blank_row(row, required_fields):
            continue
        _validate_optional_scenario_name(
            row,
            allowed_scenario_names,
            "SegmentLevel_Forecast",
            row_number,
        )
        geography_code = _require_known_geography(row, geography_codes, "SegmentLevel_Forecast", row_number)
        module = _require_nonempty_row_value(row, "module", "SegmentLevel_Forecast", row_number)
        segment_code = _translate_segment_code(
            module=module,
            segment_code=_require_nonempty_row_value(
                row,
                "segment_code",
                "SegmentLevel_Forecast",
                row_number,
            ),
        )
        record = SegmentLevelForecastRecord.from_row(
            {
                "geography_code": geography_code,
                "module": module,
                "segment_code": segment_code,
                "month_index": _require_nonempty_row_value(
                    row, "month_index", "SegmentLevel_Forecast", row_number
                ),
                "patients_treated": _require_nonempty_row_value(
                    row,
                    "patients_treated",
                    "SegmentLevel_Forecast",
                    row_number,
                ),
            }
        )
        normalized_rows.append(
            ForecastInputRow(
                geography_code=record.geography_code,
                module=record.module,
                segment_code=record.segment_code,
                month_index=record.month_index,
                patients_treated=record.patients_treated,
                source_frequency=FORECAST_FREQUENCY_MONTHLY,
                source_grain=FORECAST_GRAIN_SEGMENT_LEVEL,
                source_sheet="SegmentLevel_Forecast",
                profile_id_used="",
                notes=row.get("notes", "").strip(),
            )
        )
    return normalized_rows


def _normalize_annual_module_level_forecast(
    rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    geography_codes: set[str],
) -> list[AnnualModuleForecastInput]:
    normalized_rows: list[AnnualModuleForecastInput] = []
    required_fields = (
        "geography_code",
        "module",
        "year_index",
        "patients_treated_annual",
        "monthlyization_profile_id",
        "notes",
    )
    for row_number, row in enumerate(rows, start=2):
        if _is_blank_row(row, required_fields):
            continue
        _validate_optional_scenario_name(
            row,
            allowed_scenario_names,
            "Annual_ModuleLevel_Forecast",
            row_number,
        )
        geography_code = _require_known_geography(row, geography_codes, "Annual_ModuleLevel_Forecast", row_number)
        module = _require_nonempty_row_value(row, "module", "Annual_ModuleLevel_Forecast", row_number)
        if module not in PHASE1_MODULES:
            raise ValueError(
                f"Annual_ModuleLevel_Forecast row {row_number} has invalid module {module!r}."
            )
        normalized_rows.append(
            AnnualModuleForecastInput(
                geography_code=geography_code,
                module=module,
                year_index=_parse_positive_int(
                    _require_nonempty_row_value(
                        row,
                        "year_index",
                        "Annual_ModuleLevel_Forecast",
                        row_number,
                    ),
                    "year_index",
                    "Annual_ModuleLevel_Forecast",
                    row_number,
                ),
                patients_treated_annual=_parse_nonnegative_float(
                    _require_nonempty_row_value(
                        row,
                        "patients_treated_annual",
                        "Annual_ModuleLevel_Forecast",
                        row_number,
                    ),
                    "patients_treated_annual",
                    "Annual_ModuleLevel_Forecast",
                    row_number,
                ),
                monthlyization_profile_id=_require_nonempty_row_value(
                    row,
                    "monthlyization_profile_id",
                    "Annual_ModuleLevel_Forecast",
                    row_number,
                ),
                notes=row.get("notes", "").strip(),
            )
        )
    return normalized_rows


def _normalize_annual_segment_level_forecast(
    rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    geography_codes: set[str],
) -> list[AnnualSegmentForecastInput]:
    normalized_rows: list[AnnualSegmentForecastInput] = []
    required_fields = (
        "geography_code",
        "module",
        "segment_code",
        "year_index",
        "patients_treated_annual",
        "monthlyization_profile_id",
        "notes",
    )
    for row_number, row in enumerate(rows, start=2):
        if _is_blank_row(row, required_fields):
            continue
        _validate_optional_scenario_name(
            row,
            allowed_scenario_names,
            "Annual_SegmentLevel_Forecast",
            row_number,
        )
        geography_code = _require_known_geography(row, geography_codes, "Annual_SegmentLevel_Forecast", row_number)
        module = _require_nonempty_row_value(row, "module", "Annual_SegmentLevel_Forecast", row_number)
        segment_code = _translate_segment_code(
            module=module,
            segment_code=_require_nonempty_row_value(
                row,
                "segment_code",
                "Annual_SegmentLevel_Forecast",
                row_number,
            ),
        )
        SegmentLevelForecastRecord.from_row(
            {
                "geography_code": geography_code,
                "module": module,
                "segment_code": segment_code,
                "month_index": "1",
                "patients_treated": _require_nonempty_row_value(
                    row,
                    "patients_treated_annual",
                    "Annual_SegmentLevel_Forecast",
                    row_number,
                ),
            }
        )
        normalized_rows.append(
            AnnualSegmentForecastInput(
                geography_code=geography_code,
                module=module,
                segment_code=segment_code,
                year_index=_parse_positive_int(
                    _require_nonempty_row_value(
                        row,
                        "year_index",
                        "Annual_SegmentLevel_Forecast",
                        row_number,
                    ),
                    "year_index",
                    "Annual_SegmentLevel_Forecast",
                    row_number,
                ),
                patients_treated_annual=_parse_nonnegative_float(
                    _require_nonempty_row_value(
                        row,
                        "patients_treated_annual",
                        "Annual_SegmentLevel_Forecast",
                        row_number,
                    ),
                    "patients_treated_annual",
                    "Annual_SegmentLevel_Forecast",
                    row_number,
                ),
                monthlyization_profile_id=_require_nonempty_row_value(
                    row,
                    "monthlyization_profile_id",
                    "Annual_SegmentLevel_Forecast",
                    row_number,
                ),
                notes=row.get("notes", "").strip(),
            )
        )
    return normalized_rows


def _normalize_aml_mix(
    rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    geography_codes: set[str],
    approval_date: date,
) -> list[dict[str, str]]:
    return _normalize_mix_sheet(
        rows=rows,
        allowed_scenario_names=allowed_scenario_names,
        geography_codes=geography_codes,
        approval_date=approval_date,
        sheet_name="AML_Mix",
        module="AML",
        share_columns=(
            ("1L_fit", "1L_fit_share"),
            ("1L_unfit", "1L_unfit_share"),
            ("RR", "RR_share"),
        ),
    )


def _normalize_mds_mix(
    rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    geography_codes: set[str],
    approval_date: date,
) -> list[dict[str, str]]:
    return _normalize_mix_sheet(
        rows=rows,
        allowed_scenario_names=allowed_scenario_names,
        geography_codes=geography_codes,
        approval_date=approval_date,
        sheet_name="MDS_Mix",
        module="MDS",
        share_columns=(
            ("HR_MDS", "HR_MDS_share"),
            ("LR_MDS", "LR_MDS_share"),
        ),
    )


def _normalize_mix_sheet(
    *,
    rows: list[dict[str, str]],
    allowed_scenario_names: tuple[str, ...],
    geography_codes: set[str],
    approval_date: date,
    sheet_name: str,
    module: str,
    share_columns: tuple[tuple[str, str], ...],
) -> list[dict[str, str]]:
    required_fields = ("geography_code", "year_index", "month_index", *(column for _, column in share_columns))
    annual_rows: dict[tuple[str, int], tuple[tuple[str, float], ...]] = {}
    monthly_rows: dict[tuple[str, int], tuple[tuple[str, float], ...]] = {}
    for row_number, row in enumerate(rows, start=2):
        if _is_blank_row(row, required_fields):
            continue
        _validate_optional_scenario_name(
            row,
            allowed_scenario_names,
            sheet_name,
            row_number,
        )
        geography_code = _require_known_geography(row, geography_codes, sheet_name, row_number)
        year_value = row.get("year_index", "").strip()
        month_value = row.get("month_index", "").strip()
        if not year_value and not month_value:
            raise ValueError(
                f"{sheet_name} row {row_number} must provide either year_index for annual default mix "
                "or month_index for a monthly override."
            )
        if year_value and month_value:
            raise ValueError(
                f"{sheet_name} row {row_number} must provide either year_index or month_index, not both."
            )

        segment_shares = tuple(
            (
                segment_code,
                _parse_nonnegative_float(
                    _require_nonempty_row_value(row, column_name, sheet_name, row_number),
                    column_name,
                    sheet_name,
                    row_number,
                ),
            )
            for segment_code, column_name in share_columns
        )
        share_total = sum(segment_share for _, segment_share in segment_shares)
        if abs(share_total - 1.0) > 1e-6:
            raise ValueError(
                f"{sheet_name} row {row_number} shares must sum to 1.0, received {share_total}."
            )

        if month_value:
            month_index = _parse_positive_int(month_value, "month_index", sheet_name, row_number)
            if month_index > PHASE1_HORIZON_MONTHS:
                raise ValueError(
                    f"{sheet_name} row {row_number} has month_index {month_index}, which exceeds the "
                    f"{PHASE1_HORIZON_MONTHS}-month horizon."
                )
            key = (geography_code, month_index)
            if key in monthly_rows:
                raise ValueError(
                    f"{sheet_name} row {row_number} duplicates monthly mix for geography_code={geography_code!r}, "
                    f"month_index={month_index}."
                )
            monthly_rows[key] = segment_shares
            continue

        year_index = _parse_positive_int(year_value, "year_index", sheet_name, row_number)
        if year_index > 25:
            raise ValueError(f"{sheet_name} row {row_number} has unsupported year_index {year_index}; max is 25.")
        key = (geography_code, year_index)
        if key in annual_rows:
            raise ValueError(
                f"{sheet_name} row {row_number} duplicates annual mix for geography_code={geography_code!r}, "
                f"year_index={year_index}."
            )
        annual_rows[key] = segment_shares

    effective_rows: dict[tuple[str, int], tuple[tuple[str, float], ...]] = {}
    for (geography_code, year_index), segment_shares in annual_rows.items():
        for month_index in _expand_year_index_to_month_indices(year_index, approval_date):
            effective_rows[(geography_code, month_index)] = segment_shares
    for key, segment_shares in monthly_rows.items():
        effective_rows[key] = segment_shares

    normalized_rows: list[dict[str, str]] = []
    ordered_keys = sorted(effective_rows)
    for geography_code, month_index in ordered_keys:
        for segment_code, segment_share in effective_rows[(geography_code, month_index)]:
            normalized_rows.append(
                {
                    "geography_code": geography_code,
                    "month_index": str(month_index),
                    "segment_code": segment_code,
                    "segment_share": _format_numeric(segment_share),
                }
            )
    return normalized_rows


def _normalize_monthlyization_profiles(
    rows: list[dict[str, str]],
    *,
    geography_codes: set[str],
) -> dict[str, AnnualMonthlyizationProfile]:
    normalized_profiles: dict[str, AnnualMonthlyizationProfile] = {}
    for row_number, row in enumerate(rows, start=2):
        if _is_blank_row(row, PROFILE_HEADERS):
            continue
        profile_id = _require_nonempty_row_value(row, "profile_id", "Annual_to_Monthly_Profiles", row_number)
        if profile_id in normalized_profiles:
            raise ValueError(
                f"Annual_to_Monthly_Profiles row {row_number} duplicates profile_id {profile_id!r}."
            )
        module = row.get("module", "").strip()
        if module and module not in PHASE1_MODULES:
            raise ValueError(
                f"Annual_to_Monthly_Profiles row {row_number} has invalid module {module!r}."
            )
        geography_code = row.get("geography_code", "").strip()
        if geography_code and geography_code not in geography_codes:
            raise ValueError(
                f"Annual_to_Monthly_Profiles row {row_number} references geography_code {geography_code!r}, "
                "which is not present in Geography_Master."
            )
        profile_type = _require_nonempty_row_value(
            row,
            "profile_type",
            "Annual_to_Monthly_Profiles",
            row_number,
        )
        if profile_type not in ALLOWED_PROFILE_TYPES:
            raise ValueError(
                f"Annual_to_Monthly_Profiles row {row_number} has invalid profile_type {profile_type!r}."
            )
        weights = tuple(
            _parse_nonnegative_float(
                _require_nonempty_row_value(
                    row,
                    f"month_{month_index}_weight",
                    "Annual_to_Monthly_Profiles",
                    row_number,
                ),
                f"month_{month_index}_weight",
                "Annual_to_Monthly_Profiles",
                row_number,
            )
            for month_index in range(1, 13)
        )
        total_weight = sum(weights)
        if abs(total_weight - 100.0) > 1e-6:
            raise ValueError(
                f"Annual_to_Monthly_Profiles row {row_number} must sum to 100.0, received {total_weight}."
            )
        normalized_profiles[profile_id] = AnnualMonthlyizationProfile(
            profile_id=profile_id,
            module=module,
            geography_code=geography_code,
            segment_code=row.get("segment_code", "").strip(),
            profile_type=profile_type,
            weights=weights,
            notes=row.get("notes", "").strip(),
        )
    return normalized_profiles


def _normalize_cml_prevalent_assumptions(
    rows: list[dict[str, str]],
    *,
    allowed_scenario_names: tuple[str, ...],
    geography_codes: set[str],
    profiles: dict[str, AnnualMonthlyizationProfile],
) -> list[CMLPrevalentAssumption]:
    normalized_rows: list[CMLPrevalentAssumption] = []
    required_fields = (
        "geography_code",
        "year_index",
        "addressable_prevalent_pool_annual",
        "monthlyization_profile_id",
        "launch_month_index",
        "duration_months",
        "exhaustion_rule",
        "notes",
    )
    for row_number, row in enumerate(rows, start=2):
        if _is_blank_row(row, required_fields):
            continue
        _validate_optional_scenario_name(
            row,
            allowed_scenario_names,
            "CML_Prevalent_Assumptions",
            row_number,
        )
        geography_code = _require_known_geography(row, geography_codes, "CML_Prevalent_Assumptions", row_number)
        profile_id = _require_nonempty_row_value(
            row,
            "monthlyization_profile_id",
            "CML_Prevalent_Assumptions",
            row_number,
        )
        if profile_id not in profiles:
            raise ValueError(
                f"CML_Prevalent_Assumptions row {row_number} references missing profile_id {profile_id!r}."
            )
        exhaustion_rule = _require_nonempty_row_value(
            row,
            "exhaustion_rule",
            "CML_Prevalent_Assumptions",
            row_number,
        )
        if exhaustion_rule not in ALLOWED_EXHAUSTION_RULES:
            raise ValueError(
                f"CML_Prevalent_Assumptions row {row_number} has invalid exhaustion_rule {exhaustion_rule!r}."
            )
        fallback_value = row.get("fallback_patients_treated_annual", "").strip()
        normalized_rows.append(
            CMLPrevalentAssumption(
                geography_code=geography_code,
                year_index=_parse_positive_int(
                    _require_nonempty_row_value(
                        row,
                        "year_index",
                        "CML_Prevalent_Assumptions",
                        row_number,
                    ),
                    "year_index",
                    "CML_Prevalent_Assumptions",
                    row_number,
                ),
                addressable_prevalent_pool_annual=_parse_nonnegative_float(
                    _require_nonempty_row_value(
                        row,
                        "addressable_prevalent_pool_annual",
                        "CML_Prevalent_Assumptions",
                        row_number,
                    ),
                    "addressable_prevalent_pool_annual",
                    "CML_Prevalent_Assumptions",
                    row_number,
                ),
                fallback_patients_treated_annual=(
                    _parse_nonnegative_float(
                        fallback_value,
                        "fallback_patients_treated_annual",
                        "CML_Prevalent_Assumptions",
                        row_number,
                    )
                    if fallback_value
                    else None
                ),
                monthlyization_profile_id=profile_id,
                launch_month_index=_parse_positive_int(
                    _require_nonempty_row_value(
                        row,
                        "launch_month_index",
                        "CML_Prevalent_Assumptions",
                        row_number,
                    ),
                    "launch_month_index",
                    "CML_Prevalent_Assumptions",
                    row_number,
                ),
                duration_months=_parse_positive_int(
                    _require_nonempty_row_value(
                        row,
                        "duration_months",
                        "CML_Prevalent_Assumptions",
                        row_number,
                    ),
                    "duration_months",
                    "CML_Prevalent_Assumptions",
                    row_number,
                ),
                exhaustion_rule=exhaustion_rule,
                notes=row.get("notes", "").strip(),
            )
        )
    return normalized_rows


def _monthlyize_annual_module_level_forecast(
    annual_rows: list[AnnualModuleForecastInput],
    *,
    profiles: dict[str, AnnualMonthlyizationProfile],
    context: WorkbookSubmissionContext,
) -> list[ForecastInputRow]:
    normalized_rows: list[ForecastInputRow] = []
    for row in annual_rows:
        profile = _require_profile(profiles, row.monthlyization_profile_id, "Annual_ModuleLevel_Forecast")
        for month_index, patients_treated in _distribute_annual_value(
            annual_value=row.patients_treated_annual,
            year_index=row.year_index,
            profile=profile,
            approval_date=context.us_aml_mds_initial_approval_date,
            row_label=f"Annual_ModuleLevel_Forecast {row.geography_code}/{row.module}/year_index {row.year_index}",
        ):
            normalized_rows.append(
                ForecastInputRow(
                    geography_code=row.geography_code,
                    module=row.module,
                    month_index=month_index,
                    patients_treated=patients_treated,
                    source_frequency=FORECAST_FREQUENCY_ANNUAL,
                    source_grain=FORECAST_GRAIN_MODULE_LEVEL,
                    source_sheet="Annual_ModuleLevel_Forecast",
                    profile_id_used=row.monthlyization_profile_id,
                    notes=row.notes,
                )
            )
    return normalized_rows


def _monthlyize_annual_segment_level_forecast(
    annual_rows: list[AnnualSegmentForecastInput],
    *,
    profiles: dict[str, AnnualMonthlyizationProfile],
    context: WorkbookSubmissionContext,
) -> list[ForecastInputRow]:
    normalized_rows: list[ForecastInputRow] = []
    for row in annual_rows:
        profile = _require_profile(profiles, row.monthlyization_profile_id, "Annual_SegmentLevel_Forecast")
        for month_index, patients_treated in _distribute_annual_value(
            annual_value=row.patients_treated_annual,
            year_index=row.year_index,
            profile=profile,
            approval_date=context.us_aml_mds_initial_approval_date,
            row_label=f"Annual_SegmentLevel_Forecast {row.geography_code}/{row.module}/{row.segment_code}/year_index {row.year_index}",
        ):
            normalized_rows.append(
                ForecastInputRow(
                    geography_code=row.geography_code,
                    module=row.module,
                    segment_code=row.segment_code,
                    month_index=month_index,
                    patients_treated=patients_treated,
                    source_frequency=FORECAST_FREQUENCY_ANNUAL,
                    source_grain=FORECAST_GRAIN_SEGMENT_LEVEL,
                    source_sheet="Annual_SegmentLevel_Forecast",
                    profile_id_used=row.monthlyization_profile_id,
                    notes=row.notes,
                )
            )
    return normalized_rows


def _build_cml_prevalent_outputs(
    *,
    assumptions: list[CMLPrevalentAssumption],
    context: WorkbookSubmissionContext,
    profiles: dict[str, AnnualMonthlyizationProfile],
) -> tuple[list[dict[str, str]], list[ForecastInputRow], list[ForecastInputRow], tuple[str, ...]]:
    pool_by_key: dict[tuple[str, int], float] = defaultdict(float)
    fallback_module_rows: list[ForecastInputRow] = []
    fallback_segment_rows: list[ForecastInputRow] = []
    warnings: list[str] = []

    for assumption in assumptions:
        profile = _require_profile(profiles, assumption.monthlyization_profile_id, "CML_Prevalent_Assumptions")
        active_month_end = min(
            PHASE1_HORIZON_MONTHS,
            assumption.launch_month_index + assumption.duration_months - 1,
        )
        pool_distribution = _distribute_annual_value(
            annual_value=assumption.addressable_prevalent_pool_annual,
            year_index=assumption.year_index,
            profile=profile,
            approval_date=context.us_aml_mds_initial_approval_date,
            active_month_start=assumption.launch_month_index,
            active_month_end=active_month_end,
            row_label=(
                f"CML_Prevalent_Assumptions pool {assumption.geography_code}/year_index {assumption.year_index}"
            ),
        )
        for month_index, pool_value in pool_distribution:
            pool_by_key[(assumption.geography_code, month_index)] += pool_value

        if assumption.fallback_patients_treated_annual is None:
            continue
        fallback_distribution = _distribute_annual_value(
            annual_value=assumption.fallback_patients_treated_annual,
            year_index=assumption.year_index,
            profile=profile,
            approval_date=context.us_aml_mds_initial_approval_date,
            active_month_start=assumption.launch_month_index,
            active_month_end=active_month_end,
            row_label=(
                f"CML_Prevalent_Assumptions fallback {assumption.geography_code}/year_index {assumption.year_index}"
            ),
        )
        for month_index, patients_treated in fallback_distribution:
            base_kwargs = {
                "geography_code": assumption.geography_code,
                "module": "CML_Prevalent",
                "month_index": month_index,
                "patients_treated": patients_treated,
                "source_frequency": FORECAST_FREQUENCY_ANNUAL,
                "source_grain": context.forecast_grain,
                "source_sheet": "CML_Prevalent_Assumptions",
                "profile_id_used": assumption.monthlyization_profile_id,
                "notes": _combine_notes(
                    assumption.notes,
                    f"fallback_generated_from_assumptions; exhaustion_rule={assumption.exhaustion_rule}",
                ),
            }
            fallback_module_rows.append(ForecastInputRow(**base_kwargs))
            fallback_segment_rows.append(ForecastInputRow(segment_code="CML_Prevalent", **base_kwargs))

        if assumption.exhaustion_rule != "track_vs_pool":
            warnings.append(
                f"CML_Prevalent_Assumptions for {assumption.geography_code} year_index {assumption.year_index} uses exhaustion_rule "
                f"{assumption.exhaustion_rule!r}; this value is captured for audit metadata but does not add new depletion logic in Phase 1."
            )

    pool_rows: list[dict[str, str]] = []
    for (geography_code, month_index), pool_value in sorted(pool_by_key.items()):
        record = CMLPrevalentPoolRecord.from_row(
            {
                "geography_code": geography_code,
                "month_index": str(month_index),
                "addressable_prevalent_pool": _format_numeric(pool_value),
            }
        )
        pool_rows.append(
            {
                "geography_code": record.geography_code,
                "month_index": str(record.month_index),
                "addressable_prevalent_pool": _format_numeric(record.addressable_prevalent_pool),
            }
        )
    return pool_rows, fallback_module_rows, fallback_segment_rows, tuple(warnings)


def _distribute_annual_value(
    *,
    annual_value: float,
    year_index: int,
    profile: AnnualMonthlyizationProfile,
    approval_date: date,
    row_label: str,
    active_month_start: int = 1,
    active_month_end: int = PHASE1_HORIZON_MONTHS,
) -> list[tuple[int, float]]:
    if year_index > 25:
        raise ValueError(f"{row_label} exceeds the supported 25-year workbook range.")
    approval_month_offset = approval_date.month - 1
    active_weights: list[tuple[int, float]] = []
    for month_number, raw_weight in enumerate(profile.weights, start=1):
        month_index = ((year_index - 1) * 12) + month_number - approval_month_offset
        if month_index < 1 or month_index > PHASE1_HORIZON_MONTHS:
            continue
        if month_index < active_month_start or month_index > active_month_end:
            continue
        active_weights.append((month_index, raw_weight))

    total_weight = sum(weight for _, weight in active_weights)
    if annual_value > 0 and total_weight <= 0:
        raise ValueError(
            f"{row_label} has positive annual value {annual_value}, but no in-scope months remain after launch filtering."
        )
    if total_weight <= 0:
        return []
    return [
        (month_index, annual_value * (weight / total_weight))
        for month_index, weight in active_weights
    ]


def _expand_year_index_to_month_indices(year_index: int, approval_date: date) -> tuple[int, ...]:
    approval_month_offset = approval_date.month - 1
    month_indices: list[int] = []
    for month_number in range(1, 13):
        month_index = ((year_index - 1) * 12) + month_number - approval_month_offset
        if 1 <= month_index <= PHASE1_HORIZON_MONTHS:
            month_indices.append(month_index)
    return tuple(month_indices)


def _year_index_for_month_index(month_index: int, approval_date: date) -> int:
    approval_month_offset = approval_date.month - 1
    return ((month_index + approval_month_offset - 1) // 12) + 1


def _build_monthlyized_output_rows(
    *,
    context: WorkbookSubmissionContext,
    calendar: MonthlyCalendar,
    module_rows: list[ForecastInputRow],
    segment_rows: list[ForecastInputRow],
    aml_mix_rows: list[dict[str, str]],
    mds_mix_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    outputs: list[dict[str, str]] = []
    if context.forecast_grain == FORECAST_GRAIN_SEGMENT_LEVEL:
        for row in segment_rows:
            outputs.append(_build_monthlyized_output_record(calendar=calendar, row=row, segment_code=row.segment_code or ""))
        return outputs

    aml_mix_lookup = _build_mix_lookup(aml_mix_rows, AML_SEGMENTS)
    mds_mix_lookup = _build_mix_lookup(mds_mix_rows, MDS_SEGMENTS)
    for row in module_rows:
        if row.module == "AML":
            mix_rows = aml_mix_lookup.get((row.geography_code, row.month_index))
            if not mix_rows:
                raise ValueError(
                    "Missing AML_Mix coverage for "
                    f"geography_code={row.geography_code!r}, month_index={row.month_index}. "
                    "Provide either a monthly override row or an annual year_index row."
                )
            for segment_code, segment_share in mix_rows:
                outputs.append(
                    _build_monthlyized_output_record(
                        calendar=calendar,
                        row=row,
                        segment_code=segment_code,
                        patients_treated=row.patients_treated * segment_share,
                        notes=_combine_notes(row.notes, "allocated_using_AML_Mix"),
                    )
                )
            continue
        if row.module == "MDS":
            mix_rows = mds_mix_lookup.get((row.geography_code, row.month_index))
            if not mix_rows:
                raise ValueError(
                    "Missing MDS_Mix coverage for "
                    f"geography_code={row.geography_code!r}, month_index={row.month_index}. "
                    "Provide either a monthly override row or an annual year_index row."
                )
            for segment_code, segment_share in mix_rows:
                outputs.append(
                    _build_monthlyized_output_record(
                        calendar=calendar,
                        row=row,
                        segment_code=segment_code,
                        patients_treated=row.patients_treated * segment_share,
                        notes=_combine_notes(row.notes, "allocated_using_MDS_Mix"),
                    )
                )
            continue
        outputs.append(
            _build_monthlyized_output_record(
                calendar=calendar,
                row=row,
                segment_code=row.module,
            )
        )
    return outputs


def _build_monthlyized_output_record(
    *,
    calendar: MonthlyCalendar,
    row: ForecastInputRow,
    segment_code: str,
    patients_treated: float | None = None,
    notes: str | None = None,
) -> dict[str, str]:
    month = calendar.get_month(row.month_index)
    return {
        "scenario_name": "",
        "geography_code": row.geography_code,
        "module": row.module,
        "segment_code": segment_code,
        "month_index": str(row.month_index),
        "calendar_month": month.month_start.isoformat(),
        "patients_treated_monthly": _format_numeric(
            row.patients_treated if patients_treated is None else patients_treated
        ),
        "source_frequency": row.source_frequency,
        "source_grain": row.source_grain,
        "source_sheet": row.source_sheet,
        "profile_id_used": row.profile_id_used,
        "notes": notes if notes is not None else row.notes,
    }


def _build_mix_lookup(
    mix_rows: list[dict[str, str]],
    expected_segments: tuple[str, ...],
) -> dict[tuple[str, int], tuple[tuple[str, float], ...]]:
    grouped: dict[tuple[str, int], list[tuple[str, float]]] = defaultdict(list)
    for row in mix_rows:
        grouped[(row["geography_code"], int(row["month_index"]))].append(
            (row["segment_code"], float(row["segment_share"]))
        )
    lookup: dict[tuple[str, int], tuple[tuple[str, float], ...]] = {}
    expected_set = set(expected_segments)
    order = {segment_code: index for index, segment_code in enumerate(expected_segments)}
    for key, values in grouped.items():
        if {segment_code for segment_code, _ in values} != expected_set:
            continue
        if abs(sum(segment_share for _, segment_share in values) - 1.0) > 1e-9:
            continue
        lookup[key] = tuple(sorted(values, key=lambda value: order[value[0]]))
    return lookup


def _validate_required_mix_rows(
    *,
    module_rows: list[ForecastInputRow],
    mix_rows: list[dict[str, str]],
    module: str,
    approval_date: date,
) -> None:
    required_keys = {
        (row.geography_code, row.month_index)
        for row in module_rows
        if row.module == module
    }
    available_keys = {
        (row["geography_code"], int(row["month_index"]))
        for row in mix_rows
    }
    missing_keys = sorted(required_keys - available_keys)
    if missing_keys:
        geography_code, month_index = missing_keys[0]
        year_index = _year_index_for_month_index(month_index, approval_date)
        raise ValueError(
            f"{module}_Mix is missing required annual or monthly coverage for geography_code={geography_code!r}, "
            f"month_index={month_index}, year_index={year_index}."
        )


def _resolve_output_dir(
    *,
    workbook_path: Path,
    output_dir: Path | None,
    scenario_name: str,
) -> Path:
    if output_dir is not None:
        return output_dir.resolve()
    resolved_workbook = workbook_path.resolve()
    repo_root = resolved_workbook.parents[1] if len(resolved_workbook.parents) >= 2 else resolved_workbook.parent
    slug = _slugify(scenario_name)
    return (repo_root / "data" / "curated" / slug).resolve()


def _write_import_outputs(
    *,
    output_dir: Path,
    geography_rows: list[dict[str, str]],
    module_rows: list[ForecastInputRow],
    segment_rows: list[ForecastInputRow],
    aml_mix_rows: list[dict[str, str]],
    mds_mix_rows: list[dict[str, str]],
    cml_pool_rows: list[dict[str, str]],
    monthlyized_output_rows: list[dict[str, str]],
    warnings: tuple[str, ...],
    context: WorkbookSubmissionContext,
    workbook_path: Path,
    cml_prevalent_primary_source: str,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "geography_master": output_dir / "geography_master.csv",
        "commercial_forecast_module_level": output_dir / "commercial_forecast_module_level.csv",
        "commercial_forecast_segment_level": output_dir / "commercial_forecast_segment_level.csv",
        "aml_segment_mix": output_dir / "aml_segment_mix.csv",
        "mds_segment_mix": output_dir / "mds_segment_mix.csv",
        "inp_epi_crosscheck": output_dir / "inp_epi_crosscheck.csv",
        "inp_cml_prevalent": output_dir / "inp_cml_prevalent.csv",
        "monthlyized_output": output_dir / "monthlyized_output.csv",
        "import_summary": output_dir / "workbook_import_summary.json",
    }
    _write_csv(paths["geography_master"], GEOGRAPHY_MASTER_HEADERS, geography_rows)
    _write_csv(
        paths["commercial_forecast_module_level"],
        ("geography_code", "module", "month_index", "patients_treated"),
        [
            {
                "geography_code": row.geography_code,
                "module": row.module,
                "month_index": str(row.month_index),
                "patients_treated": _format_numeric(row.patients_treated),
            }
            for row in module_rows
        ],
    )
    _write_csv(
        paths["commercial_forecast_segment_level"],
        ("geography_code", "module", "segment_code", "month_index", "patients_treated"),
        [
            {
                "geography_code": row.geography_code,
                "module": row.module,
                "segment_code": row.segment_code or "",
                "month_index": str(row.month_index),
                "patients_treated": _format_numeric(row.patients_treated),
            }
            for row in segment_rows
        ],
    )
    _write_csv(
        paths["aml_segment_mix"],
        ("geography_code", "month_index", "segment_code", "segment_share"),
        aml_mix_rows,
    )
    _write_csv(
        paths["mds_segment_mix"],
        ("geography_code", "month_index", "segment_code", "segment_share"),
        mds_mix_rows,
    )
    _write_csv(
        paths["inp_epi_crosscheck"],
        ("geography_code", "module", "month_index", "treatable_patients"),
        [],
    )
    _write_csv(
        paths["inp_cml_prevalent"],
        ("geography_code", "month_index", "addressable_prevalent_pool"),
        cml_pool_rows,
    )
    _write_csv(
        paths["monthlyized_output"],
        MONTHLYIZED_OUTPUT_HEADERS,
        [{**row, "scenario_name": context.scenario_name} for row in monthlyized_output_rows],
    )

    summary_payload = {
        "workbook_path": str(workbook_path),
        "scenario_name": context.scenario_name,
        "forecast_grain": context.forecast_grain,
        "forecast_frequency": context.forecast_frequency,
        "us_aml_mds_initial_approval_date": context.us_aml_mds_initial_approval_date.isoformat(),
        "real_geography_list_confirmed": context.real_geography_list_confirmed,
        "output_dir": str(output_dir),
        "cml_prevalent_primary_source": cml_prevalent_primary_source,
        "row_counts": {
            "geography_master": len(geography_rows),
            "commercial_forecast_module_level": len(module_rows),
            "commercial_forecast_segment_level": len(segment_rows),
            "aml_segment_mix": len(aml_mix_rows),
            "mds_segment_mix": len(mds_mix_rows),
            "inp_cml_prevalent": len(cml_pool_rows),
            "monthlyized_output": len(monthlyized_output_rows),
        },
        "monthlyized_output_rows_by_module": dict(
            sorted(Counter(row["module"] for row in monthlyized_output_rows).items())
        ),
        "monthlyized_output_rows_by_segment": dict(
            sorted(Counter(row["segment_code"] for row in monthlyized_output_rows).items())
        ),
        "warnings": list(warnings),
        "notes": [
            "Annual_to_Monthly_Profiles remain editable starter defaults rather than fixed business assumptions.",
            "CML_Prevalent_Assumptions is optional for explicit CML_Prevalent demand import. If provided, it generates inp_cml_prevalent.csv for validation; if explicit forecast rows are absent, it can also generate fallback CML_Prevalent demand.",
            "The importer writes monthlyized_output.csv as the authoritative normalized monthly workbook export in Phase 1; the workbook tab is reserved as a generated/reference placeholder.",
            "exhaustion_rule is carried as workbook metadata and does not introduce new depletion logic beyond the supplied annual totals, launch_month_index, and duration_months.",
        ],
    }
    paths["import_summary"].write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    return paths


def _write_csv(path: Path, fieldnames: tuple[str, ...], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _build_warnings(
    *,
    context: WorkbookSubmissionContext,
    module_rows: list[ForecastInputRow],
    segment_rows: list[ForecastInputRow],
    aml_mix_rows: list[dict[str, str]],
    mds_mix_rows: list[dict[str, str]],
    cml_pool_rows: list[dict[str, str]],
    cml_prevalent_primary_source: str,
) -> tuple[str, ...]:
    warnings: list[str] = []
    if not context.real_geography_list_confirmed:
        warnings.append(
            "real_geography_list_confirmed is false on the Inputs sheet; review Geography_Master before using this package as a production scenario."
        )
    if context.forecast_grain == FORECAST_GRAIN_MODULE_LEVEL and segment_rows:
        warnings.append(
            "A segment-level normalized CSV was also written for QA/reference, but module_level remains the active demand contract for this scenario."
        )
    if context.forecast_grain == FORECAST_GRAIN_SEGMENT_LEVEL and module_rows:
        warnings.append(
            "A module-level normalized CSV was also written for QA/reference, but segment_level remains the active demand contract for this scenario."
        )
    if context.forecast_grain == FORECAST_GRAIN_SEGMENT_LEVEL and aml_mix_rows:
        warnings.append(
            "AML_Mix rows were imported, but AML mix is validation/reference-only in segment_level mode."
        )
    if context.forecast_grain == FORECAST_GRAIN_SEGMENT_LEVEL and mds_mix_rows:
        warnings.append(
            "MDS_Mix rows were imported, but MDS mix is validation/reference-only in segment_level mode."
        )
    if cml_prevalent_primary_source == "explicit_forecast" and cml_pool_rows:
        warnings.append(
            "Explicit CML_Prevalent forecast rows remained the primary demand input; CML_Prevalent_Assumptions was used only to generate the validation pool."
        )
    elif cml_prevalent_primary_source == "explicit_forecast":
        warnings.append(
            "Explicit CML_Prevalent forecast rows remained the primary demand input. No usable CML_Prevalent_Assumptions rows were provided, so no validation pool was generated and inp_cml_prevalent.csv is header-only."
        )
    elif cml_prevalent_primary_source == "assumption_fallback":
        warnings.append(
            "No explicit CML_Prevalent forecast rows were found in the active source sheet, so fallback monthly demand was generated from CML_Prevalent_Assumptions."
        )
    if not cml_pool_rows and cml_prevalent_primary_source != "explicit_forecast":
        warnings.append(
            "inp_cml_prevalent.csv is header-only because no usable CML_Prevalent_Assumptions rows were provided."
        )
    warnings.append(
        "Populate inp_epi_crosscheck.csv only if you want epi cross-check coverage for this submission."
    )
    return tuple(warnings)


def _sort_forecast_rows(rows: list[ForecastInputRow]) -> list[ForecastInputRow]:
    return sorted(
        rows,
        key=lambda row: (
            row.geography_code,
            row.module,
            row.segment_code or "",
            row.month_index,
            row.source_sheet,
        ),
    )


def _has_explicit_cml_prevalent_rows(
    *,
    forecast_grain: str,
    module_rows: list[ForecastInputRow],
    segment_rows: list[ForecastInputRow],
) -> bool:
    if forecast_grain == FORECAST_GRAIN_MODULE_LEVEL:
        return any(row.module == "CML_Prevalent" for row in module_rows)
    return any(row.module == "CML_Prevalent" for row in segment_rows)


def _require_profile(
    profiles: dict[str, AnnualMonthlyizationProfile],
    profile_id: str,
    sheet_name: str,
) -> AnnualMonthlyizationProfile:
    if profile_id not in profiles:
        raise ValueError(f"{sheet_name} references missing profile_id {profile_id!r}.")
    return profiles[profile_id]


def _row_to_cell_map(row: ET.Element, shared_strings: list[str]) -> dict[int, str]:
    values: dict[int, str] = {}
    for cell in row.findall("a:c", XML_NS):
        ref = cell.attrib.get("r", "")
        match = CELL_REF_PATTERN.match(ref)
        if match is None:
            continue
        values[_column_index(match.group("column"))] = _extract_cell_text(cell, shared_strings)
    return values


def _extract_cell_text(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "s":
        raw = cell.findtext("a:v", default="", namespaces=XML_NS)
        return shared_strings[int(raw)] if raw else ""
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.findall(".//a:t", XML_NS))
    if cell_type == "b":
        return "true" if cell.findtext("a:v", default="0", namespaces=XML_NS) == "1" else "false"
    return cell.findtext("a:v", default="", namespaces=XML_NS).strip()


def _column_index(column_ref: str) -> int:
    value = 0
    for character in column_ref:
        value = (value * 26) + (ord(character) - 64)
    return value


def _is_blank_row(row: dict[str, str], keys: tuple[str, ...]) -> bool:
    return all(not row.get(key, "").strip() for key in keys)


def _validate_optional_scenario_name(
    row: dict[str, str],
    allowed_scenario_names: tuple[str, ...],
    sheet_name: str,
    row_number: int,
) -> None:
    provided = row.get("scenario_name", "").strip()
    if not provided or provided in allowed_scenario_names:
        return
    if len(allowed_scenario_names) == 1:
        expected_message = repr(allowed_scenario_names[0])
    else:
        expected_message = f"one of {allowed_scenario_names!r}"
    raise ValueError(
        f"{sheet_name} row {row_number} has scenario_name {provided!r}, expected {expected_message}."
    )


def _require_nonempty_value(value: str, field_name: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} is required on the Inputs sheet.")
    return stripped


def _require_nonempty_row_value(
    row: dict[str, str],
    column_name: str,
    sheet_name: str,
    row_number: int,
) -> str:
    value = row.get(column_name, "").strip()
    if not value:
        raise ValueError(f"{sheet_name} row {row_number} is missing required value for {column_name!r}.")
    return value


def _require_known_geography(
    row: dict[str, str],
    geography_codes: set[str],
    sheet_name: str,
    row_number: int,
) -> str:
    geography_code = _require_nonempty_row_value(row, "geography_code", sheet_name, row_number)
    if geography_code not in geography_codes:
        raise ValueError(
            f"{sheet_name} row {row_number} references geography_code {geography_code!r}, "
            "which is not present in Geography_Master."
        )
    return geography_code


def _parse_positive_int(
    value: str,
    field_name: str,
    sheet_name: str,
    row_number: int,
) -> int:
    try:
        parsed = int(float(value))
    except ValueError as exc:
        raise ValueError(
            f"{sheet_name} row {row_number} has non-integer {field_name!r}: {value!r}."
        ) from exc
    if parsed <= 0:
        raise ValueError(
            f"{sheet_name} row {row_number} requires positive {field_name!r}, received {parsed}."
        )
    return parsed


def _parse_nonnegative_float(
    value: str,
    field_name: str,
    sheet_name: str,
    row_number: int,
) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(
            f"{sheet_name} row {row_number} has non-numeric {field_name!r}: {value!r}."
        ) from exc
    if parsed < 0:
        raise ValueError(
            f"{sheet_name} row {row_number} requires non-negative {field_name!r}, received {parsed}."
        )
    return parsed


def _parse_boolish(
    value: str,
    field_name: str,
    sheet_name: str = "Inputs",
    row_number: int | None = None,
) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "yes", "1"}:
        return True
    if normalized in {"false", "no", "0"}:
        return False
    location = f"{sheet_name} row {row_number}" if row_number is not None else sheet_name
    raise ValueError(f"{location} has invalid boolean value for {field_name!r}: {value!r}.")


def _format_yes_no(value: bool) -> str:
    return "yes" if value else "no"


def _parse_workbook_date(value: str, field_name: str) -> date:
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} is required on the Inputs sheet.")
    try:
        serial = float(stripped)
    except ValueError:
        for format_string in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(stripped, format_string).date()
            except ValueError:
                continue
        raise ValueError(f"{field_name} must be a valid Excel date or ISO date, received {value!r}.")
    return EXCEL_EPOCH + timedelta(days=int(round(serial)))


def _translate_segment_code(*, module: str, segment_code: str) -> str:
    if module == "CML_Incident" and segment_code == "ALL":
        return "CML_Incident"
    if module == "CML_Prevalent" and segment_code == "ALL":
        return "CML_Prevalent"
    return segment_code


def _combine_notes(existing_notes: str, appended_note: str) -> str:
    cleaned_existing = existing_notes.strip()
    cleaned_appended = appended_note.strip()
    if cleaned_existing and cleaned_appended:
        return f"{cleaned_existing}; {cleaned_appended}"
    return cleaned_existing or cleaned_appended


def _format_numeric(value: float) -> str:
    return format(value, ".15g")


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", value.strip()).strip("_").lower()
    return normalized or "workbook_import"
