"""Raw workbook ingest for a single real Phase 1 scenario."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
import csv
import re
import xml.etree.ElementTree as ET
import zipfile

from ..constants import AML_SEGMENTS, MDS_SEGMENTS

XML_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
YEAR_PATTERN = re.compile(r"^Year (?P<year_index>\d+)$")
RAW_TO_TARGET_MODULE = {
    "AML": "AML",
    "CML-inc": "CML_Incident",
    "CML_prev": "CML_Prevalent",
    "MDS": "MDS",
}
PLACEHOLDER_EQUAL_AML_SHARES = (
    ("1L_fit", Decimal("0.333333333333")),
    ("1L_unfit", Decimal("0.333333333333")),
    ("RR", Decimal("0.333333333334")),
)
PLACEHOLDER_EQUAL_MDS_SHARES = (
    ("HR_MDS", Decimal("0.5")),
    ("LR_MDS", Decimal("0.5")),
)


@dataclass(frozen=True)
class AnnualForecastRecord:
    geography_code: str
    module: str
    year_index: int
    annual_patients_treated: Decimal


@dataclass(frozen=True)
class BasePoolRecord:
    geography_code: str
    module: str
    annual_pool: Decimal


@dataclass(frozen=True)
class RawScenarioData:
    annual_forecast: tuple[AnnualForecastRecord, ...]
    base_pools: tuple[BasePoolRecord, ...]
    extracted_notes: tuple[str, ...]


def _decimal_to_str(value: Decimal) -> str:
    normalized = value.normalize()
    return format(normalized, "f")


def _read_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    values: list[str] = []
    for si in root.findall("a:si", XML_NS):
        pieces = [text.text or "" for text in si.iterfind(".//a:t", XML_NS)]
        values.append("".join(pieces))
    return values


def _read_sheet_cells(workbook_path: Path) -> dict[tuple[int, str], str]:
    with zipfile.ZipFile(workbook_path) as zf:
        shared_strings = _read_shared_strings(zf)
        sheet = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))

    cells: dict[tuple[int, str], str] = {}
    for row in sheet.findall(".//a:sheetData/a:row", XML_NS):
        row_number = int(row.attrib["r"])
        for cell in row.findall("a:c", XML_NS):
            ref = cell.attrib["r"]
            column = "".join(character for character in ref if character.isalpha())
            value_node = cell.find("a:v", XML_NS)
            if value_node is None:
                continue
            raw = value_node.text or ""
            if cell.attrib.get("t") == "s":
                value = shared_strings[int(raw)]
            else:
                value = raw
            cells[(row_number, column)] = value
    return cells


def extract_raw_scenario_data(workbook_path: Path) -> RawScenarioData:
    cells = _read_sheet_cells(workbook_path)
    annual_forecast: list[AnnualForecastRecord] = []
    notes: list[str] = []

    year_header_rows = sorted(
        row_number
        for (row_number, column), value in cells.items()
        if column == "C" and YEAR_PATTERN.match(value)
    )
    for row_number in year_header_rows:
        year_match = YEAR_PATTERN.match(cells[(row_number, "C")])
        if year_match is None:
            continue
        year_index = int(year_match.group("year_index"))
        module_headers = {
            column: RAW_TO_TARGET_MODULE[cells[(row_number + 1, column)]]
            for column in ("C", "D", "E", "F")
            if cells.get((row_number + 1, column)) in RAW_TO_TARGET_MODULE
        }
        for data_row in (row_number + 2, row_number + 3):
            geography_code = cells.get((data_row, "B"), "")
            if geography_code not in {"US", "EU"}:
                continue
            for column, module in module_headers.items():
                raw_value = cells.get((data_row, column), "0") or "0"
                annual_forecast.append(
                    AnnualForecastRecord(
                        geography_code=geography_code,
                        module=module,
                        year_index=year_index,
                        annual_patients_treated=Decimal(raw_value),
                    )
                )

    for row_number in range(1, max(row for row, _ in cells) + 1):
        note = cells.get((row_number, "K"), "") or cells.get((row_number, "J"), "")
        if note and (note.startswith("•") or note.startswith("−")):
            notes.append(note)

    base_pools = (
        BasePoolRecord(
            geography_code="US",
            module="CML_Prevalent",
            annual_pool=Decimal(cells[(3, "M")]),
        ),
        BasePoolRecord(
            geography_code="EU",
            module="CML_Prevalent",
            annual_pool=Decimal(cells[(4, "M")]),
        ),
    )
    return RawScenarioData(
        annual_forecast=tuple(annual_forecast),
        base_pools=base_pools,
        extracted_notes=tuple(notes),
    )


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _build_module_level_forecast_rows(annual_forecast: tuple[AnnualForecastRecord, ...]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in annual_forecast:
        first_month_index = ((record.year_index - 1) * 12) + 1
        monthly_value = record.annual_patients_treated / Decimal("12")
        for month_offset in range(12):
            rows.append(
                {
                    "geography_code": record.geography_code,
                    "module": record.module,
                    "month_index": str(first_month_index + month_offset),
                    "patients_treated": _decimal_to_str(monthly_value),
                    "source_file": "treatable pts 250.xlsx",
                    "source_grain": "annual_by_geography_module",
                    "transformation_note": (
                        "PLACEHOLDER: annual source value split evenly across 12 months for Phase 1."
                    ),
                }
            )
    return rows


def _build_segment_level_forecast_rows() -> list[dict[str, str]]:
    return []


def _build_mix_rows(
    module_level_rows: list[dict[str, str]],
    module: str,
    placeholder_shares: tuple[tuple[str, Decimal], ...],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    unique_pairs = sorted(
        {
            (row["geography_code"], row["month_index"])
            for row in module_level_rows
            if row["module"] == module
        }
    )
    for geography_code, month_index in unique_pairs:
        for segment_code, segment_share in placeholder_shares:
            rows.append(
                {
                    "geography_code": geography_code,
                    "month_index": month_index,
                    "segment_code": segment_code,
                    "segment_share": _decimal_to_str(segment_share),
                    "placeholder_flag": "yes",
                    "source_gap": (
                        "Raw workbook does not include approved Phase 1 segment mix source values."
                    ),
                    "placeholder_method": "neutral_equal_share_default",
                }
            )
    return rows


def _build_cml_prevalent_pool_rows(
    module_level_rows: list[dict[str, str]],
    base_pools: tuple[BasePoolRecord, ...],
) -> list[dict[str, str]]:
    pool_lookup = {
        record.geography_code: record.annual_pool / Decimal("12") for record in base_pools
    }
    rows: list[dict[str, str]] = []
    unique_pairs = sorted(
        {
            (row["geography_code"], row["month_index"])
            for row in module_level_rows
            if row["module"] == "CML_Prevalent"
        }
    )
    for geography_code, month_index in unique_pairs:
        rows.append(
            {
                "geography_code": geography_code,
                "month_index": month_index,
                "addressable_prevalent_pool": _decimal_to_str(pool_lookup[geography_code]),
                "source_file": "treatable pts 250.xlsx",
                "source_value_type": "annual_pool_divided_by_12",
                "transformation_note": (
                    "Derived from raw annual CML prevalent pool because source lacks explicit monthly pool."
                ),
            }
        )
    return rows


def _build_geography_master_rows(annual_forecast: tuple[AnnualForecastRecord, ...]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for geography_code in sorted({record.geography_code for record in annual_forecast}):
        rows.append(
            {
                "geography_code": geography_code,
                "market_group": "PLACEHOLDER",
                "currency_code": "PLACEHOLDER",
                "launch_sequence_rank": "PLACEHOLDER",
                "source_file": "treatable pts 250.xlsx",
                "notes": "Only geography labels are present in raw workbook.",
            }
        )
    return rows


def _build_launch_timing_rows(annual_forecast: tuple[AnnualForecastRecord, ...]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for geography_code, module in sorted({(record.geography_code, record.module) for record in annual_forecast}):
        first_nonzero = min(
            (
                record.year_index
                for record in annual_forecast
                if record.geography_code == geography_code
                and record.module == module
                and record.annual_patients_treated > 0
            ),
            default=None,
        )
        if first_nonzero is None or (geography_code, module) in seen:
            continue
        seen.add((geography_code, module))
        rows.append(
            {
                "geography_code": geography_code,
                "module": module,
                "launch_year_index": str(first_nonzero),
                "launch_month_index": str(((first_nonzero - 1) * 12) + 1),
                "source_method": "inferred_from_first_nonzero_annual_forecast",
                "notes": "Current Phase 1 runner does not consume this file directly.",
            }
        )
    return rows


def _write_notes(path: Path, notes: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write("Extracted source notes from treatable pts 250.xlsx\n")
        handle.write("\n")
        for note in notes:
            handle.write(f"- {note}\n")


def build_real_scenario_01_curated_inputs(workbook_path: Path, output_dir: Path) -> dict[str, Path]:
    raw_data = extract_raw_scenario_data(workbook_path)
    module_level_rows = _build_module_level_forecast_rows(raw_data.annual_forecast)
    segment_level_rows = _build_segment_level_forecast_rows()
    aml_mix_rows = _build_mix_rows(module_level_rows, "AML", PLACEHOLDER_EQUAL_AML_SHARES)
    mds_mix_rows = _build_mix_rows(module_level_rows, "MDS", PLACEHOLDER_EQUAL_MDS_SHARES)
    cml_prevalent_rows = _build_cml_prevalent_pool_rows(module_level_rows, raw_data.base_pools)
    geography_rows = _build_geography_master_rows(raw_data.annual_forecast)
    launch_timing_rows = _build_launch_timing_rows(raw_data.annual_forecast)

    paths = {
        "commercial_forecast_module_level": output_dir / "commercial_forecast_module_level.csv",
        "commercial_forecast_segment_level": output_dir / "commercial_forecast_segment_level.csv",
        "epi_crosscheck": output_dir / "inp_epi_crosscheck.csv",
        "aml_segment_mix": output_dir / "aml_segment_mix.csv",
        "mds_segment_mix": output_dir / "mds_segment_mix.csv",
        "cml_prevalent": output_dir / "inp_cml_prevalent.csv",
        "geography_master": output_dir / "geography_master.csv",
        "launch_timing_inferred": output_dir / "launch_timing_inferred.csv",
        "source_notes": output_dir / "source_notes.txt",
    }

    _write_csv(
        paths["commercial_forecast_module_level"],
        [
            "geography_code",
            "module",
            "month_index",
            "patients_treated",
            "source_file",
            "source_grain",
            "transformation_note",
        ],
        module_level_rows,
    )
    _write_csv(
        paths["commercial_forecast_segment_level"],
        [
            "geography_code",
            "module",
            "segment_code",
            "month_index",
            "patients_treated",
        ],
        segment_level_rows,
    )
    _write_csv(
        paths["epi_crosscheck"],
        ["geography_code", "module", "month_index", "treatable_patients"],
        [],
    )
    _write_csv(
        paths["aml_segment_mix"],
        [
            "geography_code",
            "month_index",
            "segment_code",
            "segment_share",
            "placeholder_flag",
            "source_gap",
            "placeholder_method",
        ],
        aml_mix_rows,
    )
    _write_csv(
        paths["mds_segment_mix"],
        [
            "geography_code",
            "month_index",
            "segment_code",
            "segment_share",
            "placeholder_flag",
            "source_gap",
            "placeholder_method",
        ],
        mds_mix_rows,
    )
    _write_csv(
        paths["cml_prevalent"],
        [
            "geography_code",
            "month_index",
            "addressable_prevalent_pool",
            "source_file",
            "source_value_type",
            "transformation_note",
        ],
        cml_prevalent_rows,
    )
    _write_csv(
        paths["geography_master"],
        [
            "geography_code",
            "market_group",
            "currency_code",
            "launch_sequence_rank",
            "source_file",
            "notes",
        ],
        geography_rows,
    )
    _write_csv(
        paths["launch_timing_inferred"],
        [
            "geography_code",
            "module",
            "launch_year_index",
            "launch_month_index",
            "source_method",
            "notes",
        ],
        launch_timing_rows,
    )
    _write_notes(paths["source_notes"], raw_data.extracted_notes)
    return paths

