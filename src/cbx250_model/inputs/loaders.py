"""CSV input loading for Phase 1."""

from __future__ import annotations

from csv import DictReader
from dataclasses import dataclass
from pathlib import Path

from .config_schema import Phase1Config
from .schemas import (
    CMLPrevalentPoolRecord,
    EpiCrosscheckRecord,
    ModuleLevelForecastRecord,
    SegmentLevelForecastRecord,
    SegmentMixRecord,
    TreatmentDurationRecord,
)


@dataclass(frozen=True)
class InputBundle:
    module_level_forecast: tuple[ModuleLevelForecastRecord, ...]
    segment_level_forecast: tuple[SegmentLevelForecastRecord, ...]
    epi_crosscheck: tuple[EpiCrosscheckRecord, ...]
    aml_segment_mix: tuple[SegmentMixRecord, ...]
    mds_segment_mix: tuple[SegmentMixRecord, ...]
    cml_prevalent: tuple[CMLPrevalentPoolRecord, ...]
    treatment_duration_assumptions: tuple[TreatmentDurationRecord, ...]


def _load_csv_rows(path: Path, required_columns: tuple[str, ...]) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Required input file not found: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"Input file has no header row: {path}")
        missing_columns = [column for column in required_columns if column not in reader.fieldnames]
        if missing_columns:
            raise ValueError(f"Input file {path} is missing columns: {missing_columns}")
        return list(reader)


def load_module_level_forecast(path: Path) -> tuple[ModuleLevelForecastRecord, ...]:
    rows = _load_csv_rows(
        path,
        ("geography_code", "module", "month_index", "patients_treated"),
    )
    return tuple(ModuleLevelForecastRecord.from_row(row) for row in rows)


def load_segment_level_forecast(path: Path) -> tuple[SegmentLevelForecastRecord, ...]:
    rows = _load_csv_rows(
        path,
        ("geography_code", "module", "segment_code", "month_index", "patients_treated"),
    )
    return tuple(SegmentLevelForecastRecord.from_row(row) for row in rows)


def load_epi_crosscheck(path: Path | None) -> tuple[EpiCrosscheckRecord, ...]:
    if path is None:
        return tuple()
    rows = _load_csv_rows(
        path,
        ("geography_code", "module", "month_index", "treatable_patients"),
    )
    return tuple(EpiCrosscheckRecord.from_row(row) for row in rows)


def load_segment_mix(path: Path, module: str) -> tuple[SegmentMixRecord, ...]:
    rows = _load_csv_rows(path, ("geography_code", "month_index", "segment_code", "segment_share"))
    return tuple(SegmentMixRecord.from_row(row, module=module) for row in rows)


def load_cml_prevalent(path: Path) -> tuple[CMLPrevalentPoolRecord, ...]:
    rows = _load_csv_rows(path, ("geography_code", "month_index", "addressable_prevalent_pool"))
    return tuple(CMLPrevalentPoolRecord.from_row(row) for row in rows)


def load_treatment_duration_assumptions(path: Path) -> tuple[TreatmentDurationRecord, ...]:
    rows = _load_csv_rows(
        path,
        ("geography_code", "module", "segment_code", "treatment_duration_months"),
    )
    return tuple(TreatmentDurationRecord.from_row(row) for row in rows)


def load_phase1_inputs(config: Phase1Config) -> InputBundle:
    return InputBundle(
        module_level_forecast=load_module_level_forecast(
            config.input_paths.commercial_forecast_module_level
        ),
        segment_level_forecast=load_segment_level_forecast(
            config.input_paths.commercial_forecast_segment_level
        ),
        epi_crosscheck=load_epi_crosscheck(config.input_paths.epi_crosscheck),
        aml_segment_mix=load_segment_mix(config.input_paths.aml_segment_mix, module="AML"),
        mds_segment_mix=load_segment_mix(config.input_paths.mds_segment_mix, module="MDS"),
        cml_prevalent=load_cml_prevalent(config.input_paths.cml_prevalent),
        treatment_duration_assumptions=load_treatment_duration_assumptions(
            config.input_paths.treatment_duration_assumptions
        ),
    )
