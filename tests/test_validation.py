from __future__ import annotations

from datetime import date
from pathlib import Path

from cbx250_model.inputs.config_schema import load_phase1_config
from cbx250_model.inputs.loaders import InputBundle
from cbx250_model.inputs.schemas import (
    CMLPrevalentPoolRecord,
    ModuleLevelForecastRecord,
    SegmentMixRecord,
)
from cbx250_model.validation.rules import validate_cml_prevalent_pool, validate_segment_mix_totals


def test_validate_segment_mix_totals_flags_incomplete_mix() -> None:
    records = (
        SegmentMixRecord(
            geography_code="US",
            module="AML",
            segment_code="1L_fit",
            month_index=1,
            segment_share=0.60,
        ),
        SegmentMixRecord(
            geography_code="US",
            module="AML",
            segment_code="1L_unfit",
            month_index=1,
            segment_share=0.30,
        ),
    )

    issues = validate_segment_mix_totals(records, "BASE", "AML", ("1L_fit", "1L_unfit", "RR"))

    assert len(issues) == 2
    assert {issue.code for issue in issues} == {
        "segment_mix.missing_segments",
        "segment_mix.sum_not_one",
    }


def test_validate_cml_prevalent_pool_flags_exceeded_pool() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config = load_phase1_config(repo_root / "config" / "scenarios" / "base_phase1.toml")
    inputs = InputBundle(
        module_level_forecast=(
            ModuleLevelForecastRecord(
                geography_code="US",
                module="CML_Prevalent",
                month_index=1,
                patients_treated=110.0,
            ),
        ),
        segment_level_forecast=tuple(),
        epi_crosscheck=tuple(),
        aml_segment_mix=tuple(),
        mds_segment_mix=tuple(),
        cml_prevalent=(
            CMLPrevalentPoolRecord(
                geography_code="US",
                month_index=1,
                addressable_prevalent_pool=100.0,
            ),
        ),
    )

    issues = validate_cml_prevalent_pool(config, inputs)

    assert len(issues) == 1
    assert issues[0].code == "cml_prevalent.pool_exceeded"
