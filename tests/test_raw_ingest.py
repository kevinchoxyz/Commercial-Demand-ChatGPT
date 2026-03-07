from __future__ import annotations

from pathlib import Path

from cbx250_model.inputs.raw_ingest import build_real_scenario_01_curated_inputs, extract_raw_scenario_data


def test_extract_raw_scenario_data_reads_expected_modules_and_years() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    raw_data = extract_raw_scenario_data(repo_root / "data" / "raw" / "treatable pts 250.xlsx")

    assert len(raw_data.annual_forecast) == 96
    assert {record.geography_code for record in raw_data.annual_forecast} == {"US", "EU"}
    assert {record.module for record in raw_data.annual_forecast} == {
        "AML",
        "MDS",
        "CML_Incident",
        "CML_Prevalent",
    }
    assert {record.year_index for record in raw_data.annual_forecast} == set(range(1, 13))


def test_build_real_scenario_01_curated_inputs_writes_expected_files(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    paths = build_real_scenario_01_curated_inputs(
        repo_root / "data" / "raw" / "treatable pts 250.xlsx",
        tmp_path / "real_scenario_01",
    )

    assert paths["commercial_forecast_module_level"].exists()
    assert paths["aml_segment_mix"].exists()
    assert paths["mds_segment_mix"].exists()
    assert paths["cml_prevalent"].exists()
    assert paths["launch_timing_inferred"].exists()

