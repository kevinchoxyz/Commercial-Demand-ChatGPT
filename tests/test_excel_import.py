from __future__ import annotations

from pathlib import Path
import csv
import json
import time
import xml.etree.ElementTree as ET
import zipfile

import pytest

from cbx250_model.inputs.excel_import import import_commercial_forecast_workbook
from cbx250_model.inputs.excel_template import build_commercial_forecast_template

MAIN_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
MAIN_NS_URI = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _sheet_path_map(workbook_path: Path) -> dict[str, str]:
    with zipfile.ZipFile(workbook_path) as zf:
        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        relationships = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))

    rel_lookup = {
        relationship.attrib["Id"]: relationship.attrib["Target"]
        for relationship in relationships.findall(f"{{{PKG_REL_NS}}}Relationship")
    }
    sheet_map: dict[str, str] = {}
    for sheet in workbook.findall("a:sheets/a:sheet", MAIN_NS):
        rel_id = sheet.attrib[f"{{{REL_NS}}}id"]
        sheet_map[sheet.attrib["name"]] = f"xl/{rel_lookup[rel_id]}"
    return sheet_map


def _replace_zip_entry(workbook_path: Path, entry_path: str, payload: bytes) -> None:
    temp_path = workbook_path.with_suffix(".tmp.xlsx")
    with zipfile.ZipFile(workbook_path) as source_zip:
        with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED) as target_zip:
            for member in source_zip.infolist():
                content = payload if member.filename == entry_path else source_zip.read(member.filename)
                target_zip.writestr(member, content)
    for _ in range(3):
        try:
            temp_path.replace(workbook_path)
            return
        except PermissionError:
            time.sleep(0.1)
    temp_path.replace(workbook_path)


def _set_cell(workbook_path: Path, sheet_name: str, cell_ref: str, value: str | float | int | None) -> None:
    worksheet_path = _sheet_path_map(workbook_path)[sheet_name]
    with zipfile.ZipFile(workbook_path) as zf:
        worksheet = ET.fromstring(zf.read(worksheet_path))

    target_cell: ET.Element | None = None
    for cell in worksheet.findall(".//a:sheetData/a:row/a:c", MAIN_NS):
        if cell.attrib["r"] == cell_ref:
            target_cell = cell
            break
    if target_cell is None:
        raise AssertionError(f"Cell {cell_ref} not found in {sheet_name}.")

    for child in list(target_cell):
        target_cell.remove(child)

    if value is None or value == "":
        target_cell.attrib.pop("t", None)
    elif isinstance(value, (int, float)):
        target_cell.attrib.pop("t", None)
        value_node = ET.SubElement(target_cell, f"{{{MAIN_NS_URI}}}v")
        value_node.text = str(value)
    else:
        target_cell.attrib["t"] = "inlineStr"
        inline_node = ET.SubElement(target_cell, f"{{{MAIN_NS_URI}}}is")
        text_node = ET.SubElement(inline_node, f"{{{MAIN_NS_URI}}}t")
        text_node.text = value

    _replace_zip_entry(workbook_path, worksheet_path, ET.tostring(worksheet, encoding="utf-8", xml_declaration=True))


def _find_row(rows: list[dict[str, str]], **criteria: str) -> dict[str, str]:
    for row in rows:
        if all(row[key] == value for key, value in criteria.items()):
            return row
    raise AssertionError(f"No row found matching {criteria!r}.")


def test_import_commercial_forecast_workbook_monthly_mode_writes_normalized_phase1_csvs(tmp_path: Path) -> None:
    workbook_path = tmp_path / "CBX250_Commercial_Forecast_Template.xlsx"
    output_dir = tmp_path / "normalized_submission"

    build_commercial_forecast_template(workbook_path)
    _set_cell(workbook_path, "Inputs", "B4", "monthly")
    _set_cell(workbook_path, "Inputs", "B5", "treated_census")
    result = import_commercial_forecast_workbook(workbook_path, output_dir=output_dir)

    assert result.context.scenario_name == "BASE_2029"
    assert result.context.forecast_grain == "module_level"
    assert result.context.forecast_frequency == "monthly"
    assert result.context.demand_basis == "treated_census"
    assert result.row_counts == {
        "geography_master": 2,
        "commercial_forecast_module_level": 4,
        "commercial_forecast_segment_level": 4,
        "aml_segment_mix": 72,
        "mds_segment_mix": 48,
        "inp_cml_prevalent": 33,
        "treatment_duration_assumptions": 0,
        "monthlyized_output": 7,
    }

    module_rows = _read_csv(output_dir / "commercial_forecast_module_level.csv")
    assert _find_row(
        module_rows,
        geography_code="US",
        module="AML",
        month_index="1",
    )["patients_treated"] == "25"

    cml_pool_rows = _read_csv(output_dir / "inp_cml_prevalent.csv")
    assert _find_row(
        cml_pool_rows,
        geography_code="US",
        month_index="1",
    )["addressable_prevalent_pool"] == "1.2"

    aml_mix_rows = _read_csv(output_dir / "aml_segment_mix.csv")
    assert _find_row(
        aml_mix_rows,
        geography_code="US",
        month_index="1",
        segment_code="1L_fit",
    )["segment_share"] == "0.4"
    assert _find_row(
        aml_mix_rows,
        geography_code="US",
        month_index="2",
        segment_code="1L_fit",
    )["segment_share"] == "0.5"

    monthlyized_rows = _read_csv(output_dir / "monthlyized_output.csv")
    assert _find_row(
        monthlyized_rows,
        geography_code="US",
        module="AML",
        segment_code="1L_fit",
        month_index="1",
    )["patients_treated_monthly"] == "10"
    assert _find_row(
        monthlyized_rows,
        geography_code="US",
        module="AML",
        segment_code="1L_fit",
        month_index="1",
    )["patients_active"] == "10"
    assert _find_row(
        monthlyized_rows,
        geography_code="EU",
        module="CML_Prevalent",
        segment_code="CML_Prevalent",
        month_index="1",
    )["patients_treated_monthly"] == "18"

    summary = json.loads((output_dir / "workbook_import_summary.json").read_text(encoding="utf-8"))
    assert summary["forecast_frequency"] == "monthly"
    assert summary["demand_basis"] == "treated_census"
    assert summary["cml_prevalent_primary_source"] == "explicit_forecast"
    assert any("authoritative normalized monthly workbook export" in note for note in summary["notes"])


def test_import_commercial_forecast_workbook_annual_mode_expands_annual_mix_and_allocates(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Commercial_Forecast_Template.xlsx"
    output_dir = tmp_path / "normalized_annual_submission"

    build_commercial_forecast_template(workbook_path)
    _set_cell(workbook_path, "Inputs", "B4", "annual")
    _set_cell(workbook_path, "Inputs", "B5", "treated_census")

    result = import_commercial_forecast_workbook(workbook_path, output_dir=output_dir)

    assert result.context.forecast_frequency == "annual"
    assert result.context.demand_basis == "treated_census"
    assert result.row_counts["commercial_forecast_module_level"] == 48
    assert result.row_counts["commercial_forecast_segment_level"] == 48
    assert result.row_counts["treatment_duration_assumptions"] == 0
    assert result.row_counts["monthlyized_output"] == 84

    module_rows = _read_csv(output_dir / "commercial_forecast_module_level.csv")
    assert _find_row(
        module_rows,
        geography_code="US",
        module="AML",
        month_index="1",
    )["patients_treated"] == "2.4"
    assert _find_row(
        module_rows,
        geography_code="EU",
        module="CML_Prevalent",
        month_index="1",
    )["patients_treated"] == "0.9"

    aml_mix_rows = _read_csv(output_dir / "aml_segment_mix.csv")
    assert _find_row(
        aml_mix_rows,
        geography_code="US",
        month_index="1",
        segment_code="1L_fit",
    )["segment_share"] == "0.4"
    assert _find_row(
        aml_mix_rows,
        geography_code="EU",
        month_index="12",
        segment_code="RR",
    )["segment_share"] == "0.25"

    monthlyized_rows = _read_csv(output_dir / "monthlyized_output.csv")
    assert _find_row(
        monthlyized_rows,
        geography_code="US",
        module="AML",
        segment_code="1L_fit",
        month_index="1",
    )["patients_treated_monthly"] == "0.96"
    assert _find_row(
        monthlyized_rows,
        geography_code="EU",
        module="CML_Prevalent",
        segment_code="CML_Prevalent",
        month_index="1",
    )["patients_treated_monthly"] == "0.9"


def test_import_commercial_forecast_workbook_monthly_override_mix_takes_precedence_over_annual_mix(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Commercial_Forecast_Template.xlsx"
    output_dir = tmp_path / "normalized_annual_override_submission"

    build_commercial_forecast_template(workbook_path)
    _set_cell(workbook_path, "Inputs", "B4", "annual")
    _set_cell(workbook_path, "Inputs", "B5", "treated_census")
    _set_cell(workbook_path, "AML_Mix", "D4", 1)
    _set_cell(workbook_path, "AML_Mix", "E4", 0.8)
    _set_cell(workbook_path, "AML_Mix", "F4", 0.1)
    _set_cell(workbook_path, "AML_Mix", "G4", 0.1)

    import_commercial_forecast_workbook(workbook_path, output_dir=output_dir)

    monthlyized_rows = _read_csv(output_dir / "monthlyized_output.csv")
    assert _find_row(
        monthlyized_rows,
        geography_code="US",
        module="AML",
        segment_code="1L_fit",
        month_index="1",
    )["patients_treated_monthly"] == "1.92"
    assert _find_row(
        monthlyized_rows,
        geography_code="US",
        module="AML",
        segment_code="1L_fit",
        month_index="2",
    )["patients_treated_monthly"] == "1.44"


def test_import_commercial_forecast_workbook_fails_when_required_mix_is_missing_at_both_levels(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Commercial_Forecast_Template.xlsx"

    build_commercial_forecast_template(workbook_path)
    _set_cell(workbook_path, "Inputs", "B4", "annual")
    _set_cell(workbook_path, "Inputs", "B5", "treated_census")
    for cell_ref in ("B2", "C2", "E2", "F2", "G2"):
        _set_cell(workbook_path, "AML_Mix", cell_ref, "")

    with pytest.raises(ValueError, match="AML_Mix is missing required annual or monthly coverage"):
        import_commercial_forecast_workbook(workbook_path)


def test_import_commercial_forecast_workbook_uses_cml_prevalent_fallback_when_explicit_rows_missing(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Commercial_Forecast_Template.xlsx"
    output_dir = tmp_path / "normalized_fallback_submission"

    build_commercial_forecast_template(workbook_path)
    _set_cell(workbook_path, "Inputs", "B4", "monthly")
    _set_cell(workbook_path, "Inputs", "B5", "treated_census")
    for cell_ref in ("B5", "C5", "D5", "F5", "G5"):
        _set_cell(workbook_path, "ModuleLevel_Forecast", cell_ref, "")

    result = import_commercial_forecast_workbook(workbook_path, output_dir=output_dir)

    assert result.row_counts["commercial_forecast_module_level"] == 36
    assert result.row_counts["monthlyized_output"] == 39
    assert result.row_counts["treatment_duration_assumptions"] == 0

    module_rows = _read_csv(output_dir / "commercial_forecast_module_level.csv")
    assert _find_row(
        module_rows,
        geography_code="US",
        module="CML_Prevalent",
        month_index="1",
    )["patients_treated"] == "0.6"

    summary = json.loads((output_dir / "workbook_import_summary.json").read_text(encoding="utf-8"))
    assert summary["cml_prevalent_primary_source"] == "assumption_fallback"
    assert any("fallback monthly demand was generated" in warning for warning in summary["warnings"])


def test_import_commercial_forecast_workbook_allows_explicit_cml_prevalent_without_assumptions(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Commercial_Forecast_Template.xlsx"
    output_dir = tmp_path / "normalized_explicit_without_pool_submission"

    build_commercial_forecast_template(workbook_path)
    _set_cell(workbook_path, "Inputs", "B4", "monthly")
    _set_cell(workbook_path, "Inputs", "B5", "treated_census")
    for row_number in range(2, 5):
        for column_letter in ("B", "C", "E", "F", "G", "H", "I", "J", "K"):
            _set_cell(workbook_path, "CML_Prevalent_Assumptions", f"{column_letter}{row_number}", "")

    result = import_commercial_forecast_workbook(workbook_path, output_dir=output_dir)

    assert result.context.forecast_frequency == "monthly"
    assert result.context.demand_basis == "treated_census"
    assert result.row_counts["inp_cml_prevalent"] == 0

    monthlyized_rows = _read_csv(output_dir / "monthlyized_output.csv")
    assert _find_row(
        monthlyized_rows,
        geography_code="EU",
        module="CML_Prevalent",
        segment_code="CML_Prevalent",
        month_index="1",
    )["patients_treated_monthly"] == "18"

    cml_pool_rows = _read_csv(output_dir / "inp_cml_prevalent.csv")
    assert cml_pool_rows == []

    summary = json.loads((output_dir / "workbook_import_summary.json").read_text(encoding="utf-8"))
    assert summary["cml_prevalent_primary_source"] == "explicit_forecast"
    assert any("No usable CML_Prevalent_Assumptions rows were provided" in warning for warning in summary["warnings"])


def test_import_commercial_forecast_workbook_fails_when_profile_weights_do_not_sum_to_100(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Commercial_Forecast_Template.xlsx"

    build_commercial_forecast_template(workbook_path)
    _set_cell(workbook_path, "Inputs", "B4", "annual")
    _set_cell(workbook_path, "Inputs", "B5", "treated_census")
    _set_cell(workbook_path, "Annual_to_Monthly_Profiles", "Q2", 0)

    with pytest.raises(ValueError, match="must sum to 100.0"):
        import_commercial_forecast_workbook(workbook_path)


def test_import_commercial_forecast_workbook_patient_starts_mode_rolls_forward_treated_census(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Commercial_Forecast_Template.xlsx"
    output_dir = tmp_path / "normalized_patient_starts_submission"
    treatment_duration_path = tmp_path / "treatment_duration_assumptions.csv"

    build_commercial_forecast_template(workbook_path)
    _set_cell(workbook_path, "Inputs", "B3", "segment_level")
    _set_cell(workbook_path, "Inputs", "B4", "monthly")
    _set_cell(workbook_path, "Inputs", "B5", "patient_starts")
    for row_number in range(2, 5):
        for column_letter in ("B", "C", "E", "F", "G", "H", "I", "J", "K"):
            _set_cell(workbook_path, "CML_Prevalent_Assumptions", f"{column_letter}{row_number}", "")
    for cell_ref in (
        "B2", "C2", "D2", "E2", "G2", "H2",
        "B3", "C3", "D3", "E3", "G3", "H3",
        "B4", "C4", "D4", "E4", "G4", "H4",
        "B5", "C5", "D5", "E5", "G5", "H5",
    ):
        _set_cell(workbook_path, "SegmentLevel_Forecast", cell_ref, "")
    _set_cell(workbook_path, "SegmentLevel_Forecast", "B2", "US")
    _set_cell(workbook_path, "SegmentLevel_Forecast", "C2", "AML")
    _set_cell(workbook_path, "SegmentLevel_Forecast", "D2", "1L_fit")
    _set_cell(workbook_path, "SegmentLevel_Forecast", "E2", 1)
    _set_cell(workbook_path, "SegmentLevel_Forecast", "G2", 10)
    _set_cell(workbook_path, "SegmentLevel_Forecast", "B3", "US")
    _set_cell(workbook_path, "SegmentLevel_Forecast", "C3", "AML")
    _set_cell(workbook_path, "SegmentLevel_Forecast", "D3", "1L_fit")
    _set_cell(workbook_path, "SegmentLevel_Forecast", "E3", 2)
    _set_cell(workbook_path, "SegmentLevel_Forecast", "G3", 10)

    treatment_duration_path.write_text(
        "\n".join(
            [
                "scenario_name,geography_code,module,segment_code,treatment_duration_months,active_flag,notes",
                "BASE_2029,ALL,AML,1L_fit,12,true,Approved base-case duration default.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = import_commercial_forecast_workbook(
        workbook_path,
        output_dir=output_dir,
        treatment_duration_path=treatment_duration_path,
    )

    monthlyized_rows = _read_csv(output_dir / "monthlyized_output.csv")
    assert result.context.demand_basis == "patient_starts"
    assert result.row_counts["treatment_duration_assumptions"] == 1
    assert _find_row(
        monthlyized_rows,
        geography_code="US",
        module="AML",
        segment_code="1L_fit",
        month_index="2",
    )["patients_treated_monthly"] == "20"
    month_13 = _find_row(
        monthlyized_rows,
        geography_code="US",
        module="AML",
        segment_code="1L_fit",
        month_index="13",
    )
    assert month_13["patients_treated_monthly"] == "10"
    assert month_13["patients_active"] == "10"
    assert month_13["patient_starts"] == "0"
    assert month_13["patients_continuing"] == "10"
    assert month_13["patients_rolloff"] == "10"
    assert month_13["starts_input"] == month_13["patient_starts"]
    assert month_13["continuing_patients"] == month_13["patients_continuing"]
    assert month_13["rolloff_patients"] == month_13["patients_rolloff"]
    assert month_13["treatment_duration_months_used"] == "12"


def test_import_commercial_forecast_workbook_patient_starts_mode_requires_duration_artifact(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "CBX250_Commercial_Forecast_Template.xlsx"

    build_commercial_forecast_template(workbook_path)
    _set_cell(workbook_path, "Inputs", "B4", "monthly")
    _set_cell(workbook_path, "Inputs", "B5", "patient_starts")

    with pytest.raises(ValueError, match="requires treatment duration assumptions"):
        import_commercial_forecast_workbook(workbook_path)
