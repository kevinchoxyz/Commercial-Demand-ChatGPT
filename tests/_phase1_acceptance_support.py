from __future__ import annotations

from pathlib import Path
import csv
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile

MAIN_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
MAIN_NS_URI = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def sum_csv_numeric(rows: list[dict[str, str]], column_name: str) -> float:
    return sum(float(row[column_name]) for row in rows)


def sheet_path_map(workbook_path: Path) -> dict[str, str]:
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


def replace_zip_entry(workbook_path: Path, entry_path: str, payload: bytes) -> None:
    with tempfile.NamedTemporaryFile(
        suffix=".xlsx",
        prefix=f"{workbook_path.stem}_",
        dir=workbook_path.parent,
        delete=False,
    ) as handle:
        temp_path = Path(handle.name)
    try:
        with zipfile.ZipFile(workbook_path) as source_zip:
            with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED) as target_zip:
                for member in source_zip.infolist():
                    content = payload if member.filename == entry_path else source_zip.read(member.filename)
                    target_zip.writestr(member, content)
        replace_error: PermissionError | None = None
        for _ in range(10):
            try:
                temp_path.replace(workbook_path)
                replace_error = None
                break
            except PermissionError as exc:
                replace_error = exc
                time.sleep(0.05)
        if replace_error is not None:
            raise replace_error
    finally:
        if temp_path.exists():
            temp_path.unlink()


def set_cell(workbook_path: Path, sheet_name: str, cell_ref: str, value: str | float | int | None) -> None:
    worksheet_path = sheet_path_map(workbook_path)[sheet_name]
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

    replace_zip_entry(
        workbook_path,
        worksheet_path,
        ET.tostring(worksheet, encoding="utf-8", xml_declaration=True),
    )


def clear_cells(workbook_path: Path, sheet_name: str, cell_refs: tuple[str, ...]) -> None:
    for cell_ref in cell_refs:
        set_cell(workbook_path, sheet_name, cell_ref, "")


def configure_template_for_mode(
    workbook_path: Path,
    *,
    forecast_grain: str,
    forecast_frequency: str,
) -> None:
    set_cell(workbook_path, "Inputs", "B3", forecast_grain)
    set_cell(workbook_path, "Inputs", "B4", forecast_frequency)

    # Keep the seeded CML prevalent examples inside the current Phase 1 pool guardrails.
    set_cell(workbook_path, "CML_Prevalent_Assumptions", "E4", 2000)
    set_cell(workbook_path, "CML_Prevalent_Assumptions", "G4", "CML_PREVALENT_LAUNCH")
    set_cell(workbook_path, "CML_Prevalent_Assumptions", "H4", 1)
    set_cell(workbook_path, "ModuleLevel_Forecast", "F5", 8)

    monthly_module_cells = (
        "B2", "C2", "D2", "F2", "G2",
        "B3", "C3", "D3", "F3", "G3",
        "B4", "C4", "D4", "F4", "G4",
        "B5", "C5", "D5", "F5", "G5",
    )
    monthly_segment_cells = (
        "B2", "C2", "D2", "E2", "G2", "H2",
        "B3", "C3", "D3", "E3", "G3", "H3",
        "B4", "C4", "D4", "E4", "G4", "H4",
        "B5", "C5", "D5", "E5", "G5", "H5",
    )
    annual_module_cells = (
        "B2", "C2", "D2", "F2", "G2", "H2",
        "B3", "C3", "D3", "F3", "G3", "H3",
        "B4", "C4", "D4", "F4", "G4", "H4",
        "B5", "C5", "D5", "F5", "G5", "H5",
    )
    annual_segment_cells = (
        "B2", "C2", "D2", "E2", "G2", "H2", "I2",
        "B3", "C3", "D3", "E3", "G3", "H3", "I3",
        "B4", "C4", "D4", "E4", "G4", "H4", "I4",
        "B5", "C5", "D5", "E5", "G5", "H5", "I5",
    )

    if forecast_frequency == "monthly":
        clear_cells(workbook_path, "Annual_ModuleLevel_Forecast", annual_module_cells)
        clear_cells(workbook_path, "Annual_SegmentLevel_Forecast", annual_segment_cells)
    else:
        clear_cells(workbook_path, "ModuleLevel_Forecast", monthly_module_cells)
        clear_cells(workbook_path, "SegmentLevel_Forecast", monthly_segment_cells)

    if forecast_grain == "module_level":
        if forecast_frequency == "monthly":
            clear_cells(workbook_path, "SegmentLevel_Forecast", monthly_segment_cells)
        else:
            clear_cells(workbook_path, "Annual_SegmentLevel_Forecast", annual_segment_cells)
    else:
        if forecast_frequency == "monthly":
            clear_cells(workbook_path, "ModuleLevel_Forecast", monthly_module_cells)
        else:
            clear_cells(workbook_path, "Annual_ModuleLevel_Forecast", annual_module_cells)


def write_import_backed_phase1_scenario(
    tmp_path: Path,
    *,
    scenario_name: str,
    forecast_grain: str,
    input_dir: Path,
) -> Path:
    config_dir = tmp_path / "config"
    parameters_dir = config_dir / "parameters"
    scenarios_dir = config_dir / "scenarios"
    parameters_dir.mkdir(parents=True, exist_ok=True)
    scenarios_dir.mkdir(parents=True, exist_ok=True)

    (parameters_dir / "phase1.toml").write_text(
        "\n".join(
            [
                "[model]",
                "phase = 1",
                'build_scope = "deterministic_demand_foundation"',
                'primary_demand_input = "Commercial Patients Treated"',
                f'forecast_grain = "{forecast_grain}"',
                "",
                "[horizon]",
                'us_aml_mds_initial_approval_date = "2029-01-01"',
                "forecast_horizon_months = 240",
                'time_grain = "monthly"',
                "",
                "[modules]",
                'enabled = ["AML", "MDS", "CML_Incident", "CML_Prevalent"]',
                'disabled = ["trade", "production", "inventory", "financials", "monte_carlo"]',
                "",
                "[validation]",
                "enforce_segment_share_rules = true",
                "enforce_cml_prevalent_pool_constraints = true",
                "enforce_epi_crosscheck_warning = false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (scenarios_dir / "scenario.toml").write_text(
        "\n".join(
            [
                f'scenario_name = "{scenario_name}"',
                'parameter_config = "../parameters/phase1.toml"',
                "",
                "[inputs]",
                f'commercial_forecast_module_level = "{(input_dir / "commercial_forecast_module_level.csv").as_posix()}"',
                f'commercial_forecast_segment_level = "{(input_dir / "commercial_forecast_segment_level.csv").as_posix()}"',
                f'epi_crosscheck = "{(input_dir / "inp_epi_crosscheck.csv").as_posix()}"',
                f'aml_segment_mix = "{(input_dir / "aml_segment_mix.csv").as_posix()}"',
                f'mds_segment_mix = "{(input_dir / "mds_segment_mix.csv").as_posix()}"',
                f'cml_prevalent = "{(input_dir / "inp_cml_prevalent.csv").as_posix()}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return scenarios_dir / "scenario.toml"


def write_curated_phase1_scenario(
    tmp_path: Path,
    *,
    forecast_grain: str,
    module_level_rows: list[str] | None = None,
    segment_level_rows: list[str] | None = None,
    aml_mix_rows: list[str] | None = None,
    mds_mix_rows: list[str] | None = None,
    cml_prevalent_rows: list[str] | None = None,
) -> Path:
    config_dir = tmp_path / "config"
    parameters_dir = config_dir / "parameters"
    scenarios_dir = config_dir / "scenarios"
    data_dir = tmp_path / "data"
    parameters_dir.mkdir(parents=True, exist_ok=True)
    scenarios_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    (parameters_dir / "phase1.toml").write_text(
        "\n".join(
            [
                "[model]",
                "phase = 1",
                'build_scope = "deterministic_demand_foundation"',
                'primary_demand_input = "Commercial Patients Treated"',
                f'forecast_grain = "{forecast_grain}"',
                "",
                "[horizon]",
                'us_aml_mds_initial_approval_date = "2029-01-01"',
                "forecast_horizon_months = 240",
                'time_grain = "monthly"',
                "",
                "[modules]",
                'enabled = ["AML", "MDS", "CML_Incident", "CML_Prevalent"]',
                'disabled = ["trade", "production", "inventory", "financials", "monte_carlo"]',
                "",
                "[validation]",
                "enforce_segment_share_rules = true",
                "enforce_cml_prevalent_pool_constraints = true",
                "enforce_epi_crosscheck_warning = false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (scenarios_dir / "scenario.toml").write_text(
        "\n".join(
            [
                'scenario_name = "ACCEPTANCE_BASE"',
                'parameter_config = "../parameters/phase1.toml"',
                "",
                "[inputs]",
                'commercial_forecast_module_level = "../../data/commercial_forecast_module_level.csv"',
                'commercial_forecast_segment_level = "../../data/commercial_forecast_segment_level.csv"',
                'epi_crosscheck = "../../data/inp_epi_crosscheck.csv"',
                'aml_segment_mix = "../../data/aml_segment_mix.csv"',
                'mds_segment_mix = "../../data/mds_segment_mix.csv"',
                'cml_prevalent = "../../data/inp_cml_prevalent.csv"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (data_dir / "commercial_forecast_module_level.csv").write_text(
        "\n".join(
            [
                "geography_code,module,month_index,patients_treated",
                *(module_level_rows or []),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (data_dir / "commercial_forecast_segment_level.csv").write_text(
        "\n".join(
            [
                "geography_code,module,segment_code,month_index,patients_treated",
                *(segment_level_rows or []),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (data_dir / "inp_epi_crosscheck.csv").write_text(
        "geography_code,module,month_index,treatable_patients\n",
        encoding="utf-8",
    )
    (data_dir / "aml_segment_mix.csv").write_text(
        "\n".join(
            [
                "geography_code,month_index,segment_code,segment_share",
                *(aml_mix_rows or []),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (data_dir / "mds_segment_mix.csv").write_text(
        "\n".join(
            [
                "geography_code,month_index,segment_code,segment_share",
                *(mds_mix_rows or []),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (data_dir / "inp_cml_prevalent.csv").write_text(
        "\n".join(
            [
                "geography_code,month_index,addressable_prevalent_pool",
                *(cml_prevalent_rows or []),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return scenarios_dir / "scenario.toml"
