from __future__ import annotations

from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile

MAIN_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
MAIN_NS_URI = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


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


def row_values(workbook_path: Path, worksheet_path: str, row_number: int) -> list[str]:
    with zipfile.ZipFile(workbook_path) as zf:
        worksheet = ET.fromstring(zf.read(worksheet_path))

    for row in worksheet.findall(".//a:sheetData/a:row", MAIN_NS):
        if int(row.attrib["r"]) != row_number:
            continue
        values: list[str] = []
        for cell in row.findall("a:c", MAIN_NS):
            if cell.attrib.get("t") == "inlineStr":
                values.append("".join(node.text or "" for node in cell.findall(".//a:t", MAIN_NS)))
            else:
                values.append(cell.findtext("a:v", default="", namespaces=MAIN_NS))
        return values
    raise AssertionError(f"Row {row_number} not found in {worksheet_path}.")


def cell_formula(workbook_path: Path, worksheet_path: str, cell_ref: str) -> str:
    with zipfile.ZipFile(workbook_path) as zf:
        worksheet = ET.fromstring(zf.read(worksheet_path))

    for cell in worksheet.findall(".//a:sheetData/a:row/a:c", MAIN_NS):
        if cell.attrib["r"] == cell_ref:
            return cell.findtext("a:f", default="", namespaces=MAIN_NS)
    raise AssertionError(f"Cell {cell_ref} not found in {worksheet_path}.")


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

    _replace_zip_entry(
        workbook_path,
        worksheet_path,
        ET.tostring(worksheet, encoding="utf-8", xml_declaration=True),
    )


def _replace_zip_entry(workbook_path: Path, entry_path: str, payload: bytes) -> None:
    temp_path = workbook_path.with_suffix(".tmp.xlsx")
    with zipfile.ZipFile(workbook_path) as source_zip:
        with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED) as target_zip:
            for member in source_zip.infolist():
                content = payload if member.filename == entry_path else source_zip.read(member.filename)
                target_zip.writestr(member, content)
    temp_path.replace(workbook_path)
