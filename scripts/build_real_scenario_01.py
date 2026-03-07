"""Build curated inputs and reconciliation output for real_scenario_01."""

from __future__ import annotations

from collections import Counter
from decimal import Decimal
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from cbx250_model.demand.phase1_runner import run_phase1_scenario  # noqa: E402
from cbx250_model.inputs.raw_ingest import (  # noqa: E402
    build_real_scenario_01_curated_inputs,
    extract_raw_scenario_data,
)


def _sum_annual_source_total(workbook_path: Path) -> Decimal:
    raw_data = extract_raw_scenario_data(workbook_path)
    total = Decimal("0")
    for record in raw_data.annual_forecast:
        total += record.annual_patients_treated
    return total


def _format_decimal(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.000001")), "f")


def main() -> int:
    workbook_path = REPO_ROOT / "data" / "raw" / "treatable pts 250.xlsx"
    output_dir = REPO_ROOT / "data" / "curated" / "real_scenario_01"

    build_real_scenario_01_curated_inputs(workbook_path, output_dir)
    result = run_phase1_scenario(REPO_ROOT / "config" / "scenarios" / "real_scenario_01.toml")

    source_total = _sum_annual_source_total(workbook_path)
    normalized_output_total = sum(Decimal(str(output.patients_treated)) for output in result.outputs)
    reconciliation_delta = normalized_output_total - source_total
    module_counts = Counter(output.module for output in result.outputs)
    segment_counts = Counter(output.segment_code for output in result.outputs)
    unresolved_data_gaps = [
        "AML segment mix source is missing in data/raw and is populated with clearly labeled equal-share placeholders.",
        "MDS segment mix source is missing in data/raw and is populated with clearly labeled equal-share placeholders.",
        "Raw workbook is annual; monthly normalized forecast uses an even 12-month split placeholder transformation.",
        "The raw workbook does not provide an explicit full-date Year 1 anchor, so the scenario parameter keeps the Phase 1 placeholder approval date.",
    ]

    reconciliation = {
        "scenario_name": result.config.scenario_name,
        "forecast_grain": result.config.model.forecast_grain,
        "geography_count": len(result.dimensions["dim_geography"]),
        "module_count": len(result.dimensions["dim_module"]),
        "row_counts_by_module": dict(module_counts),
        "row_counts_by_segment": dict(segment_counts),
        "source_forecast_total": _format_decimal(source_total),
        "normalized_output_total": _format_decimal(normalized_output_total),
        "difference_vs_source": _format_decimal(reconciliation_delta),
        "validation_issues": [
            {
                "code": issue.code,
                "message": issue.message,
                "level": issue.level,
                "context": issue.context,
            }
            for issue in result.validation.issues
        ],
        "unresolved_data_gaps": unresolved_data_gaps,
    }

    summary_path = output_dir / "reconciliation_summary.json"
    summary_path.write_text(json.dumps(reconciliation, indent=2), encoding="utf-8")
    markdown_path = output_dir / "reconciliation_summary.md"
    markdown_path.write_text(
        "\n".join(
            [
                "# Real Scenario 01 Reconciliation Summary",
                "",
                f"- scenario_name: `{reconciliation['scenario_name']}`",
                f"- forecast_grain: `{reconciliation['forecast_grain']}`",
                f"- geography_count: `{reconciliation['geography_count']}`",
                f"- module_count: `{reconciliation['module_count']}`",
                f"- source_forecast_total: `{reconciliation['source_forecast_total']}`",
                f"- normalized_output_total: `{reconciliation['normalized_output_total']}`",
                f"- difference_vs_source: `{reconciliation['difference_vs_source']}`",
                f"- validation_issue_count: `{len(reconciliation['validation_issues'])}`",
                "",
                "## Row Counts By Module",
                *[
                    f"- {module}: `{count}`"
                    for module, count in sorted(reconciliation["row_counts_by_module"].items())
                ],
                "",
                "## Row Counts By Segment",
                *[
                    f"- {segment}: `{count}`"
                    for segment, count in sorted(reconciliation["row_counts_by_segment"].items())
                ],
                "",
                "## Unresolved Data Gaps",
                *[f"- {gap}" for gap in unresolved_data_gaps],
            ]
        ),
        encoding="utf-8",
    )
    print(json.dumps(reconciliation, indent=2))
    return 1 if result.validation.has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
