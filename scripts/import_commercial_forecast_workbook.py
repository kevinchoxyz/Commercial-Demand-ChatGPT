"""Import a completed Commercial forecast workbook into normalized Phase 1 CSV inputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from cbx250_model.inputs.excel_import import import_commercial_forecast_workbook  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import the Commercial forecast workbook into normalized Phase 1 CSV inputs."
    )
    parser.add_argument(
        "--workbook",
        type=Path,
        default=REPO_ROOT / "templates" / "CBX250_Commercial_Forecast_Template.xlsx",
        help="Path to the completed Commercial forecast workbook.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional override for the normalized output directory.",
    )
    parser.add_argument(
        "--treatment-duration-file",
        type=Path,
        default=None,
        help="Optional treatment duration assumptions CSV. Required when the forecast workbook uses demand_basis=patient_starts.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = import_commercial_forecast_workbook(
        workbook_path=args.workbook,
        output_dir=args.output_dir,
        treatment_duration_path=args.treatment_duration_file,
    )
    print(
        json.dumps(
            {
                "workbook_path": str(result.workbook_path),
                "output_dir": str(result.output_dir),
                "scenario_name": result.context.scenario_name,
                "forecast_grain": result.context.forecast_grain,
                "forecast_frequency": result.context.forecast_frequency,
                "demand_basis": result.context.demand_basis,
                "us_aml_mds_initial_approval_date": result.context.us_aml_mds_initial_approval_date.isoformat(),
                "real_geography_list_confirmed": result.context.real_geography_list_confirmed,
                "row_counts": result.row_counts,
                "warnings": list(result.warnings),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
