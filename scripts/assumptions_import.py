"""Import the CBX250 model assumptions workbook into normalized artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from cbx250_model.inputs.assumptions_import import import_model_assumptions_workbook  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Import the business-facing CBX250 assumptions workbook into normalized CSV artifacts "
            "plus generated Phase 2 configuration files."
        )
    )
    parser.add_argument(
        "--workbook",
        type=Path,
        default=REPO_ROOT / "templates" / "CBX250_Model_Assumptions_Template.xlsx",
        help="Path to the completed model assumptions workbook.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional override for the generated assumptions artifact directory.",
    )
    parser.add_argument(
        "--scenario-name",
        type=str,
        default=None,
        help="Optional scenario name override for the generated artifacts.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = import_model_assumptions_workbook(
        workbook_path=args.workbook,
        output_dir=args.output_dir,
        scenario_name_override=args.scenario_name,
    )
    print(
        json.dumps(
            {
                "workbook_path": str(result.workbook_path),
                "output_dir": str(result.output_dir),
                "scenario_name": result.context.scenario_name,
                "forecast_grain": result.context.forecast_grain,
                "forecast_frequency": result.context.forecast_frequency,
                "dose_basis_default": result.context.dose_basis_default,
                "row_counts": result.row_counts,
                "warnings": list(result.warnings),
                "generated_files": {key: str(path) for key, path in result.file_paths.items()},
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
