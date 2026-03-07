"""Generate the CBX250 Commercial forecast Excel template."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from cbx250_model.inputs.excel_template import build_commercial_forecast_template  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate the business-facing CBX250 Commercial forecast workbook template."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "templates" / "CBX250_Commercial_Forecast_Template.xlsx",
        help="Output workbook path.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = build_commercial_forecast_template(args.output)
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
