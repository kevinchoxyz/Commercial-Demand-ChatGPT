"""Generate the CBX250 model assumptions Excel template."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from cbx250_model.inputs.assumptions_template import build_model_assumptions_template  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate the business-facing CBX250 model assumptions workbook template."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "templates" / "CBX250_Model_Assumptions_Template.xlsx",
        help="Output workbook path.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = build_model_assumptions_template(args.output)
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
