"""One-command workbook import plus deterministic Phase 2 workflow runner."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from cbx250_model.workflow import format_workflow_summary, run_forecast_workflow


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Import a Commercial forecast workbook, generate authoritative monthlyized_output.csv, "
            "and run the deterministic Phase 2 cascade."
        )
    )
    parser.add_argument(
        "--workbook",
        type=Path,
        required=True,
        help="Path to the completed Commercial forecast workbook.",
    )
    parser.add_argument(
        "--assumptions-workbook",
        type=Path,
        default=None,
        help="Optional business-facing assumptions workbook. If provided, its generated Phase 2 config becomes the active parameter source.",
    )
    parser.add_argument(
        "--scenario-name",
        type=str,
        default=None,
        help="Optional scenario name override. If omitted, a safe default is derived from the workbook filename.",
    )
    parser.add_argument(
        "--phase2-scenario",
        type=Path,
        default=None,
        help="Optional Phase 2 scenario template to use as the parameter_config source. Defaults to config/scenarios/base_phase2.toml when --assumptions-workbook is not provided.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional directory for imported Phase 1 outputs, generated Phase 2 scenario, and Phase 2 cascade output.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow writing into an existing non-empty output directory.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = run_forecast_workflow(
            workbook_path=args.workbook,
            assumptions_workbook=args.assumptions_workbook,
            scenario_name=args.scenario_name,
            phase2_scenario=args.phase2_scenario,
            output_dir=args.output_dir,
            overwrite=args.overwrite,
        )
    except (FileNotFoundError, NotADirectoryError, FileExistsError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_workflow_summary(result.summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
