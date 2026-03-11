"""One-command workbook import plus deterministic Phase 2 and optional downstream workflow runner."""

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
            "run the deterministic Phase 2 cascade, and optionally continue through the deterministic "
            "Phase 3, Phase 4, and Phase 5 layers."
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
        help="Optional business-facing assumptions workbook. If provided, its generated Phase 2 / Phase 3 / Phase 4 / Phase 5 configs become the active parameter sources for the phases that run.",
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
        "--phase3-scenario",
        type=Path,
        default=None,
        help="Optional Phase 3 scenario template to use when --run-phase3 is enabled and --assumptions-workbook is not provided. Defaults to config/scenarios/base_phase3.toml.",
    )
    parser.add_argument(
        "--phase4-scenario",
        type=Path,
        default=None,
        help="Optional Phase 4 scenario template to use when --run-phase4 or --run-phase5 is enabled and --assumptions-workbook is not provided. Defaults to config/scenarios/base_phase4.toml.",
    )
    parser.add_argument(
        "--phase5-scenario",
        type=Path,
        default=None,
        help="Optional Phase 5 scenario template to use when --run-phase5 is enabled and --assumptions-workbook is not provided. Defaults to config/scenarios/base_phase5.toml.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional directory for imported Phase 1 outputs, generated workflow scenarios, Phase 2 cascade output, and optional Phase 3 trade output.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow writing into an existing non-empty output directory.",
    )
    parser.add_argument(
        "--run-phase3",
        action="store_true",
        help="Run the deterministic Phase 3 trade layer after Phase 2.",
    )
    parser.add_argument(
        "--run-phase4",
        action="store_true",
        help="Run the deterministic Phase 4 production scheduler after Phase 3. This also runs Phase 3 if needed.",
    )
    parser.add_argument(
        "--run-phase5",
        action="store_true",
        help="Run the deterministic Phase 5 inventory layer after Phase 4. This also runs Phases 3 and 4 if needed.",
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
            phase3_scenario=args.phase3_scenario,
            phase4_scenario=args.phase4_scenario,
            phase5_scenario=args.phase5_scenario,
            output_dir=args.output_dir,
            overwrite=args.overwrite,
            run_phase3=args.run_phase3,
            run_phase4=args.run_phase4,
            run_phase5=args.run_phase5,
        )
    except (FileNotFoundError, NotADirectoryError, FileExistsError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_workflow_summary(result.summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
