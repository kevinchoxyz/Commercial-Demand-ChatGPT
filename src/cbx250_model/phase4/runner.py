"""End-to-end Phase 4 deterministic production scheduling runner."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from ..outputs.summary import format_validation_report
from ..validation.framework import ValidationReport
from .config_schema import Phase4Config, load_phase4_config
from .loaders import Phase4InputBundle, load_phase4_inputs
from .schedule import build_phase4_outputs
from .summary import build_phase4_run_summary
from .validation import run_phase4_validations
from .writer import (
    write_phase4_allocation_outputs,
    write_phase4_detail_outputs,
    write_phase4_monthly_summary,
)


@dataclass(frozen=True)
class Phase4RunResult:
    config: Phase4Config
    inputs: Phase4InputBundle
    schedule_detail: tuple
    monthly_summary: tuple
    allocation_detail: tuple
    validation: ValidationReport


def run_phase4_scenario(scenario_path: str | Path) -> Phase4RunResult:
    config = load_phase4_config(Path(scenario_path))
    inputs = load_phase4_inputs(config)
    schedule_detail, monthly_summary, allocation_detail = build_phase4_outputs(
        config,
        inputs.scheduling_signals,
    )
    validation = run_phase4_validations(
        config,
        inputs.phase3_trade_layer,
        schedule_detail,
        monthly_summary,
    )
    return Phase4RunResult(
        config=config,
        inputs=inputs,
        schedule_detail=schedule_detail,
        monthly_summary=monthly_summary,
        allocation_detail=allocation_detail,
        validation=validation,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the CBX-250 Phase 4 deterministic production scheduler.")
    parser.add_argument(
        "--scenario",
        default="config/scenarios/base_phase4.toml",
        help="Path to the Phase 4 scenario config file.",
    )
    args = parser.parse_args(argv)

    result = run_phase4_scenario(Path(args.scenario))
    detail_output_path = write_phase4_detail_outputs(
        result.config.output_paths.schedule_detail,
        result.schedule_detail,
    )
    monthly_summary_path = write_phase4_monthly_summary(
        result.config.output_paths.monthly_summary,
        result.monthly_summary,
    )
    allocation_output_path = write_phase4_allocation_outputs(
        result.config.output_paths.schedule_detail.with_name(
            result.config.output_paths.schedule_detail.name.replace(
                "_schedule_detail.csv",
                "_allocation_detail.csv",
            )
            if result.config.output_paths.schedule_detail.name.endswith("_schedule_detail.csv")
            else "phase4_allocation_detail.csv"
        ),
        result.allocation_detail,
    )
    print(
        json.dumps(
            build_phase4_run_summary(
                result,
                str(detail_output_path),
                str(monthly_summary_path),
                str(allocation_output_path),
            ),
            indent=2,
        )
    )

    rendered_report = format_validation_report(result.validation)
    if rendered_report:
        print(rendered_report)

    return 1 if result.validation.has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
