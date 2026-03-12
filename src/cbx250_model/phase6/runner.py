"""End-to-end Phase 6 deterministic financial analytics runner."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from ..outputs.summary import format_validation_report
from ..validation.framework import ValidationReport
from .config_schema import Phase6Config, load_phase6_config
from .finance import build_phase6_outputs
from .loaders import Phase6InputBundle, load_phase6_inputs
from .summary import build_phase6_run_summary
from .validation import run_phase6_validations
from .writer import (
    write_phase6_annual_summary,
    write_phase6_financial_detail,
    write_phase6_monthly_summary,
)


@dataclass(frozen=True)
class Phase6RunResult:
    config: Phase6Config
    inputs: Phase6InputBundle
    financial_detail: tuple
    monthly_summary: tuple
    annual_summary: tuple
    validation: ValidationReport


def run_phase6_scenario(scenario_path: str | Path) -> Phase6RunResult:
    config = load_phase6_config(Path(scenario_path))
    inputs = load_phase6_inputs(config)
    financial_detail, monthly_summary, annual_summary = build_phase6_outputs(
        config,
        inputs.phase4_monthly_summary,
        inputs.phase5_inventory_detail,
        inputs.phase5_monthly_inventory_summary,
    )
    validation = run_phase6_validations(
        config,
        inputs.phase4_monthly_summary,
        inputs.phase5_inventory_detail,
        inputs.phase5_monthly_inventory_summary,
        financial_detail,
        monthly_summary,
        annual_summary,
    )
    return Phase6RunResult(
        config=config,
        inputs=inputs,
        financial_detail=financial_detail,
        monthly_summary=monthly_summary,
        annual_summary=annual_summary,
        validation=validation,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the CBX-250 Phase 6 deterministic financial layer.")
    parser.add_argument(
        "--scenario",
        default="config/scenarios/base_phase6.toml",
        help="Path to the Phase 6 scenario config file.",
    )
    args = parser.parse_args(argv)

    result = run_phase6_scenario(Path(args.scenario))
    detail_output_path = write_phase6_financial_detail(
        result.config.output_paths.financial_detail,
        result.financial_detail,
    )
    monthly_summary_path = write_phase6_monthly_summary(
        result.config.output_paths.monthly_financial_summary,
        result.monthly_summary,
    )
    annual_summary_path = write_phase6_annual_summary(
        result.config.output_paths.annual_financial_summary,
        result.annual_summary,
    )
    print(
        json.dumps(
            build_phase6_run_summary(
                result,
                str(detail_output_path),
                str(monthly_summary_path),
                str(annual_summary_path),
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
