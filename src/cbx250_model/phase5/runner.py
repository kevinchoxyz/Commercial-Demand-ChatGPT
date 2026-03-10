"""End-to-end Phase 5 deterministic inventory and shelf-life runner."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from ..outputs.summary import format_validation_report
from ..validation.framework import ValidationReport
from .config_schema import Phase5Config, load_phase5_config
from .inventory import build_phase5_outputs
from .loaders import Phase5InputBundle, load_phase5_inputs
from .summary import build_phase5_run_summary
from .validation import run_phase5_validations
from .writer import (
    write_phase5_cohort_audit,
    write_phase5_inventory_detail,
    write_phase5_monthly_summary,
)


@dataclass(frozen=True)
class Phase5RunResult:
    config: Phase5Config
    inputs: Phase5InputBundle
    inventory_detail: tuple
    monthly_summary: tuple
    cohort_audit: tuple
    validation: ValidationReport


def run_phase5_scenario(scenario_path: str | Path) -> Phase5RunResult:
    config = load_phase5_config(Path(scenario_path))
    inputs = load_phase5_inputs(config)
    inventory_detail, monthly_summary, cohort_audit = build_phase5_outputs(
        config,
        inputs.inventory_signals,
        inputs.phase4_schedule_detail,
    )
    validation = run_phase5_validations(
        config,
        inputs.phase3_trade_layer,
        inputs.phase4_schedule_detail,
        inputs.phase4_monthly_summary,
        inventory_detail,
        monthly_summary,
        cohort_audit,
    )
    return Phase5RunResult(
        config=config,
        inputs=inputs,
        inventory_detail=inventory_detail,
        monthly_summary=monthly_summary,
        cohort_audit=cohort_audit,
        validation=validation,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the CBX-250 Phase 5 deterministic inventory layer.")
    parser.add_argument(
        "--scenario",
        default="config/scenarios/base_phase5.toml",
        help="Path to the Phase 5 scenario config file.",
    )
    args = parser.parse_args(argv)

    result = run_phase5_scenario(Path(args.scenario))
    detail_output_path = write_phase5_inventory_detail(
        result.config.output_paths.inventory_detail,
        result.inventory_detail,
    )
    summary_output_path = write_phase5_monthly_summary(
        result.config.output_paths.monthly_inventory_summary,
        result.monthly_summary,
    )
    cohort_audit_output_path = write_phase5_cohort_audit(
        result.config.output_paths.cohort_audit,
        result.cohort_audit,
    )
    print(
        json.dumps(
            build_phase5_run_summary(
                result,
                str(detail_output_path),
                str(summary_output_path),
                str(cohort_audit_output_path),
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
