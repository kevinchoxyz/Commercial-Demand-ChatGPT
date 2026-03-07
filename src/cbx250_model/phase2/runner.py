"""End-to-end Phase 2 scenario runner."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from ..outputs.summary import format_validation_report
from ..validation.framework import ValidationReport
from .cascade import build_phase2_outputs
from .config_schema import Phase2Config, load_phase2_config
from .loaders import Phase2InputBundle, load_phase2_inputs
from .summary import build_phase2_run_summary
from .validation import run_phase2_validations
from .writer import write_phase2_outputs


@dataclass(frozen=True)
class Phase2RunResult:
    config: Phase2Config
    inputs: Phase2InputBundle
    outputs: tuple
    validation: ValidationReport


def run_phase2_scenario(scenario_path: str | Path) -> Phase2RunResult:
    config = load_phase2_config(Path(scenario_path))
    inputs = load_phase2_inputs(config)
    outputs = build_phase2_outputs(config, inputs.phase1_monthlyized_output)
    validation = run_phase2_validations(config, inputs.phase1_monthlyized_output, outputs)
    return Phase2RunResult(
        config=config,
        inputs=inputs,
        outputs=outputs,
        validation=validation,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the CBX-250 Phase 2 deterministic cascade.")
    parser.add_argument(
        "--scenario",
        default="config/scenarios/base_phase2.toml",
        help="Path to the Phase 2 scenario config file.",
    )
    args = parser.parse_args(argv)

    result = run_phase2_scenario(Path(args.scenario))
    output_path = write_phase2_outputs(
        result.config.output_paths.deterministic_cascade,
        result.outputs,
    )
    print(json.dumps(build_phase2_run_summary(result, str(output_path)), indent=2))

    rendered_report = format_validation_report(result.validation)
    if rendered_report:
        print(rendered_report)

    return 1 if result.validation.has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
