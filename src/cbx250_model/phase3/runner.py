"""End-to-end Phase 3 scenario runner."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from ..outputs.summary import format_validation_report
from ..validation.framework import ValidationReport
from .config_schema import Phase3Config, load_phase3_config
from .loaders import Phase3InputBundle, load_phase3_inputs
from .summary import build_phase3_run_summary
from .trade import build_phase3_outputs
from .validation import run_phase3_validations
from .writer import write_phase3_outputs


@dataclass(frozen=True)
class Phase3RunResult:
    config: Phase3Config
    inputs: Phase3InputBundle
    outputs: tuple
    validation: ValidationReport


def run_phase3_scenario(scenario_path: str | Path) -> Phase3RunResult:
    config = load_phase3_config(Path(scenario_path))
    inputs = load_phase3_inputs(config)
    outputs = build_phase3_outputs(config, inputs.phase2_deterministic_cascade)
    validation = run_phase3_validations(config, inputs.phase2_deterministic_cascade, outputs)
    return Phase3RunResult(
        config=config,
        inputs=inputs,
        outputs=outputs,
        validation=validation,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the CBX-250 Phase 3 deterministic trade layer.")
    parser.add_argument(
        "--scenario",
        default="config/scenarios/base_phase3.toml",
        help="Path to the Phase 3 scenario config file.",
    )
    args = parser.parse_args(argv)

    result = run_phase3_scenario(Path(args.scenario))
    output_path = write_phase3_outputs(
        result.config.output_paths.deterministic_trade_layer,
        result.outputs,
    )
    print(json.dumps(build_phase3_run_summary(result, str(output_path)), indent=2))

    rendered_report = format_validation_report(result.validation)
    if rendered_report:
        print(rendered_report)

    return 1 if result.validation.has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
