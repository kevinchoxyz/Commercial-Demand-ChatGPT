"""End-to-end Phase 1 scenario runner."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from ..calendar.monthly_calendar import MonthlyCalendar, build_monthly_calendar
from ..dimensions.tables import build_dimensions
from ..inputs.config_schema import Phase1Config, load_phase1_config
from ..inputs.loaders import InputBundle, load_phase1_inputs
from ..outputs.summary import build_run_summary, format_validation_report
from ..validation.framework import ValidationReport
from ..validation.rules import run_phase1_validations
from .aml import AMLDemandModule
from .base import DemandOutputRecord
from .cml_incident import CMLIncidentDemandModule
from .cml_prevalent import CMLPrevalentDemandModule
from .mds import MDSDemandModule


@dataclass(frozen=True)
class Phase1RunResult:
    config: Phase1Config
    inputs: InputBundle
    calendar: MonthlyCalendar
    dimensions: dict[str, list[dict[str, object]]]
    outputs: tuple[DemandOutputRecord, ...]
    validation: ValidationReport


def run_phase1_scenario(scenario_path: str | Path) -> Phase1RunResult:
    config = load_phase1_config(Path(scenario_path))
    inputs = load_phase1_inputs(config)
    calendar = build_monthly_calendar(
        config.horizon.us_aml_mds_initial_approval_date,
        config.horizon.forecast_horizon_months,
    )

    modules = (
        AMLDemandModule(),
        MDSDemandModule(),
        CMLIncidentDemandModule(),
        CMLPrevalentDemandModule(),
    )
    outputs = tuple(
        output
        for module in modules
        for output in module.build(config, calendar, inputs=inputs)
    )

    validation = run_phase1_validations(config, inputs, calendar, outputs)
    dimensions = build_dimensions(calendar, inputs)
    return Phase1RunResult(
        config=config,
        inputs=inputs,
        calendar=calendar,
        dimensions=dimensions,
        outputs=outputs,
        validation=validation,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the CBX-250 Phase 1 demand scaffold.")
    parser.add_argument(
        "--scenario",
        default="config/scenarios/base_phase1.toml",
        help="Path to the scenario config file.",
    )
    args = parser.parse_args(argv)

    result = run_phase1_scenario(Path(args.scenario))
    print(json.dumps(build_run_summary(result), indent=2))

    rendered_report = format_validation_report(result.validation)
    if rendered_report:
        print(rendered_report)

    return 1 if result.validation.has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
