"""Thin end-to-end workflow wrapper for workbook import plus Phase 2 and optional Phase 3."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import os
import re
import tomllib

from .inputs.assumptions_import import AssumptionsImportResult, import_model_assumptions_workbook
from .inputs.excel_import import WorkbookImportResult, import_commercial_forecast_workbook
from .outputs.summary import format_validation_report
from .phase2.runner import Phase2RunResult, run_phase2_scenario
from .phase2.summary import build_phase2_run_summary
from .phase2.writer import write_phase2_outputs
from .phase3.runner import Phase3RunResult, run_phase3_scenario
from .phase3.summary import build_phase3_run_summary
from .phase3.writer import write_phase3_outputs


@dataclass(frozen=True)
class ForecastWorkflowResult:
    workbook_path: Path
    output_dir: Path
    scenario_name: str
    assumptions_workbook_path: Path | None
    assumptions_output_dir: Path | None
    phase2_template_path: Path
    generated_phase2_scenario_path: Path
    phase3_template_path: Path | None
    generated_phase3_scenario_path: Path | None
    phase1_monthlyized_output_path: Path
    phase2_output_path: Path
    phase3_output_path: Path | None
    assumptions_result: AssumptionsImportResult | None
    import_result: WorkbookImportResult
    phase2_result: Phase2RunResult
    phase3_result: Phase3RunResult | None
    summary: dict[str, object]


def run_forecast_workflow(
    *,
    workbook_path: str | Path,
    assumptions_workbook: str | Path | None = None,
    scenario_name: str | None = None,
    phase2_scenario: str | Path | None = None,
    phase3_scenario: str | Path | None = None,
    output_dir: str | Path | None = None,
    overwrite: bool = False,
    run_phase3: bool = False,
) -> ForecastWorkflowResult:
    repo_root = Path(__file__).resolve().parents[2]
    resolved_workbook_path = Path(workbook_path).resolve()
    if not resolved_workbook_path.exists() or not resolved_workbook_path.is_file():
        raise FileNotFoundError(f"Workbook not found: {resolved_workbook_path}")
    resolved_assumptions_workbook_path = (
        Path(assumptions_workbook).resolve() if assumptions_workbook is not None else None
    )
    if resolved_assumptions_workbook_path is not None and (
        not resolved_assumptions_workbook_path.exists() or not resolved_assumptions_workbook_path.is_file()
    ):
        raise FileNotFoundError(f"Assumptions workbook not found: {resolved_assumptions_workbook_path}")

    effective_scenario_name = scenario_name.strip() if scenario_name and scenario_name.strip() else _derive_safe_scenario_name(
        resolved_workbook_path
    )
    resolved_output_dir = _resolve_output_dir(
        repo_root=repo_root,
        scenario_name=effective_scenario_name,
        output_dir=output_dir,
    )
    _validate_output_dir_state(resolved_output_dir, overwrite=overwrite)

    workflow_warnings: list[str] = []
    assumptions_result: AssumptionsImportResult | None = None
    assumptions_output_dir: Path | None = None
    if resolved_assumptions_workbook_path is not None:
        if phase2_scenario is not None:
            workflow_warnings.append(
                f"assumptions_workbook was provided, so phase2_scenario {Path(phase2_scenario).resolve()} was ignored."
            )
        if run_phase3 and phase3_scenario is not None:
            workflow_warnings.append(
                f"assumptions_workbook was provided, so phase3_scenario {Path(phase3_scenario).resolve()} was ignored."
            )
        assumptions_output_dir = (resolved_output_dir / "assumptions").resolve()
        assumptions_result = _run_assumptions_import_step(
            workbook_path=resolved_assumptions_workbook_path,
            output_dir=assumptions_output_dir,
            scenario_name=effective_scenario_name,
        )
    elif phase3_scenario is not None and not run_phase3:
        workflow_warnings.append(
            f"phase3_scenario {Path(phase3_scenario).resolve()} was ignored because run_phase3 was not enabled."
        )

    import_result = _run_import_step(
        workbook_path=resolved_workbook_path,
        output_dir=resolved_output_dir,
        scenario_name=effective_scenario_name,
        treatment_duration_path=(
            assumptions_result.file_paths["treatment_duration_assumptions"]
            if assumptions_result is not None
            else None
        ),
    )
    if (
        assumptions_result is not None
        and assumptions_result.context.demand_basis != import_result.context.demand_basis
    ):
        raise ValueError(
            "Assumptions workbook demand_basis "
            f"{assumptions_result.context.demand_basis!r} does not match forecast workbook demand_basis "
            f"{import_result.context.demand_basis!r}."
        )
    monthlyized_output_path = import_result.file_paths.get("monthlyized_output")
    if monthlyized_output_path is None or not monthlyized_output_path.exists():
        raise FileNotFoundError(
            "Workbook import did not produce the authoritative Phase 1 output monthlyized_output.csv."
        )

    phase2_template_path = _resolve_phase2_template_path(
        repo_root=repo_root,
        assumptions_result=assumptions_result,
        phase2_scenario=phase2_scenario,
    )
    if not phase2_template_path.exists() or not phase2_template_path.is_file():
        raise FileNotFoundError(f"Phase 2 scenario template not found: {phase2_template_path}")

    generated_phase2_scenario_path = _write_generated_phase2_scenario(
        template_scenario_path=phase2_template_path,
        output_dir=resolved_output_dir,
        scenario_name=import_result.context.scenario_name,
        phase1_monthlyized_output_path=monthlyized_output_path,
    )
    phase2_result = run_phase2_scenario(generated_phase2_scenario_path)
    if phase2_result.validation.has_errors:
        rendered_report = format_validation_report(phase2_result.validation)
        raise ValueError(f"Phase 2 validation failed.\n{rendered_report}")

    phase2_output_path = write_phase2_outputs(
        phase2_result.config.output_paths.deterministic_cascade,
        phase2_result.outputs,
    )
    phase3_template_path: Path | None = None
    generated_phase3_scenario_path: Path | None = None
    phase3_result: Phase3RunResult | None = None
    phase3_output_path: Path | None = None
    if run_phase3:
        phase3_template_path = _resolve_phase3_template_path(
            repo_root=repo_root,
            assumptions_result=assumptions_result,
            phase3_scenario=phase3_scenario,
        )
        if not phase3_template_path.exists() or not phase3_template_path.is_file():
            raise FileNotFoundError(f"Phase 3 scenario template not found: {phase3_template_path}")

        generated_phase3_scenario_path = _write_generated_phase3_scenario(
            template_scenario_path=phase3_template_path,
            output_dir=resolved_output_dir,
            scenario_name=import_result.context.scenario_name,
            phase2_deterministic_cascade_path=phase2_output_path,
        )
        phase3_result = run_phase3_scenario(generated_phase3_scenario_path)
        if phase3_result.validation.has_errors:
            rendered_report = format_validation_report(phase3_result.validation)
            raise ValueError(f"Phase 3 validation failed.\n{rendered_report}")
        phase3_output_path = write_phase3_outputs(
            phase3_result.config.output_paths.deterministic_trade_layer,
            phase3_result.outputs,
        )
    summary = build_workflow_summary(
        assumptions_result=assumptions_result,
        import_result=import_result,
        phase2_result=phase2_result,
        phase2_output_path=phase2_output_path,
        phase2_template_path=phase2_template_path,
        generated_phase2_scenario_path=generated_phase2_scenario_path,
        phase3_result=phase3_result,
        phase3_output_path=phase3_output_path,
        phase3_template_path=phase3_template_path,
        generated_phase3_scenario_path=generated_phase3_scenario_path,
        workflow_warnings=tuple(workflow_warnings),
    )
    return ForecastWorkflowResult(
        workbook_path=resolved_workbook_path,
        output_dir=resolved_output_dir,
        scenario_name=import_result.context.scenario_name,
        assumptions_workbook_path=resolved_assumptions_workbook_path,
        assumptions_output_dir=assumptions_output_dir,
        phase2_template_path=phase2_template_path,
        generated_phase2_scenario_path=generated_phase2_scenario_path,
        phase3_template_path=phase3_template_path,
        generated_phase3_scenario_path=generated_phase3_scenario_path,
        phase1_monthlyized_output_path=monthlyized_output_path,
        phase2_output_path=phase2_output_path,
        phase3_output_path=phase3_output_path,
        assumptions_result=assumptions_result,
        import_result=import_result,
        phase2_result=phase2_result,
        phase3_result=phase3_result,
        summary=summary,
    )


def build_workflow_summary(
    *,
    assumptions_result: AssumptionsImportResult | None,
    import_result: WorkbookImportResult,
    phase2_result: Phase2RunResult,
    phase2_output_path: Path,
    phase2_template_path: Path,
    generated_phase2_scenario_path: Path,
    phase3_result: Phase3RunResult | None,
    phase3_output_path: Path | None,
    phase3_template_path: Path | None,
    generated_phase3_scenario_path: Path | None,
    workflow_warnings: tuple[str, ...],
) -> dict[str, object]:
    phase2_summary = build_phase2_run_summary(phase2_result, str(phase2_output_path))
    geography_count = (
        len({row.geography_code for row in phase2_result.outputs})
        if phase2_result.outputs
        else import_result.row_counts.get("geography_master", 0)
    )
    authoritative_output_files = {
        "phase1_monthlyized_output": str(import_result.file_paths["monthlyized_output"]),
        "phase2_deterministic_cascade": str(phase2_output_path),
    }
    phase3_summary: dict[str, object] | None = None
    if phase3_result is not None and phase3_output_path is not None:
        authoritative_output_files["phase3_trade_layer"] = str(phase3_output_path)
        phase3_summary = build_phase3_run_summary(phase3_result, str(phase3_output_path))
    assumptions_artifacts = None
    if assumptions_result is not None:
        assumptions_artifacts = {
            "assumptions_output_dir": str(assumptions_result.output_dir),
            "generated_phase2_scenario": str(assumptions_result.file_paths["generated_phase2_scenario"]),
            "generated_phase2_parameters": str(assumptions_result.file_paths["generated_phase2_parameters"]),
            "generated_phase3_scenario": str(assumptions_result.file_paths["generated_phase3_scenario"]),
            "generated_phase3_parameters": str(assumptions_result.file_paths["generated_phase3_parameters"]),
            "treatment_duration_assumptions": str(
                assumptions_result.file_paths["treatment_duration_assumptions"]
            ),
        }
    summary = {
        "scenario_name": import_result.context.scenario_name,
        "forecast_grain": import_result.context.forecast_grain,
        "forecast_frequency": import_result.context.forecast_frequency,
        "demand_basis": import_result.context.demand_basis,
        "geography_count": geography_count,
        "phase1_output_row_count": import_result.row_counts.get("monthlyized_output", 0),
        **phase2_summary,
        "phase2_parameter_source": (
            "assumptions_workbook" if assumptions_result is not None else "phase2_scenario_template"
        ),
        "phase2_parameter_template_used": str(phase2_template_path),
        "phase2_parameter_config_used": str(phase2_result.config.parameter_config_path),
        "assumptions_workbook_used": assumptions_result is not None,
        "assumptions_artifacts": assumptions_artifacts,
        "generated_phase2_scenario": str(generated_phase2_scenario_path),
        "authoritative_output_files": authoritative_output_files,
        "import_warning_count": len(import_result.warnings),
        "import_warnings": list(import_result.warnings),
        "assumptions_warning_count": len(assumptions_result.warnings) if assumptions_result is not None else 0,
        "assumptions_warnings": list(assumptions_result.warnings) if assumptions_result is not None else [],
        "workflow_warning_count": len(workflow_warnings),
        "workflow_warnings": list(workflow_warnings),
    }
    if phase3_summary is not None and phase3_template_path is not None and generated_phase3_scenario_path is not None:
        summary.update(
            {
                "phase3_ran": True,
                "phase3_input_row_count": phase3_summary["input_row_count"],
                "phase3_output_row_count": phase3_summary["output_row_count"],
                "phase3_output_rows_by_module": phase3_summary["output_rows_by_module"],
                "total_patient_fg_demand_units": phase3_summary["total_patient_fg_demand_units"],
                "total_sublayer2_pull_units": phase3_summary["total_sublayer2_pull_units"],
                "total_ex_factory_fg_demand_units": phase3_summary["total_ex_factory_fg_demand_units"],
                "total_ss_site_stocking_units": phase3_summary["total_ss_site_stocking_units"],
                "bullwhip_flag_row_count": phase3_summary["bullwhip_flag_row_count"],
                "phase3_validation_issue_count": phase3_summary["validation_issue_count"],
                "phase3_authoritative_output_file": phase3_summary["authoritative_output_file"],
                "phase3_parameter_source": (
                    "assumptions_workbook"
                    if assumptions_result is not None
                    else "phase3_scenario_template"
                ),
                "phase3_parameter_template_used": str(phase3_template_path),
                "phase3_parameter_config_used": str(phase3_result.config.parameter_config_path),
                "generated_phase3_scenario": str(generated_phase3_scenario_path),
            }
        )
    else:
        summary["phase3_ran"] = False
    return summary


def _run_import_step(
    *,
    workbook_path: Path,
    output_dir: Path,
    scenario_name: str,
    treatment_duration_path: Path | None,
) -> WorkbookImportResult:
    try:
        return import_commercial_forecast_workbook(
            workbook_path=workbook_path,
            output_dir=output_dir,
            scenario_name_override=scenario_name,
            treatment_duration_path=treatment_duration_path,
        )
    except ValueError as exc:
        raise ValueError(f"Workbook import failed: {exc}") from exc


def _run_assumptions_import_step(
    *,
    workbook_path: Path,
    output_dir: Path,
    scenario_name: str,
) -> AssumptionsImportResult:
    try:
        return import_model_assumptions_workbook(
            workbook_path=workbook_path,
            output_dir=output_dir,
            scenario_name_override=scenario_name,
        )
    except ValueError as exc:
        raise ValueError(f"Assumptions import failed: {exc}") from exc


def _resolve_output_dir(
    *,
    repo_root: Path,
    scenario_name: str,
    output_dir: str | Path | None,
) -> Path:
    if output_dir is not None:
        return Path(output_dir).resolve()
    return (repo_root / "data" / "curated" / _slugify_scenario_name(scenario_name)).resolve()


def _validate_output_dir_state(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists():
        if not output_dir.is_dir():
            raise NotADirectoryError(f"Output path is not a directory: {output_dir}")
        if any(output_dir.iterdir()) and not overwrite:
            raise FileExistsError(
                f"Output directory already exists and is not empty: {output_dir}. Re-run with --overwrite to replace generated files."
            )
        return
    output_dir.mkdir(parents=True, exist_ok=True)


def _resolve_phase2_template_path(
    *,
    repo_root: Path,
    assumptions_result: AssumptionsImportResult | None,
    phase2_scenario: str | Path | None,
) -> Path:
    if assumptions_result is not None:
        return assumptions_result.file_paths["generated_phase2_scenario"].resolve()
    if phase2_scenario is not None:
        return Path(phase2_scenario).resolve()
    return (repo_root / "config" / "scenarios" / "base_phase2.toml").resolve()


def _resolve_phase3_template_path(
    *,
    repo_root: Path,
    assumptions_result: AssumptionsImportResult | None,
    phase3_scenario: str | Path | None,
) -> Path:
    if assumptions_result is not None:
        return assumptions_result.file_paths["generated_phase3_scenario"].resolve()
    if phase3_scenario is not None:
        return Path(phase3_scenario).resolve()
    return (repo_root / "config" / "scenarios" / "base_phase3.toml").resolve()


def _write_generated_phase2_scenario(
    *,
    template_scenario_path: Path,
    output_dir: Path,
    scenario_name: str,
    phase1_monthlyized_output_path: Path,
) -> Path:
    template_data = _load_toml(template_scenario_path)
    raw_parameter_config = template_data.get("parameter_config")
    if not isinstance(raw_parameter_config, str) or not raw_parameter_config.strip():
        raise ValueError(
            f"Phase 2 scenario template {template_scenario_path} is missing parameter_config."
        )

    parameter_config_path = _resolve_relative_path(
        base_dir=template_scenario_path.parent,
        raw_path=raw_parameter_config,
    )
    if not parameter_config_path.exists():
        raise FileNotFoundError(
            f"Phase 2 parameter_config referenced by template does not exist: {parameter_config_path}"
        )

    generated_scenario_path = output_dir / "generated_phase2_scenario.toml"
    deterministic_cascade_path = output_dir / "phase2_deterministic_cascade.csv"
    parameter_config_ref = _relative_path_for_toml(parameter_config_path, start=output_dir)
    input_ref = _relative_path_for_toml(phase1_monthlyized_output_path, start=output_dir)
    output_ref = _relative_path_for_toml(deterministic_cascade_path, start=output_dir)

    generated_scenario_path.write_text(
        "\n".join(
            [
                "# GENERATED BY scripts/run_forecast_workflow.py",
                "# Thin orchestration layer around workbook import plus Phase 2 deterministic cascade.",
                f'scenario_name = "{scenario_name}"',
                f'parameter_config = "{parameter_config_ref}"',
                "",
                "[inputs]",
                f'phase1_monthlyized_output = "{input_ref}"',
                "",
                "[outputs]",
                f'deterministic_cascade = "{output_ref}"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return generated_scenario_path


def _write_generated_phase3_scenario(
    *,
    template_scenario_path: Path,
    output_dir: Path,
    scenario_name: str,
    phase2_deterministic_cascade_path: Path,
) -> Path:
    template_data = _load_toml(template_scenario_path)
    raw_parameter_config = template_data.get("parameter_config")
    if not isinstance(raw_parameter_config, str) or not raw_parameter_config.strip():
        raise ValueError(
            f"Phase 3 scenario template {template_scenario_path} is missing parameter_config."
        )

    parameter_config_path = _resolve_relative_path(
        base_dir=template_scenario_path.parent,
        raw_path=raw_parameter_config,
    )
    if not parameter_config_path.exists():
        raise FileNotFoundError(
            f"Phase 3 parameter_config referenced by template does not exist: {parameter_config_path}"
        )

    generated_scenario_path = output_dir / "generated_phase3_scenario.toml"
    deterministic_trade_layer_path = output_dir / "phase3_trade_layer.csv"
    parameter_config_ref = _relative_path_for_toml(parameter_config_path, start=output_dir)
    input_ref = _relative_path_for_toml(phase2_deterministic_cascade_path, start=output_dir)
    output_ref = _relative_path_for_toml(deterministic_trade_layer_path, start=output_dir)

    generated_scenario_path.write_text(
        "\n".join(
            [
                "# GENERATED BY scripts/run_forecast_workflow.py",
                "# Thin orchestration layer around workbook import plus deterministic Phase 2 and optional Phase 3.",
                f'scenario_name = "{scenario_name}"',
                f'parameter_config = "{parameter_config_ref}"',
                "",
                "[inputs]",
                f'phase2_deterministic_cascade = "{input_ref}"',
                "",
                "[outputs]",
                f'deterministic_trade_layer = "{output_ref}"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return generated_scenario_path


def _load_toml(path: Path) -> dict[str, object]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def _resolve_relative_path(*, base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else (base_dir / path).resolve()


def _relative_path_for_toml(path: Path, *, start: Path) -> str:
    return Path(os.path.relpath(path, start=start)).as_posix()


def _derive_safe_scenario_name(workbook_path: Path) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9]+", "_", workbook_path.stem).strip("_")
    return sanitized.upper() or "WORKBOOK_IMPORT"


def _slugify_scenario_name(scenario_name: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", scenario_name.strip()).strip("_").lower()
    return normalized or "forecast_workflow"


def format_workflow_summary(summary: dict[str, object]) -> str:
    return json.dumps(summary, indent=2)
