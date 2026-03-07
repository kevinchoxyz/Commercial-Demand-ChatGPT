"""Thin end-to-end workflow wrapper for workbook import plus Phase 2 cascade."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import os
import re
import tomllib

from .inputs.excel_import import WorkbookImportResult, import_commercial_forecast_workbook
from .outputs.summary import format_validation_report
from .phase2.runner import Phase2RunResult, run_phase2_scenario
from .phase2.summary import build_phase2_run_summary
from .phase2.writer import write_phase2_outputs


@dataclass(frozen=True)
class ForecastWorkflowResult:
    workbook_path: Path
    output_dir: Path
    scenario_name: str
    phase2_template_path: Path
    generated_phase2_scenario_path: Path
    phase1_monthlyized_output_path: Path
    phase2_output_path: Path
    import_result: WorkbookImportResult
    phase2_result: Phase2RunResult
    summary: dict[str, object]


def run_forecast_workflow(
    *,
    workbook_path: str | Path,
    scenario_name: str | None = None,
    phase2_scenario: str | Path | None = None,
    output_dir: str | Path | None = None,
    overwrite: bool = False,
) -> ForecastWorkflowResult:
    repo_root = Path(__file__).resolve().parents[2]
    resolved_workbook_path = Path(workbook_path).resolve()
    if not resolved_workbook_path.exists() or not resolved_workbook_path.is_file():
        raise FileNotFoundError(f"Workbook not found: {resolved_workbook_path}")

    effective_scenario_name = scenario_name.strip() if scenario_name and scenario_name.strip() else _derive_safe_scenario_name(
        resolved_workbook_path
    )
    resolved_output_dir = _resolve_output_dir(
        repo_root=repo_root,
        scenario_name=effective_scenario_name,
        output_dir=output_dir,
    )
    _validate_output_dir_state(resolved_output_dir, overwrite=overwrite)

    import_result = _run_import_step(
        workbook_path=resolved_workbook_path,
        output_dir=resolved_output_dir,
        scenario_name=effective_scenario_name,
    )
    monthlyized_output_path = import_result.file_paths.get("monthlyized_output")
    if monthlyized_output_path is None or not monthlyized_output_path.exists():
        raise FileNotFoundError(
            "Workbook import did not produce the authoritative Phase 1 output monthlyized_output.csv."
        )

    phase2_template_path = Path(phase2_scenario).resolve() if phase2_scenario is not None else (
        repo_root / "config" / "scenarios" / "base_phase2.toml"
    ).resolve()
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
    summary = build_workflow_summary(
        import_result=import_result,
        phase2_result=phase2_result,
        phase2_output_path=phase2_output_path,
        generated_phase2_scenario_path=generated_phase2_scenario_path,
    )
    return ForecastWorkflowResult(
        workbook_path=resolved_workbook_path,
        output_dir=resolved_output_dir,
        scenario_name=import_result.context.scenario_name,
        phase2_template_path=phase2_template_path,
        generated_phase2_scenario_path=generated_phase2_scenario_path,
        phase1_monthlyized_output_path=monthlyized_output_path,
        phase2_output_path=phase2_output_path,
        import_result=import_result,
        phase2_result=phase2_result,
        summary=summary,
    )


def build_workflow_summary(
    *,
    import_result: WorkbookImportResult,
    phase2_result: Phase2RunResult,
    phase2_output_path: Path,
    generated_phase2_scenario_path: Path,
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
    return {
        "scenario_name": import_result.context.scenario_name,
        "forecast_grain": import_result.context.forecast_grain,
        "forecast_frequency": import_result.context.forecast_frequency,
        "geography_count": geography_count,
        "phase1_output_row_count": import_result.row_counts.get("monthlyized_output", 0),
        **phase2_summary,
        "generated_phase2_scenario": str(generated_phase2_scenario_path),
        "authoritative_output_files": authoritative_output_files,
        "import_warning_count": len(import_result.warnings),
        "import_warnings": list(import_result.warnings),
    }


def _run_import_step(
    *,
    workbook_path: Path,
    output_dir: Path,
    scenario_name: str,
) -> WorkbookImportResult:
    try:
        return import_commercial_forecast_workbook(
            workbook_path=workbook_path,
            output_dir=output_dir,
            scenario_name_override=scenario_name,
        )
    except ValueError as exc:
        raise ValueError(f"Workbook import failed: {exc}") from exc


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

