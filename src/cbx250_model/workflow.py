"""Thin end-to-end workflow wrapper for workbook import plus deterministic downstream phases."""

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
from .phase4.runner import Phase4RunResult, run_phase4_scenario
from .phase4.summary import build_phase4_run_summary
from .phase4.writer import write_phase4_detail_outputs, write_phase4_monthly_summary
from .phase5.runner import Phase5RunResult, run_phase5_scenario
from .phase5.summary import build_phase5_run_summary
from .phase5.writer import (
    write_phase5_cohort_audit,
    write_phase5_inventory_detail,
    write_phase5_monthly_summary,
)


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
    phase4_template_path: Path | None
    generated_phase4_scenario_path: Path | None
    phase5_template_path: Path | None
    generated_phase5_scenario_path: Path | None
    phase1_monthlyized_output_path: Path
    phase2_output_path: Path
    phase3_output_path: Path | None
    phase4_schedule_detail_path: Path | None
    phase4_monthly_summary_path: Path | None
    phase5_inventory_detail_path: Path | None
    phase5_monthly_summary_path: Path | None
    phase5_cohort_audit_path: Path | None
    assumptions_result: AssumptionsImportResult | None
    import_result: WorkbookImportResult
    phase2_result: Phase2RunResult
    phase3_result: Phase3RunResult | None
    phase4_result: Phase4RunResult | None
    phase5_result: Phase5RunResult | None
    summary: dict[str, object]


def run_forecast_workflow(
    *,
    workbook_path: str | Path,
    assumptions_workbook: str | Path | None = None,
    scenario_name: str | None = None,
    phase2_scenario: str | Path | None = None,
    phase3_scenario: str | Path | None = None,
    phase4_scenario: str | Path | None = None,
    phase5_scenario: str | Path | None = None,
    output_dir: str | Path | None = None,
    overwrite: bool = False,
    run_phase3: bool = False,
    run_phase4: bool = False,
    run_phase5: bool = False,
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

    effective_run_phase5 = run_phase5
    effective_run_phase4 = run_phase4 or effective_run_phase5
    effective_run_phase3 = run_phase3 or effective_run_phase4

    workflow_warnings: list[str] = []
    assumptions_result: AssumptionsImportResult | None = None
    assumptions_output_dir: Path | None = None
    if resolved_assumptions_workbook_path is not None:
        if phase2_scenario is not None:
            workflow_warnings.append(
                f"assumptions_workbook was provided, so phase2_scenario {Path(phase2_scenario).resolve()} was ignored."
            )
        if effective_run_phase3 and phase3_scenario is not None:
            workflow_warnings.append(
                f"assumptions_workbook was provided, so phase3_scenario {Path(phase3_scenario).resolve()} was ignored."
            )
        if effective_run_phase4 and phase4_scenario is not None:
            workflow_warnings.append(
                f"assumptions_workbook was provided, so phase4_scenario {Path(phase4_scenario).resolve()} was ignored."
            )
        if effective_run_phase5 and phase5_scenario is not None:
            workflow_warnings.append(
                f"assumptions_workbook was provided, so phase5_scenario {Path(phase5_scenario).resolve()} was ignored."
            )
        assumptions_output_dir = (resolved_output_dir / "assumptions").resolve()
        assumptions_result = _run_assumptions_import_step(
            workbook_path=resolved_assumptions_workbook_path,
            output_dir=assumptions_output_dir,
            scenario_name=effective_scenario_name,
        )
    elif phase3_scenario is not None and not effective_run_phase3:
        workflow_warnings.append(
            f"phase3_scenario {Path(phase3_scenario).resolve()} was ignored because run_phase3 was not enabled."
        )
    if phase4_scenario is not None and not effective_run_phase4:
        workflow_warnings.append(
            f"phase4_scenario {Path(phase4_scenario).resolve()} was ignored because run_phase4 was not enabled."
        )
    if phase5_scenario is not None and not effective_run_phase5:
        workflow_warnings.append(
            f"phase5_scenario {Path(phase5_scenario).resolve()} was ignored because run_phase5 was not enabled."
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
    phase4_template_path: Path | None = None
    generated_phase4_scenario_path: Path | None = None
    phase4_result: Phase4RunResult | None = None
    phase4_schedule_detail_path: Path | None = None
    phase4_monthly_summary_path: Path | None = None
    phase5_template_path: Path | None = None
    generated_phase5_scenario_path: Path | None = None
    phase5_result: Phase5RunResult | None = None
    phase5_inventory_detail_path: Path | None = None
    phase5_monthly_summary_path: Path | None = None
    phase5_cohort_audit_path: Path | None = None
    if effective_run_phase3:
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
    if effective_run_phase4:
        if phase3_output_path is None:
            raise ValueError("Phase 4 requires a valid Phase 3 output path, but Phase 3 did not complete.")
        phase4_template_path = _resolve_phase4_template_path(
            repo_root=repo_root,
            assumptions_result=assumptions_result,
            phase4_scenario=phase4_scenario,
        )
        if not phase4_template_path.exists() or not phase4_template_path.is_file():
            raise FileNotFoundError(f"Phase 4 scenario template not found: {phase4_template_path}")
        generated_phase4_scenario_path = _write_generated_phase4_scenario(
            template_scenario_path=phase4_template_path,
            output_dir=resolved_output_dir,
            scenario_name=import_result.context.scenario_name,
            phase3_trade_layer_path=phase3_output_path,
        )
        phase4_result = run_phase4_scenario(generated_phase4_scenario_path)
        if phase4_result.validation.has_errors:
            rendered_report = format_validation_report(phase4_result.validation)
            raise ValueError(f"Phase 4 validation failed.\n{rendered_report}")
        phase4_schedule_detail_path = write_phase4_detail_outputs(
            phase4_result.config.output_paths.schedule_detail,
            phase4_result.schedule_detail,
        )
        phase4_monthly_summary_path = write_phase4_monthly_summary(
            phase4_result.config.output_paths.monthly_summary,
            phase4_result.monthly_summary,
        )
    if effective_run_phase5:
        if phase3_output_path is None or phase4_schedule_detail_path is None or phase4_monthly_summary_path is None:
            raise ValueError(
                "Phase 5 requires valid Phase 3 and Phase 4 outputs, but one or more upstream phases did not complete."
            )
        phase5_template_path = _resolve_phase5_template_path(
            repo_root=repo_root,
            assumptions_result=assumptions_result,
            phase5_scenario=phase5_scenario,
        )
        if not phase5_template_path.exists() or not phase5_template_path.is_file():
            raise FileNotFoundError(f"Phase 5 scenario template not found: {phase5_template_path}")
        generated_phase5_scenario_path = _write_generated_phase5_scenario(
            template_scenario_path=phase5_template_path,
            output_dir=resolved_output_dir,
            scenario_name=import_result.context.scenario_name,
            phase3_trade_layer_path=phase3_output_path,
            phase4_schedule_detail_path=phase4_schedule_detail_path,
            phase4_monthly_summary_path=phase4_monthly_summary_path,
        )
        phase5_result = run_phase5_scenario(generated_phase5_scenario_path)
        if phase5_result.validation.has_errors:
            rendered_report = format_validation_report(phase5_result.validation)
            raise ValueError(f"Phase 5 validation failed.\n{rendered_report}")
        phase5_inventory_detail_path = write_phase5_inventory_detail(
            phase5_result.config.output_paths.inventory_detail,
            phase5_result.inventory_detail,
        )
        phase5_monthly_summary_path = write_phase5_monthly_summary(
            phase5_result.config.output_paths.monthly_inventory_summary,
            phase5_result.monthly_summary,
        )
        phase5_cohort_audit_path = write_phase5_cohort_audit(
            phase5_result.config.output_paths.cohort_audit,
            phase5_result.cohort_audit,
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
        phase4_result=phase4_result,
        phase4_schedule_detail_path=phase4_schedule_detail_path,
        phase4_monthly_summary_path=phase4_monthly_summary_path,
        phase4_template_path=phase4_template_path,
        generated_phase4_scenario_path=generated_phase4_scenario_path,
        phase5_result=phase5_result,
        phase5_inventory_detail_path=phase5_inventory_detail_path,
        phase5_monthly_summary_path=phase5_monthly_summary_path,
        phase5_cohort_audit_path=phase5_cohort_audit_path,
        phase5_template_path=phase5_template_path,
        generated_phase5_scenario_path=generated_phase5_scenario_path,
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
        phase4_template_path=phase4_template_path,
        generated_phase4_scenario_path=generated_phase4_scenario_path,
        phase5_template_path=phase5_template_path,
        generated_phase5_scenario_path=generated_phase5_scenario_path,
        phase1_monthlyized_output_path=monthlyized_output_path,
        phase2_output_path=phase2_output_path,
        phase3_output_path=phase3_output_path,
        phase4_schedule_detail_path=phase4_schedule_detail_path,
        phase4_monthly_summary_path=phase4_monthly_summary_path,
        phase5_inventory_detail_path=phase5_inventory_detail_path,
        phase5_monthly_summary_path=phase5_monthly_summary_path,
        phase5_cohort_audit_path=phase5_cohort_audit_path,
        assumptions_result=assumptions_result,
        import_result=import_result,
        phase2_result=phase2_result,
        phase3_result=phase3_result,
        phase4_result=phase4_result,
        phase5_result=phase5_result,
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
    phase4_result: Phase4RunResult | None,
    phase4_schedule_detail_path: Path | None,
    phase4_monthly_summary_path: Path | None,
    phase4_template_path: Path | None,
    generated_phase4_scenario_path: Path | None,
    phase5_result: Phase5RunResult | None,
    phase5_inventory_detail_path: Path | None,
    phase5_monthly_summary_path: Path | None,
    phase5_cohort_audit_path: Path | None,
    phase5_template_path: Path | None,
    generated_phase5_scenario_path: Path | None,
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
    phase4_summary: dict[str, object] | None = None
    if (
        phase4_result is not None
        and phase4_schedule_detail_path is not None
        and phase4_monthly_summary_path is not None
    ):
        authoritative_output_files["phase4_schedule_detail"] = str(phase4_schedule_detail_path)
        authoritative_output_files["phase4_monthly_summary"] = str(phase4_monthly_summary_path)
        phase4_summary = build_phase4_run_summary(
            phase4_result,
            str(phase4_schedule_detail_path),
            str(phase4_monthly_summary_path),
        )
    phase5_summary: dict[str, object] | None = None
    if (
        phase5_result is not None
        and phase5_inventory_detail_path is not None
        and phase5_monthly_summary_path is not None
        and phase5_cohort_audit_path is not None
    ):
        authoritative_output_files["phase5_inventory_detail"] = str(phase5_inventory_detail_path)
        authoritative_output_files["phase5_monthly_summary"] = str(phase5_monthly_summary_path)
        authoritative_output_files["phase5_inventory_cohort_audit"] = str(phase5_cohort_audit_path)
        phase5_summary = build_phase5_run_summary(
            phase5_result,
            str(phase5_inventory_detail_path),
            str(phase5_monthly_summary_path),
            str(phase5_cohort_audit_path),
        )
    assumptions_artifacts = None
    if assumptions_result is not None:
        assumptions_artifacts = {
            "assumptions_output_dir": str(assumptions_result.output_dir),
            "generated_phase2_scenario": str(assumptions_result.file_paths["generated_phase2_scenario"]),
            "generated_phase2_parameters": str(assumptions_result.file_paths["generated_phase2_parameters"]),
            "generated_phase3_scenario": str(assumptions_result.file_paths["generated_phase3_scenario"]),
            "generated_phase3_parameters": str(assumptions_result.file_paths["generated_phase3_parameters"]),
            "generated_phase4_scenario": str(assumptions_result.file_paths["generated_phase4_scenario"]),
            "generated_phase4_parameters": str(assumptions_result.file_paths["generated_phase4_parameters"]),
            "generated_phase5_scenario": str(assumptions_result.file_paths["generated_phase5_scenario"]),
            "generated_phase5_parameters": str(assumptions_result.file_paths["generated_phase5_parameters"]),
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
    if phase4_summary is not None and phase4_template_path is not None and generated_phase4_scenario_path is not None:
        summary.update(
            {
                "phase4_ran": True,
                "phase4_input_row_count": phase4_summary["input_row_count"],
                "phase4_schedule_detail_row_count": phase4_summary["schedule_detail_row_count"],
                "phase4_schedule_detail_rows_by_stage": phase4_summary["schedule_detail_rows_by_stage"],
                "phase4_monthly_summary_row_count": phase4_summary["monthly_summary_row_count"],
                "total_fg_release_units": phase4_summary["total_fg_release_units"],
                "total_dp_release_units": phase4_summary["total_dp_release_units"],
                "total_ds_release_quantity_mg": phase4_summary["total_ds_release_quantity_mg"],
                "total_ds_release_quantity_kg": phase4_summary["total_ds_release_quantity_mg"] / 1_000_000.0,
                "total_ss_release_units": phase4_summary["total_ss_release_units"],
                "phase4_supply_gap_row_count": phase4_summary["supply_gap_row_count"],
                "phase4_bullwhip_review_row_count": phase4_summary["bullwhip_review_row_count"],
                "phase4_validation_issue_count": phase4_summary["validation_issue_count"],
                "phase4_schedule_detail_file": phase4_summary["authoritative_schedule_detail_file"],
                "phase4_monthly_summary_file": phase4_summary["authoritative_monthly_summary_file"],
                "phase4_parameter_source": (
                    "assumptions_workbook" if assumptions_result is not None else "phase4_scenario_template"
                ),
                "phase4_parameter_template_used": str(phase4_template_path),
                "phase4_parameter_config_used": str(phase4_result.config.parameter_config_path),
                "generated_phase4_scenario": str(generated_phase4_scenario_path),
            }
        )
    else:
        summary["phase4_ran"] = False
    if phase5_summary is not None and phase5_template_path is not None and generated_phase5_scenario_path is not None:
        final_month_index = (
            max(row.month_index for row in phase5_result.monthly_summary)
            if phase5_result and phase5_result.monthly_summary
            else 0
        )
        ending_fg_inventory_units = (
            sum(
                row.fg_inventory_units
                for row in phase5_result.monthly_summary
                if row.month_index == final_month_index
            )
            if phase5_result
            else 0.0
        )
        ending_ss_inventory_units = (
            sum(
                row.ss_inventory_units
                for row in phase5_result.monthly_summary
                if row.month_index == final_month_index
            )
            if phase5_result
            else 0.0
        )
        summary.update(
            {
                "phase5_ran": True,
                "phase5_inventory_detail_row_count": phase5_summary["inventory_detail_row_count"],
                "phase5_monthly_summary_row_count": phase5_summary["monthly_summary_row_count"],
                "phase5_cohort_audit_row_count": phase5_summary["cohort_audit_row_count"],
                "phase5_stockout_row_count": phase5_summary["stockout_row_count"],
                "phase5_excess_inventory_row_count": phase5_summary["excess_inventory_row_count"],
                "phase5_expiry_row_count": phase5_summary["expiry_row_count"],
                "phase5_fg_ss_mismatch_row_count": phase5_summary["fg_ss_mismatch_row_count"],
                "phase5_validation_issue_count": phase5_summary["validation_issue_count"],
                "ending_fg_inventory_units": ending_fg_inventory_units,
                "ending_ss_inventory_units": ending_ss_inventory_units,
                "phase5_inventory_detail_file": phase5_summary["authoritative_inventory_detail_file"],
                "phase5_monthly_inventory_summary_file": phase5_summary["authoritative_monthly_inventory_summary_file"],
                "phase5_cohort_audit_file": phase5_summary["authoritative_cohort_audit_file"],
                "phase5_parameter_source": (
                    "assumptions_workbook" if assumptions_result is not None else "phase5_scenario_template"
                ),
                "phase5_parameter_template_used": str(phase5_template_path),
                "phase5_parameter_config_used": str(phase5_result.config.parameter_config_path),
                "generated_phase5_scenario": str(generated_phase5_scenario_path),
            }
        )
    else:
        summary["phase5_ran"] = False
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


def _resolve_phase4_template_path(
    *,
    repo_root: Path,
    assumptions_result: AssumptionsImportResult | None,
    phase4_scenario: str | Path | None,
) -> Path:
    if assumptions_result is not None:
        return assumptions_result.file_paths["generated_phase4_scenario"].resolve()
    if phase4_scenario is not None:
        return Path(phase4_scenario).resolve()
    return (repo_root / "config" / "scenarios" / "base_phase4.toml").resolve()


def _resolve_phase5_template_path(
    *,
    repo_root: Path,
    assumptions_result: AssumptionsImportResult | None,
    phase5_scenario: str | Path | None,
) -> Path:
    if assumptions_result is not None:
        return assumptions_result.file_paths["generated_phase5_scenario"].resolve()
    if phase5_scenario is not None:
        return Path(phase5_scenario).resolve()
    return (repo_root / "config" / "scenarios" / "base_phase5.toml").resolve()


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


def _write_generated_phase4_scenario(
    *,
    template_scenario_path: Path,
    output_dir: Path,
    scenario_name: str,
    phase3_trade_layer_path: Path,
) -> Path:
    template_data = _load_toml(template_scenario_path)
    raw_parameter_config = template_data.get("parameter_config")
    if not isinstance(raw_parameter_config, str) or not raw_parameter_config.strip():
        raise ValueError(
            f"Phase 4 scenario template {template_scenario_path} is missing parameter_config."
        )

    parameter_config_path = _resolve_relative_path(
        base_dir=template_scenario_path.parent,
        raw_path=raw_parameter_config,
    )
    if not parameter_config_path.exists():
        raise FileNotFoundError(
            f"Phase 4 parameter_config referenced by template does not exist: {parameter_config_path}"
        )

    generated_scenario_path = output_dir / "generated_phase4_scenario.toml"
    schedule_detail_path = output_dir / "phase4_schedule_detail.csv"
    monthly_summary_path = output_dir / "phase4_monthly_summary.csv"
    parameter_config_ref = _relative_path_for_toml(parameter_config_path, start=output_dir)
    input_ref = _relative_path_for_toml(phase3_trade_layer_path, start=output_dir)
    detail_ref = _relative_path_for_toml(schedule_detail_path, start=output_dir)
    summary_ref = _relative_path_for_toml(monthly_summary_path, start=output_dir)

    generated_scenario_path.write_text(
        "\n".join(
            [
                "# GENERATED BY scripts/run_forecast_workflow.py",
                "# Thin orchestration layer around workbook import plus deterministic Phases 2 to 4.",
                f'scenario_name = "{scenario_name}"',
                f'parameter_config = "{parameter_config_ref}"',
                "",
                "[inputs]",
                f'phase3_trade_layer = "{input_ref}"',
                "",
                "[outputs]",
                f'schedule_detail = "{detail_ref}"',
                f'monthly_summary = "{summary_ref}"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return generated_scenario_path


def _write_generated_phase5_scenario(
    *,
    template_scenario_path: Path,
    output_dir: Path,
    scenario_name: str,
    phase3_trade_layer_path: Path,
    phase4_schedule_detail_path: Path,
    phase4_monthly_summary_path: Path,
) -> Path:
    template_data = _load_toml(template_scenario_path)
    raw_parameter_config = template_data.get("parameter_config")
    if not isinstance(raw_parameter_config, str) or not raw_parameter_config.strip():
        raise ValueError(
            f"Phase 5 scenario template {template_scenario_path} is missing parameter_config."
        )

    parameter_config_path = _resolve_relative_path(
        base_dir=template_scenario_path.parent,
        raw_path=raw_parameter_config,
    )
    if not parameter_config_path.exists():
        raise FileNotFoundError(
            f"Phase 5 parameter_config referenced by template does not exist: {parameter_config_path}"
        )

    generated_scenario_path = output_dir / "generated_phase5_scenario.toml"
    inventory_detail_path = output_dir / "phase5_inventory_detail.csv"
    monthly_summary_path = output_dir / "phase5_monthly_summary.csv"
    cohort_audit_path = output_dir / "phase5_inventory_cohort_audit.csv"
    parameter_config_ref = _relative_path_for_toml(parameter_config_path, start=output_dir)
    phase3_ref = _relative_path_for_toml(phase3_trade_layer_path, start=output_dir)
    phase4_detail_ref = _relative_path_for_toml(phase4_schedule_detail_path, start=output_dir)
    phase4_summary_ref = _relative_path_for_toml(phase4_monthly_summary_path, start=output_dir)
    detail_ref = _relative_path_for_toml(inventory_detail_path, start=output_dir)
    summary_ref = _relative_path_for_toml(monthly_summary_path, start=output_dir)
    cohort_ref = _relative_path_for_toml(cohort_audit_path, start=output_dir)

    generated_scenario_path.write_text(
        "\n".join(
            [
                "# GENERATED BY scripts/run_forecast_workflow.py",
                "# Thin orchestration layer around workbook import plus deterministic Phases 2 to 5.",
                f'scenario_name = "{scenario_name}"',
                f'parameter_config = "{parameter_config_ref}"',
                "",
                "[inputs]",
                f'phase3_trade_layer = "{phase3_ref}"',
                f'phase4_schedule_detail = "{phase4_detail_ref}"',
                f'phase4_monthly_summary = "{phase4_summary_ref}"',
                "",
                "[outputs]",
                f'inventory_detail = "{detail_ref}"',
                f'monthly_inventory_summary = "{summary_ref}"',
                f'cohort_audit = "{cohort_ref}"',
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
