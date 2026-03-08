# CBX-250 Commercial Planning Model

This repository contains the accepted Phase 1 deterministic demand foundation plus the deterministic Phase 2 dose and unit cascade for the CBX-250 commercial planning model. Later-phase capabilities remain explicit placeholders.

## Read These Files First
- `docs/model_contract/model_contract_phase0.md`
- `docs/model_contract/variable_registry_v1.md`
- `docs/model_contract/correlation_matrix_scaffold_v1.md`
- `docs/model_contract/data_dictionary_skeleton.md`
- `AGENTS.md`

## Current Scope
- Accepted Phase 1 deterministic demand foundation
- Deterministic Phase 2 dose and unit cascade
- Monthly 240-month horizon
- Config-driven setup
- Demand logic for AML, MDS, CML Incident, and CML Prevalent
- Phase 2 cascade outputs for doses, FG, SS, DP, and DS
- Configurable commercial forecast grain:
  - `module_level`
  - `segment_level`
- Configurable deterministic dose basis:
  - `fixed`
  - `weight_based`
- Validation scaffolds for AML/MDS segment shares and CML prevalent pool limits

## Quick Start
- `python -m pytest`
- `python -m pytest tests/test_phase1_acceptance.py`
- `python -m pytest tests/test_phase2_runner.py tests/test_phase2_acceptance.py`
- `python -m pytest tests/test_forecast_workflow.py`
- `python -m pytest tests/test_assumptions_template.py tests/test_assumptions_import.py`
- `python scripts/run_phase1.py --scenario config/scenarios/base_phase1.toml`
- `python scripts/build_real_scenario_01.py`
- `python scripts/run_phase1.py --scenario config/scenarios/real_scenario_01.toml`
- `python scripts/generate_commercial_forecast_template.py`
- `python scripts/import_commercial_forecast_workbook.py --workbook templates/CBX250_Commercial_Forecast_Template.xlsx`
- `python scripts/generate_model_assumptions_template.py`
- `python scripts/assumptions_import.py --workbook templates/CBX250_Model_Assumptions_Template.xlsx`
- `python scripts/run_phase2.py --scenario config/scenarios/base_phase2.toml`
- `python scripts/run_forecast_workflow.py --workbook "data/raw/CBX250_Commercial_Forecast_REAL.xlsx" --scenario-name "REAL_2029" --overwrite`

## Forecast Templates
- `templates/CBX250_Commercial_Forecast_Template.xlsx`
- `python scripts/generate_commercial_forecast_template.py`
- `python scripts/import_commercial_forecast_workbook.py --workbook <completed_workbook.xlsx>`
- `data/reference/commercial_forecast_module_level_template.csv`
- `data/reference/commercial_forecast_segment_level_template.csv`
- `data/reference/aml_segment_mix_template.csv`
- `data/reference/mds_segment_mix_template.csv`
- `templates/README.md` explains:
  - when to use monthly vs annual entry
  - when to use `module_level` vs `segment_level`
  - how annual-to-monthly monthlyization works
  - how `AML_Mix` and `MDS_Mix` can be entered by `year_index` for annual `module_level` submissions, with optional `month_index` overrides taking precedence
  - how `CML_Prevalent_Assumptions` optionally feeds `inp_cml_prevalent.csv` for validation and can generate fallback demand when explicit CML prevalent forecast rows are absent
  - which sheets are user-entry vs generated

## Workbook Import Outputs
- `data/curated/<scenario_name>/monthlyized_output.csv` is the authoritative normalized monthly workbook export for Phase 1.
- `data/curated/<scenario_name>/commercial_forecast_module_level.csv` or `data/curated/<scenario_name>/commercial_forecast_segment_level.csv` remain the lower-level normalized contract inputs consumed by the current Phase 1 runner, depending on `forecast_grain`.
- `data/curated/<scenario_name>/inp_cml_prevalent.csv` is the CML prevalent validation-pool file generated from workbook assumptions when usable assumptions are provided; otherwise it is written as header-only and the importer warns that no pool validation was generated.
- The workbook tab `Monthlyized_Output` is a generated/reference placeholder in Phase 1 unless future exporter logic explicitly writes rows back into the workbook.

## Assumptions Workbook
- Use `templates/CBX250_Model_Assumptions_Template.xlsx` as the business-facing assumptions entry point instead of hand-editing Phase 2 TOML files.
- Generate a fresh workbook with:
  - `python scripts/generate_model_assumptions_template.py`
- Import a completed assumptions workbook with:
  - `python scripts/assumptions_import.py --workbook templates/CBX250_Model_Assumptions_Template.xlsx`
- By default the importer writes normalized artifacts under:
  - `data/outputs/<scenario_name>/assumptions/`
- Generated artifacts include:
  - `scenario_controls.csv`
  - `launch_timing.csv`
  - `dosing_assumptions.csv`
  - `product_parameters.csv`
  - `yield_assumptions.csv`
  - `packaging_and_vialing.csv`
  - `ss_assumptions.csv`
  - `cml_prevalent_assumptions.csv`
  - `trade_inventory_futurehooks.csv`
  - `resolved_phase2_config_snapshot.json`
  - `assumptions_import_summary.json`
  - `generated_phase2_parameters.toml`
  - `generated_phase2_scenario.toml`
- Current wiring into the active Phase 2 engine:
  - `Scenario_Controls.dose_basis_default -> model.dose_basis`
  - `Dosing_Assumptions` module rows -> `module_settings.<module>` fixed dose, weight-based dose, average weight, and doses-per-patient-per-month
  - `Product_Parameters` scenario default plus module override rows -> module `fg_mg_per_unit`
  - `Packaging_and_Vialing` module rows -> module `fg_vialing_rule`
  - `Yield_Assumptions` scenario-default row -> `yield.plan.*` and `ds.overage_factor`
  - `Product_Parameters` scenario-default row -> `ds.qty_per_dp_unit_mg`
  - `SS_Assumptions` scenario-default row -> `ss.ratio_to_fg` and `model.co_pack_mode`
- Preserved as future-ready only in this task:
  - `Launch_Timing`
  - `CML_Prevalent_Assumptions`
  - `Trade_Inventory_FutureHooks`
  - `Product_Parameters` module overrides for `ds_qty_per_dp_unit_mg`
  - `Yield_Assumptions` module overrides for `ds_overage_factor`
  - `dp_concentration_mg_per_ml`
  - `dp_fill_volume_ml`

## One-Command Workflow
- Run the end-to-end import plus deterministic cascade from the repo root with:
  - `python scripts/run_forecast_workflow.py --workbook "data/raw/CBX250_Commercial_Forecast_REAL.xlsx" --scenario-name "REAL_2029"`
- If `--scenario-name` is omitted, the wrapper derives a safe default from the workbook filename.
- Optional arguments:
  - `--phase2-scenario config/scenarios/base_phase2.toml`
  - `--output-dir data/curated/real_2029`
  - `--overwrite`
- The wrapper does not duplicate business logic. It:
  - imports the workbook with the existing importer
  - verifies that authoritative `monthlyized_output.csv` was generated
  - creates a generated Phase 2 scenario pointing to that CSV
  - runs the existing deterministic Phase 2 cascade
  - writes the final deterministic cascade CSV
- Expected outputs from the wrapper are written to the selected output directory:
  - `monthlyized_output.csv`
  - `phase2_deterministic_cascade.csv`
  - `generated_phase2_scenario.toml`
  - the standard normalized Phase 1 CSV package and `workbook_import_summary.json`
- The terminal summary reports:
  - `scenario_name`
  - `forecast_grain`
  - `forecast_frequency`
  - `geography_count`
  - `output_row_count`
  - `total_patients_treated`
  - `total_fg_units_required`
  - `total_ss_units_required`
  - `total_dp_units_required`
  - `total_ds_required` (backward-compatible mg total)
  - `total_ds_required_mg`
  - `total_ds_required_g`
  - `total_ds_required_kg`
  - `validation_issue_count`
  - authoritative Phase 1 and Phase 2 output paths

## Phase 2 Deterministic Cascade
- Phase 2 consumes the accepted Phase 1 normalized contract only: `data/curated/<scenario_name>/monthlyized_output.csv`.
- Phase 2 does not read raw commercial forecast files or workbook entry tabs directly.
- The dose basis is config-driven through `dose_basis = "fixed" | "weight_based"` with module-specific settings under `[module_settings.<module>]`.
- The approved Phase 2 base-case assumptions in `config/parameters/phase2_deterministic_cascade.toml` are:
  - fixed dose = `0.15 mg`
  - weight-based dose = `0.0023 mg/kg`
  - deterministic average patient weight = `80 kg`
  - AML and MDS dosing cadence = `4.33 doses/month` (`QW`)
  - `CML_Incident` and `CML_Prevalent` dosing cadence = `1.00 dose/month` (`Q4W`)
  - `fg_mg_per_unit = 1.0 mg`
  - `fg_vialing_rule = "ceil_mg_per_unit_no_sharing"` which implements dose-level vialing: `ceil(mg_per_dose_after_reduction / fg_mg_per_unit) * doses_required`
- `ds.qty_per_dp_unit_mg = 1.0`
- `yield.plan.ds_to_dp = 0.90`
- `ds.overage_factor = 0.05`
- Under the current approved Phase 2 base case, DS is calculated and exported on an mg basis using: `ds_required_mg = dp_units_required * ds.qty_per_dp_unit_mg / yield.plan.ds_to_dp * (1 + ds.overage_factor)`. The cascade CSV now includes `ds_required_mg`, `ds_required_g`, and `ds_required_kg`; retained `ds_required` is the backward-compatible mg field.
- Step-up configuration is wired but inactive by default. Enabling it currently raises a clearly labeled `PLACEHOLDER` error instead of silently inventing logic.
- Dose reductions are applied to mg first, then FG/SS/DP/DS are recalculated from the reduced patient-dose vial requirement.
- `CML_Incident` and `CML_Prevalent` remain separate modules through the full cascade.
- SS demand is derived in parallel from FG vial demand through `ss.ratio_to_fg`.
- Active deterministic planning yields are `yield.plan.ds_to_dp` and `yield.plan.dp_to_fg`.
- The current deterministic DS conversion uses:
  - `dp_units_required`
  - `ds.qty_per_dp_unit_mg`
  - `yield.plan.ds_to_dp`
  - `ds.overage_factor`
- `yield.plan.ss` remains in config as a preserved future hook and is not currently applied to the Phase 2 SS parallel demand calculation.
- `yield.plan.fg_pack` remains fixed at `1.0` as a preserved future hook and is not used to override the approved vial round-up rule in the current base case.

## Phase 2 Outputs
- `config/scenarios/base_phase2.toml` is the sample Phase 2 scenario.
- `config/parameters/phase2_deterministic_cascade.toml` contains the Phase 2 config-driven business parameters.
- The authoritative Phase 2 output table is the deterministic cascade CSV written by the scenario config under `[outputs].deterministic_cascade`.
- The sample output path is `data/outputs/base_phase2_deterministic_cascade.csv` when you run `scripts/run_phase2.py`.
- The one-command workflow writes its authoritative Phase 2 output to `<output_dir>/phase2_deterministic_cascade.csv` and keeps authoritative Phase 1 input at `<output_dir>/monthlyized_output.csv`.
- Output grain remains `scenario x geography x module x segment x month`.
- DS columns in the Phase 2 CSV are:
  - `ds_required` = backward-compatible mg field
  - `ds_required_mg` = explicit mg value
  - `ds_required_g` = `ds_required_mg / 1000`
  - `ds_required_kg` = `ds_required_mg / 1000000`

## Acceptance Tests
- Run the business acceptance layer with `python -m pytest tests/test_phase1_acceptance.py`.
- Run the Phase 2 business acceptance layer with `python -m pytest tests/test_phase2_runner.py tests/test_phase2_acceptance.py`.
- The acceptance suite validates workbook import reconciliation for all `forecast_grain x forecast_frequency` combinations, authoritative `monthlyized_output.csv` generation, runner consistency against normalized monthly outputs, calendar horizon coverage, output-key uniqueness, actionable validation context, and the current Phase 1 CML prevalent guardrails.
- The Phase 2 acceptance suite validates the deterministic cascade from accepted `monthlyized_output.csv` into doses, FG, SS, DP, and DS without bypassing the Phase 1 normalized contract, including the approved base-case `0.15 mg` fixed dose, `0.0023 mg/kg x 80 kg` weight-based default, module-specific monthly dosing cadence, `1.0 mg` FG units, and ceiling vialing.

## CML Prevalent Phase 1 Limitation
- `CML_Prevalent` remains a separate module.
- Explicit `CML_Prevalent` forecast rows remain primary when present; assumptions are still loaded for validation and audit.
- Fallback generation from workbook assumptions is supported when explicit active-sheet `CML_Prevalent` rows are absent.
- `exhaustion_rule` is captured and audited, but Phase 1 does not implement a full dynamic depletion or remainder engine.
- Current Phase 1 behavior relies only on supplied annual totals, `launch_month_index`, `duration_months`, and profile logic. Fuller depletion mechanics are deferred.

## Deferred Beyond Phase 2
- trade
- production scheduling
- inventory
- financials
- stochastic manufacturing performance yields
- Monte Carlo
- full co-pack logic beyond the preserved `separate_sku_first` hook
- dynamic CML prevalent depletion and remainder logic

## Real Scenario 01
- Raw source workbook: `data/raw/treatable pts 250.xlsx`
- Build curated inputs: `python scripts/build_real_scenario_01.py`
- Run the real scenario: `python scripts/run_phase1.py --scenario config/scenarios/real_scenario_01.toml`
- Source-to-target mapping: `docs/model_contract/real_scenario_01_source_to_target_mapping.md`
- Reconciliation output is written to `data/curated/real_scenario_01/reconciliation_summary.json`
