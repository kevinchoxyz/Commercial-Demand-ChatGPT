# CBX-250 Commercial Planning Model

This repository contains the Phase 1 scaffold for a spec-driven CBX-250 commercial demand and supply planning model. The current implementation is intentionally limited to the deterministic demand foundation and leaves later-phase capabilities as explicit placeholders.

## Read These Files First
- `docs/model_contract/model_contract_phase0.md`
- `docs/model_contract/variable_registry_v1.md`
- `docs/model_contract/correlation_matrix_scaffold_v1.md`
- `docs/model_contract/data_dictionary_skeleton.md`
- `AGENTS.md`

## Current Scope
- Phase 1 deterministic demand foundation only
- Monthly 240-month horizon
- Config-driven setup
- Demand logic for AML, MDS, CML Incident, and CML Prevalent
- Configurable commercial forecast grain:
  - `module_level`
  - `segment_level`
- Validation scaffolds for AML/MDS segment shares and CML prevalent pool limits

## Quick Start
- `python -m pytest`
- `python -m pytest tests/test_phase1_acceptance.py`
- `python scripts/run_phase1.py --scenario config/scenarios/base_phase1.toml`
- `python scripts/build_real_scenario_01.py`
- `python scripts/run_phase1.py --scenario config/scenarios/real_scenario_01.toml`
- `python scripts/generate_commercial_forecast_template.py`
- `python scripts/import_commercial_forecast_workbook.py --workbook templates/CBX250_Commercial_Forecast_Template.xlsx`

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
  - how `CML_Prevalent_Assumptions` feeds `inp_cml_prevalent.csv` and fallback demand
  - which sheets are user-entry vs generated

## Workbook Import Outputs
- `data/curated/<scenario_name>/monthlyized_output.csv` is the authoritative normalized monthly workbook export for Phase 1.
- `data/curated/<scenario_name>/commercial_forecast_module_level.csv` or `data/curated/<scenario_name>/commercial_forecast_segment_level.csv` remain the lower-level normalized contract inputs consumed by the current Phase 1 runner, depending on `forecast_grain`.
- `data/curated/<scenario_name>/inp_cml_prevalent.csv` is the authoritative CML prevalent validation-pool file generated from workbook assumptions.
- The workbook tab `Monthlyized_Output` is a generated/reference placeholder in Phase 1 unless future exporter logic explicitly writes rows back into the workbook.

## Acceptance Tests
- Run the business acceptance layer with `python -m pytest tests/test_phase1_acceptance.py`.
- The acceptance suite validates workbook import reconciliation for all `forecast_grain x forecast_frequency` combinations, authoritative `monthlyized_output.csv` generation, runner consistency against normalized monthly outputs, calendar horizon coverage, output-key uniqueness, actionable validation context, and the current Phase 1 CML prevalent guardrails.

## CML Prevalent Phase 1 Limitation
- `CML_Prevalent` remains a separate module.
- Explicit `CML_Prevalent` forecast rows remain primary when present; assumptions are still loaded for validation and audit.
- Fallback generation from workbook assumptions is supported when explicit active-sheet `CML_Prevalent` rows are absent.
- `exhaustion_rule` is captured and audited, but Phase 1 does not implement a full dynamic depletion or remainder engine.
- Current Phase 1 behavior relies only on supplied annual totals, `launch_month_index`, `duration_months`, and profile logic. Fuller depletion mechanics are deferred.

## Real Scenario 01
- Raw source workbook: `data/raw/treatable pts 250.xlsx`
- Build curated inputs: `python scripts/build_real_scenario_01.py`
- Run the real scenario: `python scripts/run_phase1.py --scenario config/scenarios/real_scenario_01.toml`
- Source-to-target mapping: `docs/model_contract/real_scenario_01_source_to_target_mapping.md`
- Reconciliation output is written to `data/curated/real_scenario_01/reconciliation_summary.json`
