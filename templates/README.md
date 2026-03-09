# Commercial Forecast Workbook

Use [CBX250_Commercial_Forecast_Template.xlsx](/c:/Users/KevinCho/CBX250%20Commercial%20ChatGPT/templates/CBX250_Commercial_Forecast_Template.xlsx) as the business submission template for Phase 1 Commercial patient forecasts.

Seeded base-case defaults:
- `forecast_frequency = annual`
- `demand_basis = patient_starts`

Preferred/default operating mode:
- use annual patient starts
- provide treatment duration assumptions through the assumptions workbook workflow

Backward-compatible mode:
- use `treated_census` only when Commercial is already providing treated patients directly
- do not apply treatment duration again in `treated_census` mode

To normalize a completed workbook into the model CSV contracts, run:
`python scripts/import_commercial_forecast_workbook.py --workbook templates/CBX250_Commercial_Forecast_Template.xlsx`

Use monthly entry when Commercial already has month-level patients treated:
- `module_level`: fill `Inputs`, `Geography_Master`, `ModuleLevel_Forecast`, `AML_Mix`, and `MDS_Mix`.
- `segment_level`: fill `Inputs`, `Geography_Master`, and `SegmentLevel_Forecast`.

Use annual entry when Commercial only has year buckets:
- `module_level`: fill `Inputs`, `Geography_Master`, `Annual_ModuleLevel_Forecast`, `AML_Mix`, `MDS_Mix`, and `Annual_to_Monthly_Profiles`.
- `segment_level`: fill `Inputs`, `Geography_Master`, `Annual_SegmentLevel_Forecast`, and `Annual_to_Monthly_Profiles`.

`Annual_to_Monthly_Profiles` controls annual-to-monthly conversion. The starter profiles are editable defaults, not fixed business assumptions. The importer renormalizes Year 1 if launch starts after January, so pre-launch months go to zero and the remaining in-year weights sum to 100%.

For `AML_Mix` and `MDS_Mix`:
- standard annual `module_level` entry can be one row per `scenario_name x geography_code x year_index`
- optional monthly overrides can be entered with `month_index`
- when both exist, `month_index` rows take precedence for that geography/month
- if only year-level rows exist, the importer expands them uniformly across the monthlyized months of that year
- users do not need to populate every month unless within-year mix differs

Always keep CML as separate modules `CML_Incident` and `CML_Prevalent`. On `SegmentLevel_Forecast` and `Annual_SegmentLevel_Forecast`, use `segment_code = ALL` for CML submissions.

`CML_Prevalent_Assumptions` is optional for explicit demand import:
- If explicit `CML_Prevalent` forecast rows exist in the active forecast sheet, those remain the primary demand input.
- If usable assumptions are also provided, they generate `inp_cml_prevalent.csv` for addressable pool validation.
- If explicit `CML_Prevalent` forecast rows exist and usable assumptions are missing, the importer continues with a warning and writes a header-only `inp_cml_prevalent.csv`.
- If explicit `CML_Prevalent` forecast rows are missing, the importer can generate fallback monthly demand from `fallback_patients_treated_annual` plus the selected profile.
- `exhaustion_rule` is captured and audited, but Phase 1 does not implement a full dynamic depletion or remainder engine. Current behavior relies only on supplied annual totals, `launch_month_index`, `duration_months`, and profile logic.

Importer behavior:
- Writes normalized CSVs under `data/curated/<scenario_name>/` by default.
- Converts `ALL` on CML segment rows into the model's internal segment codes.
- Monthlyizes annual forecast rows into the normalized monthly Phase 1 contracts.
- Writes `monthlyized_output.csv` as the authoritative normalized monthly workbook export for Phase 1.
- `monthlyized_output.csv` now separates:
  - `patient_starts`
  - `patients_continuing`
  - `patients_rolloff`
  - `patients_active`
- `patients_treated_monthly` is retained temporarily as the backward-compatible alias of `patients_active`.
- Leaves the workbook `Monthlyized_Output` tab as a generated/reference placeholder unless future exporter logic explicitly writes rows back into the workbook.
- Creates a header-only placeholder for `inp_epi_crosscheck.csv` because epi cross-check inputs are not collected in the workbook.

User-entry sheets:
- `Inputs`
- `Geography_Master`
- `ModuleLevel_Forecast`
- `SegmentLevel_Forecast`
- `Annual_ModuleLevel_Forecast`
- `Annual_SegmentLevel_Forecast`
- `AML_Mix`
- `MDS_Mix`
- `Annual_to_Monthly_Profiles`
- `CML_Prevalent_Assumptions`

Generated / audit sheet:
- `Monthlyized_Output`

Authoritative Phase 1 workbook-import outputs:
- `monthlyized_output.csv` is the authoritative normalized monthly workbook export.
- `commercial_forecast_module_level.csv` or `commercial_forecast_segment_level.csv` remain the lower-level normalized contract inputs used by the current Phase 1 runner, depending on `forecast_grain`.
- `inp_cml_prevalent.csv` is the CML prevalent validation-pool file generated from workbook assumptions when usable assumptions are provided; otherwise it is written as header-only.

Separate from the Commercial forecast workbook, the repo now also includes a business-facing assumptions workbook:
- `templates/CBX250_Model_Assumptions_Template.xlsx`
- generate it with `python scripts/generate_model_assumptions_template.py`
- import it with `python scripts/assumptions_import.py --workbook templates/CBX250_Model_Assumptions_Template.xlsx`
- the assumptions importer writes normalized assumption artifacts plus generated Phase 2 and Phase 3 config files under `data/outputs/<scenario_name>/assumptions/`
- `Trade_Inventory_FutureHooks` is now the business-facing sheet for the active deterministic Phase 3 trade config:
  - `scenario_default` row -> global `trade.*`
  - `geography_default` rows -> `geography_defaults.<geography>`
  - `launch_event` rows -> `launch_events.<module>.<geography>.launch_month_index`
