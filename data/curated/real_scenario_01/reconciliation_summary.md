# Real Scenario 01 Reconciliation Summary

- scenario_name: `REAL_SCENARIO_01`
- forecast_grain: `module_level`
- geography_count: `2`
- module_count: `4`
- source_forecast_total: `449100.000000`
- normalized_output_total: `449100.000000`
- difference_vs_source: `0.000000`
- validation_issue_count: `0`

## Row Counts By Module
- AML: `864`
- CML_Incident: `288`
- CML_Prevalent: `288`
- MDS: `576`

## Row Counts By Segment
- 1L_fit: `288`
- 1L_unfit: `288`
- CML_Incident: `288`
- CML_Prevalent: `288`
- HR_MDS: `288`
- LR_MDS: `288`
- RR: `288`

## Unresolved Data Gaps
- AML segment mix source is missing in data/raw and is populated with clearly labeled equal-share placeholders.
- MDS segment mix source is missing in data/raw and is populated with clearly labeled equal-share placeholders.
- Raw workbook is annual; monthly normalized forecast uses an even 12-month split placeholder transformation.
- The raw workbook does not provide an explicit full-date Year 1 anchor, so the scenario parameter keeps the Phase 1 placeholder approval date.