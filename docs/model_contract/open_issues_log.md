# Open Issues Log

Status: `working extract from approved contract`

## Still Open But Not Blocking
| issue_id | title | status | blocking_phase | owner | notes |
| --- | --- | --- | --- | --- | --- |
| ISSUE-001 | Exact month and day of initial 2029 AML/MDS approval | open | Phase 1 | TBD | Sample config uses `2029-01-01` as a clearly labeled placeholder. |
| ISSUE-002 | Default base-case dose basis selection at startup | open | Later Phase | TBD | Not needed for the current deterministic patient-treated scaffold. |
| ISSUE-003 | Step-up schedule details if activated later | open | Later Phase | TBD | Out of scope for the current pass. |
| ISSUE-004 | Numeric AML mix assumptions by geography and time | open | Phase 1 | TBD | Segment mix templates remain empty until approved values are loaded. |
| ISSUE-005 | Numeric MDS HR/LR mix assumptions by geography and time | open | Phase 1 | TBD | Segment mix templates remain empty until approved values are loaded. |
| ISSUE-006 | Whether EU needs a REMS-equivalent activation construct | open | Phase 3 | TBD | Phase 3 now supports geography-specific launch trade settings, but the exact EU certification / site-activation construct remains to be business-approved. |
| ISSUE-007 | Exact safety stock relationship between FG and SS in co-pack mode | open | Later Phase | TBD | SS logic is deferred. |
| ISSUE-008 | Workbook `Monthlyized_Output` tab is reference-only in Phase 1 | open | Phase 1 | TBD | `monthlyized_output.csv` is the authoritative normalized workbook export in Phase 1. Writing generated rows back into the workbook tab is deferred unless a low-risk exporter is added later. |
| ISSUE-009 | Full CML prevalent dynamic depletion / remainder logic | open | Later Phase | TBD | Phase 1 captures and audits `exhaustion_rule`, but current behavior only uses supplied annual totals, `launch_month_index`, `duration_months`, and profile logic. Fuller depletion mechanics are deferred. |
| ISSUE-010 | Deterministic monthly dosing cadence placeholder replaced by approved Phase 2 base case | resolved | Phase 2 | business-approved | Phase 2 now uses module-specific config fields `module_settings.<module>.doses_per_patient_per_month` with approved base-case values: AML/MDS = `4.33`, `CML_Incident` = `1.00`, `CML_Prevalent` = `1.00`. |
| ISSUE-011 | FG mg-per-unit / vialing placeholder replaced by approved Phase 2 base case | resolved | Phase 2 | business-approved | Phase 2 now uses `module_settings.<module>.fg_mg_per_unit = 1.0` and `fg_vialing_rule = "ceil_mg_per_unit_no_sharing"` so FG demand is calculated at the patient-dose level as `ceil(mg_per_dose_after_reduction / 1.0) * doses_required` with single-patient use and no vial sharing. |
| ISSUE-012 | Geography- or segment-specific patient weight refinement beyond the 80 kg base case | open | Later Phase | TBD | Phase 2 now uses approved module-specific base-case values `weight_based_dose_mg_per_kg = 0.0023` and `average_patient_weight_kg = 80.0`. More granular geography- or segment-specific weight logic remains deferred. |
| ISSUE-013 | Exact approved base-case trade target weeks on hand and site-activation counts | open | Phase 3 | TBD | `config/parameters/phase3_trade_layer.toml` uses clearly labeled placeholder sample values for target weeks on hand midpoints, site activation rates, certified sites at launch, certified sites at peak, launch-fill months of demand, and softening defaults until business-approved values are loaded. |
| ISSUE-014 | Assumptions workbook trade hooks are normalized but not yet wired into active Phase 3 config generation | open | Later Phase | TBD | `Trade_Inventory_FutureHooks` remains preserved in normalized artifacts, but the current deterministic Phase 3 runner still reads trade parameters from `phase3_trade_layer.toml`. |
| ISSUE-015 | Segment-level allocation of shared trade adjustments | open | Phase 3 | TBD | Phase 3 computes trade math at module/geography/month and allocates shared adjustments back to segment rows using the nearest available patient FG demand shares to preserve stable output grain. More granular segment-specific trade data can replace this rule later if approved. |
| ISSUE-016 | Treatment-duration range handling beyond deterministic base-case months | open | Later Phase | TBD | Phase 1 now supports `patient_starts` cohort roll-forward with deterministic `treatment_duration_months`, but richer duration distributions or range-based stop logic remain deferred. |
