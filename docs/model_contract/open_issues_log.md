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
| ISSUE-006 | Whether EU needs a REMS-equivalent activation construct | open | Later Phase | TBD | Trade layer is out of scope for this pass. |
| ISSUE-007 | Exact safety stock relationship between FG and SS in co-pack mode | open | Later Phase | TBD | SS logic is deferred. |
| ISSUE-008 | Workbook `Monthlyized_Output` tab is reference-only in Phase 1 | open | Phase 1 | TBD | `monthlyized_output.csv` is the authoritative normalized workbook export in Phase 1. Writing generated rows back into the workbook tab is deferred unless a low-risk exporter is added later. |
| ISSUE-009 | Full CML prevalent dynamic depletion / remainder logic | open | Later Phase | TBD | Phase 1 captures and audits `exhaustion_rule`, but current behavior only uses supplied annual totals, `launch_month_index`, `duration_months`, and profile logic. Fuller depletion mechanics are deferred. |
