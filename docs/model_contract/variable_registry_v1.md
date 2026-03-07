# Variable Registry v1

Status: `Phase 1-aligned starter scaffold`

This file follows the approved starter schema and keeps the first pass focused on deterministic demand inputs plus explicit placeholders for later phases.

## Required Columns
- `variable_name`
- `mapped_parameter_name`
- `distribution_type`
- `param_1`
- `param_2`
- `param_3`
- `minimum_value`
- `maximum_value`
- `draw_frequency`
- `correlation_group`
- `active_flag`
- `business_owner`
- `notes`

## Phase 1 Starter Rows
| variable_name | mapped_parameter_name | distribution_type | param_1 | param_2 | param_3 | minimum_value | maximum_value | draw_frequency | correlation_group | active_flag | business_owner | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| demand.aml.us.uptake | patients_treated_input | PERT | TBD | TBD | TBD | TBD | TBD | once_per_iteration | demand_aml | yes | TBD | Approved starter row from the working contract. |
| demand.aml.eu.uptake | patients_treated_input | PERT | TBD | TBD | TBD | TBD | TBD | once_per_iteration | demand_aml | yes | TBD | Approved starter row from the working contract. |
| demand.mds.us.uptake | patients_treated_input | PERT | TBD | TBD | TBD | TBD | TBD | once_per_iteration | demand_mds | yes | TBD | Approved starter row from the working contract. |
| demand.mds.eu.uptake | patients_treated_input | PERT | TBD | TBD | TBD | TBD | TBD | once_per_iteration | demand_mds | yes | TBD | Approved starter row from the working contract. |
| demand.cml_incident.us.uptake | patients_treated_input | PERT | TBD | TBD | TBD | TBD | TBD | once_per_iteration | demand_cml_incident | yes | TBD | Approved starter row from the working contract. |
| demand.cml_incident.eu.uptake | patients_treated_input | PERT | TBD | TBD | TBD | TBD | TBD | once_per_iteration | demand_cml_incident | yes | TBD | Approved starter row from the working contract. |
| demand.cml_prevalent.us.curve | cml_prevalent_distribution_curve | PERT | TBD | TBD | TBD | TBD | TBD | once_per_iteration | demand_cml_prevalent | yes | TBD | Approved starter row from the working contract. |
| demand.cml_prevalent.eu.curve | cml_prevalent_distribution_curve | PERT | TBD | TBD | TBD | TBD | TBD | once_per_iteration | demand_cml_prevalent | yes | TBD | Approved starter row from the working contract. |

## Rules
- Any variable missing `draw_frequency` is invalid.
- Any variable added later must also be added to the correlation matrix.
- Undefined correlations default to zero.
- Keep later-phase manufacturing, trade, and financial variables out of Phase 1 code even if they appear in the broader contract.
