# Data Dictionary Skeleton

Status: `Phase 1 working skeleton`

## Minimum Fields For Every Parameter
- `canonical_name`
- `plain_english_definition`
- `units`
- `allowed_range`
- `default_value`
- `source_of_truth`
- `editable_flag`
- `scenario_override_flag`
- `stochastic_eligible_flag`
- `owning_section`
- `notes`

## Source-Of-Truth Precedence
1. approved model contract
2. authoritative workbook inputs
3. approved parameter overrides
4. temporary development defaults

## Input File Skeleton
| file_name | column_name | data_type | required | example | description | status |
| --- | --- | --- | --- | --- | --- | --- |
| commercial_forecast_module_level_template.csv | geography_code | string | yes | US | Geography code. | scaffold |
| commercial_forecast_module_level_template.csv | module | string | yes | AML | Demand module. | scaffold |
| commercial_forecast_module_level_template.csv | month_index | integer | yes | 1 | Monthly index within 240-month horizon. | scaffold |
| commercial_forecast_module_level_template.csv | patients_treated | float | yes | 100.0 | Module-level treated patient count. | scaffold |
| commercial_forecast_segment_level_template.csv | geography_code | string | yes | US | Geography code. | scaffold |
| commercial_forecast_segment_level_template.csv | module | string | yes | AML | Demand module. | scaffold |
| commercial_forecast_segment_level_template.csv | segment_code | string | yes | 1L_fit | Approved segment code for the module. | scaffold |
| commercial_forecast_segment_level_template.csv | month_index | integer | yes | 1 | Monthly index within 240-month horizon. | scaffold |
| commercial_forecast_segment_level_template.csv | patients_treated | float | yes | 40.0 | Segment-level treated patient count. | scaffold |
| inp_epi_crosscheck_template.csv | treatable_patients | float | yes | 120.0 | Optional epi cross-check value. | scaffold |
| aml_segment_mix_template.csv | segment_share | float | yes | 0.40 | AML share by geography and month index. | scaffold |
| mds_segment_mix_template.csv | segment_share | float | yes | 0.55 | MDS share by geography and month index. | scaffold |
| inp_cml_prevalent_template.csv | addressable_prevalent_pool | float | yes | 1000.0 | Addressable prevalent pool cap for validation. | scaffold |

## Output File Skeleton
| output_name | column_name | data_type | required | description | status |
| --- | --- | --- | --- | --- | --- |
| phase1_demand_output | scenario_name | string | yes | Scenario identifier. | scaffold |
| phase1_demand_output | geography | string | yes | Geography code. | scaffold |
| phase1_demand_output | module | string | yes | Demand module. | scaffold |
| phase1_demand_output | segment | string | yes | Segment label. | scaffold |
| phase1_demand_output | month_start | date | yes | Monthly bucket start date. | scaffold |
| phase1_demand_output | patients_treated | float | yes | Deterministic Phase 1 treated-patient value. | scaffold |
