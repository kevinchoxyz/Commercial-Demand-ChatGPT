# CBX-250 Commercial Demand & Supply Planning Model - Phase 0 Model Contract

Status: `working repo extract aligned to cbx_250_model_contract_phase_0_v_1.md`

## Purpose
This document is the repo-local working extract for the approved Phase 0 contract. It captures the sections needed to scaffold the first Codex implementation pass without pulling later-phase logic into code prematurely.

## 1. Locked Architecture Decisions Relevant To This Scaffold

### 1.1 Core Model Architecture
- Separate demand modules by indication and geography:
  - AML
  - MDS
  - CML Incident
  - CML Prevalent
- Commercial / brand `Patients Treated` forecast is the primary demand driver.
- Epidemiology is a validation and fallback layer, not the main driver when commercial forecast exists.

### 1.2 Time Structure
- Model horizon: 20 years from initial US AML/MDS launch.
- Engine calculation grain: monthly across the full horizon.
- Reporting rollups are downstream presentation logic and do not change the core monthly engine grain.

### 1.3 Year 1 Anchor
- Base parameter: `us_aml_mds_initial_approval_date`
- Default approval year: `2029`
- Store as a full date, not year only.

### 1.4 Segment Structure
AML segment mix by geography and time:
- `1L_fit`
- `1L_unfit`
- `RR`

MDS segment mix by geography and time:
- `HR_MDS`
- `LR_MDS`

CML v1 rule:
- no AML/MDS-style segment mix table
- use separate modules `CML_Incident` and `CML_Prevalent`
- keep geography split in both modules

## 2. Model Section Build Order
1. Calendar and launch engine
2. Demand input and epi cross-check
3. Indication modules: AML, MDS, CML Incident, CML Prevalent
4. Dose and unit cascade
5. Trade layer
6. Production scheduling
7. Inventory and shelf-life tracking
8. Financials
9. Scenarios and Monte Carlo
10. Board and executive outputs

Current repo implementation boundary:
- build steps 1 to 5
- keep later steps as placeholders

## 3. Phase 1-Relevant Parameter Map

### 3.1 Calendar And Launch
- `us_aml_mds_initial_approval_date`
- `forecast_horizon_months`
- `planning_cycle_frequency`

### 3.2 Geography And Market
- `geography_code`
- `market_group`
- `currency_code`
- `launch_sequence_rank`

### 3.3 Indication And Segment
- `indication_code`
- `segment_code`
- `segment_mix_share`
- `segment_mix_effective_start`
- `segment_mix_effective_end`

### 3.4 Demand Input
- `patients_treated_input`
- `patients_treated_source`
- `forecast_grain`
- `epi_treatable_patients`
- `epi_crosscheck_tolerance_pct`

### 3.5 CML Prevalent
- `cml_prevalent_total_pool`
- `cml_prevalent_addressable_pool`
- `cml_prevalent_distribution_curve`
- `cml_prevalent_launch_month`
- `cml_prevalent_exhaustion_rule`

### 3.6 Trade Layer
- `sublayer1_target_weeks_on_hand`
- `sublayer2_target_weeks_on_hand`
- `sublayer2_wastage_rate`
- `initial_stocking_units_per_new_site`
- `ss_units_per_new_site`
- `sublayer1_launch_fill_months_of_demand`
- `site_activation_rate`
- `certified_sites_at_launch`
- `certified_sites_at_peak`
- `rems_certification_lag_weeks`
- `january_softening_factor`
- `bullwhip_flag_threshold`
- `channel_fill_start_prelaunch_weeks`
- `sublayer2_fill_distribution_weeks`

## 4. Phase 1 Core Data Model

### 4.1 Dimensions
- `dim_time`
- `dim_geography`
- `dim_indication`
- `dim_segment`
- `dim_scenario`

### 4.2 Input Tables
- `commercial_forecast_module_level`
- `commercial_forecast_segment_level`
- `inp_epi_crosscheck`
- `aml_segment_mix`
- `mds_segment_mix`
- `inp_cml_prevalent`

### 4.3 Phase 1 Output Contract
- monthly patient-treated output at `scenario x geography x module x segment x month`
- business-facing cohort fields:
  - `patient_starts`
  - `patients_continuing`
  - `patients_rolloff`
  - `patients_active`
- backward-compatible alias retained during transition:
  - `patients_treated_monthly = patients_active`

### 4.4 Phase 2 Output Contract
- deterministic dose and unit cascade output at `scenario x geography x module x segment x month`

### 4.5 Phase 3 Output Contract
- deterministic trade-layer FG output at `scenario x geography x module x segment x month`
- required time series:
  - patient FG demand
  - Sub-Layer 2 pull
  - ex-factory FG demand

## 5. Validation Rules

### Hard Checks
- AML segment shares sum to 100% by geography and time.
- MDS segment shares sum to 100% by geography and time.
- CML prevalent treated pool cannot exceed addressable prevalent pool.
- Calendar engine must produce a full 240-month timeline.
- Geography and indication output keys must remain unique and deterministic.

### Warning-Level Placeholder
- epi cross-check variance exceeds tolerance

## 6. Immediate Build Scope
- repository scaffold
- config schemas
- dimension tables
- input loader framework
- calendar engine
- demand-module skeletons for AML, MDS, CML Incident, CML Prevalent
- deterministic dose and unit cascade consuming normalized monthly patient outputs
- deterministic trade layer consuming accepted Phase 2 FG outputs
- validation framework
- unit tests
- example configs and sample input files

## 7. Explicitly Out Of Scope For The First Pass
- production scheduling
- inventory balance logic
- financials
- Monte Carlo / stochastic simulation
- downstream reporting rollups

## Notes
- Preserve `PLACEHOLDER` labels where the approved contract still leaves unresolved details.
- The full source document remains the governing artifact if this repo extract lags it.
