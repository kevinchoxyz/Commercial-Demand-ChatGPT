# AGENTS.md - CBX-250 Commercial Demand & Supply Planning Model

## Project Purpose
This repository contains the CBX-250 commercial demand and supply planning model scaffold.

The model is specification-driven. Business logic must follow the approved model contract and configuration files. Do not invent assumptions that are not explicitly documented.

## Source Of Truth
Use this precedence order for implementation decisions:
1. `docs/model_contract/model_contract_phase0.md`
2. approved config files under `config/`
3. approved reference input files under `data/reference/`
4. tests and validation rules already in the repository
5. temporary development defaults marked clearly as `PLACEHOLDER`

## Current Implemented Scope
Build and maintain the accepted deterministic Phase 1, Phase 2, and Phase 3 baseline only.

Included:
- repository scaffold
- config schemas
- dimension tables
- input loader framework
- calendar engine
- deterministic demand modules for AML, MDS, CML Incident, CML Prevalent
- validation framework
- monthly patient-treated output tables
- deterministic dose and unit cascade consuming Phase 1 normalized monthly outputs
- deterministic outputs for doses, FG, SS, DP, and DS
- deterministic trade / channel-fill layer consuming accepted Phase 2 outputs
- deterministic outputs for patient FG demand, Sub-Layer 2 pull, and ex-factory FG demand
- unit tests
- example configs and sample input files

Out of scope:
- production scheduler
- inventory engine
- financial engine
- Monte Carlo runner
- dashboarding / BI layer

## Core Architecture Rules
- Commercial / brand `Patients Treated` forecast is the primary demand driver.
- The authoritative Phase 1 upstream contract for Phase 2 is `monthlyized_output.csv`.
- The authoritative Phase 2 upstream contract for Phase 3 is `phase2_deterministic_cascade.csv`.
- Phase 3 must remain upstream-contract driven and agnostic to whether treated demand was derived from direct treated-census input or from a later cohort / starts-duration build.
- Epidemiology is a validation / fallback layer only.
- Engine grain must remain monthly across the full 240-month horizon.
- Reporting rollups are downstream presentation logic, not a core calculation-grain change.
- AML uses segment mix buckets `1L_fit`, `1L_unfit`, `RR`.
- MDS uses segment mix buckets `HR_MDS`, `LR_MDS`.
- CML must be modeled as separate modules `CML_Incident` and `CML_Prevalent`.
- Do not create an AML/MDS-style internal segment-mix table for CML in v1.
- Year 1 anchor is `us_aml_mds_initial_approval_date`.
- Default approval year is 2029 but the value must remain editable.
- Base operating mode remains `separate_sku_first`; do not build co-pack logic beyond a future hook.
- Solution Stabilizer demand must remain a config-driven parallel requirement to FG demand.

## Coding Rules
- Read `docs/model_contract/*` before changing model logic.
- Use config-driven design.
- Keep assumptions out of code whenever possible.
- Do not hard-code stochastic variables or manufacturing performance logic.
- Prefer small, testable modules.
- Use canonical parameter names consistently.
- Add concise comments only where the logic is not obvious.
- Label unresolved behavior as `PLACEHOLDER`.

## Validation Rules Required In Phase 1
- AML segment shares must sum to 100% by geography and time period.
- MDS segment shares must sum to 100% by geography and time period.
- Required input files and required columns must be validated explicitly.
- CML prevalent treated patients must not exceed addressable prevalent pool.
- Calendar engine must produce a full 240-month timeline.
- Geography and indication output keys must remain unique and deterministic.

## Files To Read First
1. `docs/model_contract/model_contract_phase0.md`
2. `docs/model_contract/variable_registry_v1.md`
3. `docs/model_contract/correlation_matrix_scaffold_v1.md`
4. `README.md`
5. relevant files under `config/`

## When Uncertain
- Do not invent a business rule silently.
- Create a clearly labeled placeholder.
- Note the gap in the relevant config or issue log.
- Keep the implementation usable without pretending the assumption is final.
