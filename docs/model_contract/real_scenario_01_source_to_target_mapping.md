# Real Scenario 01 Source-To-Target Mapping

Status: `Phase 1 working mapping for one real-data scenario`

## Source Inventory
- Raw source folder inspected: `data/raw/`
- Actual source files found:
  - `treatable pts 250.xlsx`

## Mapping Table

| source file | source location / columns | target normalized file | transformation rules | assumptions or gaps |
| --- | --- | --- | --- | --- |
| `treatable pts 250.xlsx` | Sheet `updated calc`, annual Year blocks, row labels `US` / `EU`, module columns `AML`, `CML-inc`, `CML_prev`, `MDS` | `data/curated/real_scenario_01/commercial_forecast_module_level.csv` | Convert annual treated-patient totals into monthly rows using `month_index`. Each annual value is split evenly across the 12 months of that year. Module names are normalized to `AML`, `MDS`, `CML_Incident`, `CML_Prevalent`. | Raw source is annual, not monthly. The equal monthly split is a clearly labeled placeholder transformation for Phase 1. |
| `treatable pts 250.xlsx` | Same annual module table | `data/curated/real_scenario_01/commercial_forecast_segment_level.csv` | No direct segment-level source exists, so the file is created as header-only for compatibility with the dual-grain loader contract. | Segment-level commercial source is missing in `data/raw/`. |
| `treatable pts 250.xlsx` | Distinct geography labels in annual table: `US`, `EU` | `data/curated/real_scenario_01/geography_master.csv` | Derive distinct `geography_code` rows from the raw annual table. | `market_group`, `currency_code`, and `launch_sequence_rank` are not present in the raw workbook and remain `PLACEHOLDER`. |
| `treatable pts 250.xlsx` | No AML mix columns or table present | `data/curated/real_scenario_01/aml_segment_mix.csv` | Generate one mix row per `geography_code x month_index x segment_code` using equal-share placeholder values across `1L_fit`, `1L_unfit`, and `RR`. | Approved AML segment mix source is missing. Placeholder values are clearly labeled and should be replaced before production use. |
| `treatable pts 250.xlsx` | No MDS mix columns or table present | `data/curated/real_scenario_01/mds_segment_mix.csv` | Generate one mix row per `geography_code x month_index x segment_code` using equal-share placeholder values across `HR_MDS` and `LR_MDS`. | Approved MDS segment mix source is missing. Placeholder values are clearly labeled and should be replaced before production use. |
| `treatable pts 250.xlsx` | Base prevalent pool values in first Year block right-side table: `CML_prev` for `US` and `EU` | `data/curated/real_scenario_01/inp_cml_prevalent.csv` | Use raw annual base prevalent pool values and divide by 12 to create a monthly addressable prevalent pool for months where `CML_Prevalent` forecast exists. | Raw source lacks an explicit monthly addressable pool series. Monthly pool is derived from annual values as a clearly labeled placeholder transformation. |
| `treatable pts 250.xlsx` | Embedded note text and first non-zero annual forecast year by geography/module | `data/curated/real_scenario_01/launch_timing_inferred.csv` | Infer launch timing as the first non-zero annual forecast year for each `geography_code x module`; compute `launch_month_index` as the first month of that year. | Current Phase 1 runner does not consume a dedicated launch timing input yet. The scenario still uses the contract placeholder `us_aml_mds_initial_approval_date = 2029-01-01`. |
| No source file in `data/raw/` | None | `data/curated/real_scenario_01/inp_epi_crosscheck.csv` | Create a header-only file because no epi cross-check source was found in `data/raw/`. | Epi source is missing for this scenario. |

## Explicit Gaps
- No approved AML segment mix source file is present in `data/raw/`.
- No approved MDS segment mix source file is present in `data/raw/`.
- No segment-level commercial forecast source file is present in `data/raw/`.
- No explicit full-date launch anchor is present in the raw workbook.
- The only real source is annual, so monthly normalization requires a temporary placeholder disaggregation rule.

