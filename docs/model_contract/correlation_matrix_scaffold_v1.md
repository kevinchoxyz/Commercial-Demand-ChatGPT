# Correlation Matrix Scaffold v1

Status: `documented but not active in Phase 1`

This scaffold should be stored as a configurable square matrix keyed by `variable_name`. It remains out of scope for the deterministic Phase 1 build, but the starting structure is approved for later phases.

## Non-Zero Starting Correlations
| variable_a | variable_b | rho | note |
| --- | --- | --- | --- |
| trade.us.sites_at_launch | trade.eu.sites_at_launch | 0.40 | shared execution quality |
| trade.us.site_activation_rate | trade.eu.site_activation_rate | 0.30 | partially shared commercial execution |
| trade.us.sites_at_peak | trade.eu.sites_at_peak | 0.30 | weak positive |
| yield.ds.performance | leadtime.ds.weeks | -0.45 | low yield drives rework and longer cycle |
| yield.dp.performance | leadtime.dp.weeks | -0.35 | same logic at DP |
| yield.ds.performance | failure.ds.batch | -0.35 | process instability linkage |

## Zero-Correlation Defaults
- approval timing vs manufacturing variables = `0`
- approval timing vs demand variables = `0`
- uptake across different indications = `0`
- patient uptake vs manufacturing yield = `0`
- newly added variables default to `0` until explicitly mapped

## Matrix Validation Rules
- diagonal must equal `1.0`
- matrix must be symmetric
- all `rho` values must be between `-1` and `+1`
- engine must fail fast if the matrix is not positive semi-definite or cannot be repaired under allowed tolerance

## Phase 1 Rule
- Keep this file as reference only.
- Do not wire Monte Carlo or correlation behavior into the current scaffold.
