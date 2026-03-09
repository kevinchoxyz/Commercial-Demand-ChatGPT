"""Shared constants for the Phase 1 scaffold."""

PRIMARY_DEMAND_INPUT = "Commercial Patients Treated"
PHASE1_HORIZON_MONTHS = 240
PHASE1_TIME_GRAIN = "monthly"
PHASE2_TIME_GRAIN = "monthly"
FORECAST_GRAIN_MODULE_LEVEL = "module_level"
FORECAST_GRAIN_SEGMENT_LEVEL = "segment_level"
SUPPORTED_FORECAST_GRAINS = (
    FORECAST_GRAIN_MODULE_LEVEL,
    FORECAST_GRAIN_SEGMENT_LEVEL,
)
FORECAST_FREQUENCY_MONTHLY = "monthly"
FORECAST_FREQUENCY_ANNUAL = "annual"
SUPPORTED_FORECAST_FREQUENCIES = (
    FORECAST_FREQUENCY_MONTHLY,
    FORECAST_FREQUENCY_ANNUAL,
)
DEMAND_BASIS_TREATED_CENSUS = "treated_census"
DEMAND_BASIS_PATIENT_STARTS = "patient_starts"
SUPPORTED_DEMAND_BASES = (
    DEMAND_BASIS_TREATED_CENSUS,
    DEMAND_BASIS_PATIENT_STARTS,
)

PHASE1_MODULES = ("AML", "MDS", "CML_Incident", "CML_Prevalent")
PHASE1_MIX_TABLE_MODULES = ("AML", "MDS")
PHASE1_DISABLED_CAPABILITIES = (
    "trade",
    "production",
    "inventory",
    "financials",
    "monte_carlo",
)

AML_SEGMENTS = ("1L_fit", "1L_unfit", "RR")
MDS_SEGMENTS = ("HR_MDS", "LR_MDS")

PHASE2_BUILD_SCOPE = "deterministic_dose_unit_cascade"
PHASE2_UPSTREAM_DEMAND_CONTRACT = "monthlyized_output.csv"
PHASE3_BUILD_SCOPE = "deterministic_trade_layer"
PHASE3_UPSTREAM_DEMAND_CONTRACT = "phase2_deterministic_cascade.csv"
DOSE_BASIS_FIXED = "fixed"
DOSE_BASIS_WEIGHT_BASED = "weight_based"
SUPPORTED_DOSE_BASES = (DOSE_BASIS_FIXED, DOSE_BASIS_WEIGHT_BASED)
FG_VIALING_RULE_CEIL_MG_PER_UNIT_NO_SHARING = "ceil_mg_per_unit_no_sharing"
SUPPORTED_FG_VIALING_RULES = (FG_VIALING_RULE_CEIL_MG_PER_UNIT_NO_SHARING,)
CO_PACK_MODE_SEPARATE_SKU_FIRST = "separate_sku_first"
SUPPORTED_CO_PACK_MODES = (CO_PACK_MODE_SEPARATE_SKU_FIRST,)
PHASE3_DISABLED_CAPABILITIES = (
    "production",
    "inventory",
    "financials",
    "monte_carlo",
)

# The current contract keeps CML as separate modules rather than an AML/MDS-style mix table.
# These module-name segments are explicit placeholders until the approved spec defines an alternative.
CML_INCIDENT_SEGMENTS = ("CML_Incident",)
CML_PREVALENT_SEGMENTS = ("CML_Prevalent",)

MODULE_TO_INDICATION = {
    "AML": "AML",
    "MDS": "MDS",
    "CML_Incident": "CML",
    "CML_Prevalent": "CML",
}

MODULE_TO_SEGMENTS = {
    "AML": AML_SEGMENTS,
    "MDS": MDS_SEGMENTS,
    "CML_Incident": CML_INCIDENT_SEGMENTS,
    "CML_Prevalent": CML_PREVALENT_SEGMENTS,
}
