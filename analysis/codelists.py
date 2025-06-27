#######################################################################################
# IMPORT
#######################################################################################
from ehrql import codelist_from_csv

#######################################################################################
# CODELISTS
#######################################################################################

## Ethnicity (for diabetes-algo)
ethnicity_codes = codelist_from_csv(
    "codelists/opensafely-ethnicity-snomed-0removed.csv",  # there is a newer version, but it does not have column snomed so would have to update this
    column="code",
    category_column="Grouping_6",
)


## DIABETES (for diabetes-algo)
# T1DM
diabetes_type1_ctv3_clinical = codelist_from_csv("codelists/user-hjforbes-type-1-diabetes.csv",column="code")
# T2DM
diabetes_type2_ctv3_clinical = codelist_from_csv("codelists/user-hjforbes-type-2-diabetes.csv",column="code")
# Other or non-specific diabetes
diabetes_other_ctv3_clinical = codelist_from_csv("codelists/user-hjforbes-other-or-nonspecific-diabetes.csv",column="code")
# Gestational diabetes
diabetes_gestational_ctv3_clinical = codelist_from_csv("codelists/user-hjforbes-gestational-diabetes.csv",column="code")
diabetes_gestational_icd10 = codelist_from_csv("codelists/user-alainamstutz-gestational-diabetes-icd10-bristol.csv",column="code")
# Type 1 diabetes secondary care
diabetes_type1_icd10 = codelist_from_csv("codelists/opensafely-type-1-diabetes-secondary-care.csv",column="icd10_code")
# Type 2 diabetes secondary care
diabetes_type2_icd10 = codelist_from_csv("codelists/user-r_denholm-type-2-diabetes-secondary-care-bristol.csv",column="code")
# Non-diagnostic diabetes codes
diabetes_diagnostic_ctv3_clinical = codelist_from_csv("codelists/user-hjforbes-nondiagnostic-diabetes-codes.csv",column="code")
# HbA1c
hba1c_snomed = codelist_from_csv("codelists/opensafely-glycated-haemoglobin-hba1c-tests-numerical-value.csv",column="code")
# Antidiabetic drugs
insulin_dmd = codelist_from_csv("codelists/opensafely-insulin-medication.csv",column="id")
antidiabetic_drugs_snomed_clinical = codelist_from_csv("codelists/opensafely-antidiabetic-drugs.csv",column="id")
non_metformin_dmd = codelist_from_csv("codelists/user-r_denholm-non-metformin-antidiabetic-drugs_bristol.csv",column="id")


## CVD
cvd_chd = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-chd_cod.csv",column="code")
cvd_nonhaemorrhagic_stroke = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-ostr_cod.csv",column="code")
cvd_pad = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-pad_cod.csv",column="code")


