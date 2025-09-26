#######################################################################################
# IMPORT
#######################################################################################
from ehrql import codelist_from_csv

#######################################################################################
# CODELISTS
#######################################################################################

## Pregnancy
preg_cod_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-preg_cod.csv",
    column="code"
    )                                
c19preg_cod_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-c19preg_cod.csv",
    column="code"
)

## Contraceptive pill
cocp_dmd = codelist_from_csv(
    "codelists/user-elsie_horne-cocp_dmd.csv",
    column="code"
    )    

## Hormone replacement therapy
hrt_dmd = codelist_from_csv(
    "codelists/user-elsie_horne-hrt_dmd.csv",
    column="code"
    )    

## Prostate cancer
prostate_cancer_snomed_clinical = codelist_from_csv(
    "codelists/user-RochelleKnight-prostate_cancer_snomed.csv",
    column="code"
    )   
prostate_cancer_icd10 = codelist_from_csv(
    "codelists/user-RochelleKnight-prostate_cancer_icd10.csv",
    column="code"
    )   

## Statins contraindicated
lipidtheradv_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-lipidtheradv_cod.csv",
    column="code"
)
arstat_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-arstat_cod.csv",
    column="code"
)
aratorvastat_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-aratorvastat_cod.csv",
    column="code"
)
statall_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-statall_cod.csv",
    column="code"
)
statintol_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-statintol_cod.csv",
    column="code"
)
xstat_exp1_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-txstat_cod.csv",
    column="code"
)
xstat_exp2_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-statin-contraindications-expiring-codes.csv",
    column="code"
)
xstat_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-xstat_cod.csv",
    column="code"
)
statcontr_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-statcontr_cod.csv",
    column="code"
)
statnind_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-statnind_cod.csv",
    column="code"
)

## Palliative care
palliat_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-palcare_cod.csv",
    column="code"
)

## Haemorrhagic stroke
hstrk_snomed = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-hstrk_cod.csv",
    column="code"
)
hstrk_icd10 = codelist_from_csv(
    "codelists/opensafely-stroke-secondary-care.csv",
    column="icd"
)

## Decompensated cirrhosis
decomp_liver_cirrh_snomed = codelist_from_csv(
    "codelists/opensafely-condition-advanced-decompensated-cirrhosis-of-the-liver.csv",
    column="code"
)
decomp_liver_cirrh_icd10 = codelist_from_csv(
    "codelists/opensafely-condition-advanced-decompensated-cirrhosis-of-the-liver-and-associated-conditions-icd-10.csv",
    column="code"
)

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
cvd_chd_snomed = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-chd_cod.csv",column="code")
cvd_chd_icd10 = codelist_from_csv("codelists/user-alainamstutz-coronary-heart-disease-secondary-care.csv",column="code")

cvd_angina_snomed = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-chd_cod.csv",column="code")
cvd_angina_icd10 = codelist_from_csv("codelists/user-RochelleKnight-angina_icd10.csv",column="code")

cvd_ami_snomed = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-mi_cod.csv",column="code")
cvd_ami_icd10 = codelist_from_csv("codelists/user-RochelleKnight-ami_icd10.csv",column="code")
cvd_ami_prior_icd10 = codelist_from_csv("codelists/user-elsie_horne-ami_prior_icd10.csv",column="code")

cvd_nonhaemorrhagic_stroke_snomed = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-ostr_cod.csv",column="code")
cvd_nonhaemorrhagic_stroke_icd10 = codelist_from_csv("codelists/user-RochelleKnight-stroke_isch_icd10.csv",column="code")

cvd_pad_snomed = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-pad_cod.csv",column="code")
#cvd_pad_icd10 = 


## QRISK - but I think there are more codes than these 4, need to work on this!
qrisk_snomed = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-qriskscore_cod.csv",column="code")


## Statins - but need to double-check this codelist! 
statins_dmd = codelist_from_csv("codelists/opensafely-statin-medication.csv",column="code")


### CODELISTS - PRIMIS

## Asthma
# Asthma Diagnosis code
ast = codelist_from_csv("codelists/primis-covid19-vacc-uptake-ast.csv",column="code")
# Asthma Admission codes
astadm = codelist_from_csv("codelists/primis-covid19-vacc-uptake-astadm.csv",column="code")
# Asthma inhaler or nebuliser medication codes
astrxm1 = codelist_from_csv("codelists/primis-covid19-vacc-uptake-astrxm1.csv",column="code")
# Asthma systemic steroid medication codes
astrxm2 = codelist_from_csv("codelists/primis-covid19-vacc-uptake-astrxm2.csv",column="code")

## Chronic Respiratory Disease
resp_cov = codelist_from_csv("codelists/primis-covid19-vacc-uptake-resp_cov.csv",column="code")

## Chronic heart disease codes
chd_cov = codelist_from_csv("codelists/primis-covid19-vacc-uptake-chd_cov.csv",column="code")

## CKD
# Chronic kidney disease diagnostic codes
ckd_cov = codelist_from_csv("codelists/primis-covid19-vacc-uptake-ckd_cov.csv",column="code")
# Chronic kidney disease codes - all stages
ckd15 = codelist_from_csv("codelists/primis-covid19-vacc-uptake-ckd15.csv",column="code")
# Chronic kidney disease codes-stages 3 - 5
ckd35 = codelist_from_csv("codelists/primis-covid19-vacc-uptake-ckd35.csv",column="code")

## Chronic Liver disease codes
cld = codelist_from_csv("codelists/primis-covid19-vacc-uptake-cld.csv",column="code")

## Diabetes
# Diabetes diagnosis codes
diab = codelist_from_csv("codelists/primis-covid19-vacc-uptake-diab.csv",column="code")
## Diabetes resolved codes
dmres = codelist_from_csv("codelists/primis-covid19-vacc-uptake-dmres.csv",column="code")

## Gestational diabetes diagnosis codes
gdiab = codelist_from_csv("codelists/primis-covid19-vacc-uptake-gdiab_cod.csv",column = "code")

## Addisons disease and hypoadrenalism diagnosis codes
addis = codelist_from_csv("codelists/primis-covid19-vacc-uptake-addis_cod.csv",column = "code")

## Pregnancy / delivery
# Pregnancy delivery codes
pregdel = codelist_from_csv("codelists/primis-covid19-vacc-uptake-pregdel.csv",column = "code")
# Pregnancy codes
preg = codelist_from_csv("codelists/primis-covid19-vacc-uptake-preg.csv",column = "code")

## Severe mental illness / remission
# Severe Mental Illness codes
sev_mental = codelist_from_csv("codelists/primis-covid19-vacc-uptake-sev_mental.csv",column = "code")
# Remission codes relating to Severe Mental Illness
smhres = codelist_from_csv("codelists/primis-covid19-vacc-uptake-smhres.csv",column = "code")

## Chronic Neurological Disease including Significant Learning Disorder
cns_cov = codelist_from_csv("codelists/primis-covid19-vacc-uptake-cns_cov.csv",column = "code")

## Immunosuppression
# Immunosuppression diagnosis codes
immdx_cov = codelist_from_csv("codelists/primis-covid19-vacc-uptake-immdx_cov.csv",column = "code")
# Immunosuppression medication codes
immrx = codelist_from_csv("codelists/primis-covid19-vacc-uptake-immrx.csv",column = "code")
# Immunosuppression admin codes
immadm = codelist_from_csv("codelists/primis-covid19-vacc-uptake-immunosuppression-admin-codes.csv",column = "code")

## Chemotherapy or radiation (Primis)
dxt_chemo = codelist_from_csv("codelists/primis-covid19-vacc-uptake-dxt_chemo_cod.csv",column = "code")

## Asplenia or Dysfunction of the Spleen codes
spln_cov = codelist_from_csv("codelists/primis-covid19-vacc-uptake-spln_cov.csv",column = "code")

## Severe Obesity
# BMI
bmi = codelist_from_csv("codelists/primis-covid19-vacc-uptake-bmi.csv",column = "code")
# All BMI coded terms
bmi_stage = codelist_from_csv("codelists/primis-covid19-vacc-uptake-bmi_stage.csv",column = "code")

# Severe Obesity code recorded
sev_obesity = codelist_from_csv("codelists/primis-covid19-vacc-uptake-sev_obesity.csv",column = "code")

## Wider Learning Disability
learndis = codelist_from_csv("codelists/primis-covid19-vacc-uptake-learndis.csv",column = "code")