#######################################################################################
# IMPORT
#######################################################################################

## ehrQL functions
from ehrql import (create_dataset, show, months, years)

## for import of diabetes algo created data
from ehrql.query_language import (
    table_from_file,
    PatientFrame,
    Series
)

## tables
from ehrql.tables.tpp import (
    patients, 
    practice_registrations, 
    clinical_events,
    clinical_events_ranges, 
    medications,
    addresses,
    ons_deaths
    )

## codelists
from codelists import *

## for import of diabetes algo created data
from datetime import date

## variable helper functions 
from variable_helper_functions import *

## import diabetes algo created data
@table_from_file("output/data_processed_dm_algo.csv.gz")
class data_processed_dm_algo(PatientFrame):
    #qa_num_birth_year = Series(int) # could import it, too, but creates friction with data formatting function
    cat_diabetes = Series(str)
    t1dm_date = Series(date)



#######################################################################################
# INITIALISE the dataset and set the dummy dataset size
#######################################################################################

dataset = create_dataset()

dataset.configure_dummy_data(population_size=1000)

dataset.define_population(patients.exists_for_patient())



#######################################################################################
# DEFINE the dates
#######################################################################################

index_date = "2020-02-01"


#######################################################################################
# DATA COMPLETENESS & QUALITY ASSURANCE CRITERIA
#######################################################################################

### data completeness criteria

## alive
dataset.qa_bin_alive = patients.is_alive_on(index_date)
## registered
dataset.qa_bin_registered = (
    practice_registrations
    .spanning_with_systmone(index_date - days(365), index_date)
    .exists_for_patient()
)
    # see https://docs.opensafely.org/ehrql/reference/schemas/tpp/#practice_registrations.spanning
## known sex
dataset.qa_bin_female_or_male = patients.sex.is_in(["female", "male"]) 
## known year of birth
# dataset.qa_bin_known_birth_year = patients.date_of_birth.exists_for_patient()
# TODO check if above is needed

## known deprivation (IMD)
dataset.qa_bin_known_imd = addresses.for_patient_on(index_date).exists_for_patient()



### quality assurance criteria

## realistic age
dataset.qa_bin_age_credible = (
    (patients.age_on(index_date) >= 0) & 
    (patients.age_on(index_date) <= 110)
)
## no future death recorded
# dataset.qa_bin_no_future_death = (
    # patients.date_of_death.is_null | 
    # (patients.date_of_death < Sys.Date())
# )  # TODO: was having issues with Sys.Date(), do this part in R?
## men with pregnancy codes, hormone replacement therapy, or contraceptive pill
dataset.qa_bin_male_pregnant = (
    patients.sex == "male"
    ) & (
        clinical_events
        .where(
            clinical_events.snomedct_code.is_in(preg_cod_snomed ) | 
            clinical_events.snomedct_code.is_in(c19preg_cod_snomed)
            )
        .exists_for_patient()
    )
dataset.qa_bin_male_hrt = (
    patients.sex == "male"
    ) & (
        clinical_events
        .where(clinical_events.snomedct_code.is_in(hrt_dmd))
        .exists_for_patient()
    )
dataset.qa_bin_male_cocp = (
    patients.sex == "male"
    ) & (
        medications
        .where(medications.dmd_code.is_in(cocp_dmd))
        .exists_for_patient()
    )
## women with prostate cancer diagnosis
dataset.qa_bin_female_prostate_cancer = (
    patients.sex == "female"
    ) & (
        (
            # Primary care
            clinical_events
            .where(clinical_events.snomedct_code.is_in(prostate_cancer_snomed_clinical))
            .exists_for_patient()
        ) | (
            # HES APC
            apcs
            .where(apcs.all_diagnoses.contains_any_of(prostate_cancer_icd10))
            .exists_for_patient()
        ) # | (    # TODO
            # ONS (stated anywhere on death certificate)
            # cause_of_death_matches(prostate_cancer_icd10)    # TODO; requires helper function cause_of_death; tbd
        )


### combined completeness and quality assurance criteria
dataset.qa_passed = (
    dataset.qa_bin_alive & 
    dataset.qa_bin_registered & 
    dataset.qa_bin_female_or_male & 
    # dataset.qa_bin_known_birth_year &    # TODO check if needed
    dataset.qa_bin_known_imd & 
    dataset.qa_bin_age_credible & 
    # dataset.qa_bin_no_future_death &     # TODO
    # not
    ~(
        dataset.qa_bin_male_pregnant |
        dataset.qa_bin_male_hrt | 
        dataset.qa_bin_male_cocp | 
        dataset.qa_bin_female_prostate_cancer
      )
)




#######################################################################################
# ELIGIBILITY CRITERIA
#######################################################################################

## People eligible for statins for primary or secondary prevention of CVD at any point after 1 Feb 2015
## Must fulfill: A & (B | C) & !D
## A) General requirements:
### - Alive on index date
### - With registration on index date
### - Aged over 40 years on index date
### - Not pregnant on index date
### - No known contraindication for statin use on index date 
## B) Primary prevention: any of the following
### - >=85 years old
### - Predicted 10-year cardiovascular risk score ≥10%
### - Type 1 diabetes
### - CKD
## C) Secondary prevention:
### - Established cardiovascular disease



### general criteria

## age range
dataset.elig_bin_age_min40 = patients.age_on(index_date) >= 40 # combine with T1DM ?
dataset.elig_bin_age_below85 = patients.age_on(index_date) < 85

## pregnant
dataset.elig_bin_preg_breast = (
    clinical_events.where(
        (
            clinical_events.snomedct_code.is_in(preg_cod_snomed ) | 
            clinical_events.snomedct_code.is_in(c19preg_cod_snomed)
        ) & 
        clinical_events.date.is_on_or_between(index_date - years(2), index_date)
        )
        .exists_for_patient()
        )

## statins contraindicated or not indicated
dataset.elig_bin_contraind_statin = (
    clinical_events.where(
        (
            clinical_events.snomedct_code.is_in(lipidtheradv_snomed) | 
            clinical_events.snomedct_code.is_in(arstat_snomed) | 
            clinical_events.snomedct_code.is_in(aratorvastat_snomed) | 
            clinical_events.snomedct_code.is_in(statall_snomed) | 
            clinical_events.snomedct_code.is_in(statintol_snomed) |
            clinical_events.snomedct_code.is_in(xstat_exp1_snomed) | 
            clinical_events.snomedct_code.is_in(xstat_exp2_snomed) | 
            clinical_events.snomedct_code.is_in(xstat_snomed) | 
            clinical_events.snomedct_code.is_in(statcontr_snomed) | 
            clinical_events.snomedct_code.is_in(statnind_snomed)
            ) & 
            clinical_events.date.is_before(index_date)
            )
            .exists_for_patient()
            )

## history of palliative care
dataset.elig_bin_hist_palliat = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(palliat_snomed) & 
        clinical_events.date.is_before(index_date)
        )
        .exists_for_patient()
)

## history of haemorrhagic stroke
dataset.elig_bin_hist_hstrk = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(hstrk_snomed) & 
        clinical_events.date.is_before(index_date)
    )
    ).exists_for_patient() | (
        apcs.where(
            apcs.all_diagnoses.contains_any_of(hstrk_icd10) & 
            apcs.admission_date.is_before(index_date)
            )
    ).exists_for_patient()


## history of active liver disease?

## history of decompensated cirrhosis
dataset.elig_bin_hist_decomp_liver_cirrh = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(decomp_liver_cirrh_snomed) & 
        clinical_events.date.is_before(index_date)
        )
        ).exists_for_patient() | (
    apcs.where(
        apcs.all_diagnoses.contains_any_of(decomp_liver_cirrh_icd10) & 
        apcs.admission_date.is_before(index_date)
        )
).exists_for_patient()

## combined: general criteria
dataset.elig_bin_general = (
    dataset.elig_bin_age_min40 &
    dataset.elig_bin_age_below85 & 
    ~ dataset.elig_bin_preg_breast & 
    ~ dataset.elig_bin_contraind_statin & 
    ~ dataset.elig_bin_hist_palliat & 
    ~ dataset.elig_bin_hist_hstrk & 
    # no history of active liver disease
    ~ dataset.elig_bin_hist_decomp_liver_cirrh
)


### primary prevention

## QRISK
# Extract latest maximum QRISK value, measured in past 5 years, from event_clinical_ranges table
dataset.elig_num_qrisk = (
  clinical_events_ranges.where(
    clinical_events_ranges.snomedct_code.is_in(qrisk_snomed))
    .where(clinical_events_ranges.date.is_on_or_between(index_date - months(60), index_date))
    .numeric_value.maximum_for_patient()
)
# Extract its associated comparator (if there is any)
dataset.elig_str_qrisk_comparator = ( 
  clinical_events_ranges.where(
    clinical_events_ranges.snomedct_code.is_in(qrisk_snomed))
    .where(clinical_events_ranges.date.is_on_or_between(index_date - months(60), index_date))
    .where(clinical_events_ranges.numeric_value == dataset.elig_num_qrisk)
    .sort_by(clinical_events_ranges.date)
    .last_for_patient()
    .comparator
)
# Create binary varibale
dataset.elig_bin_qrisk = dataset.elig_num_qrisk >= 10    # TODO: check if consideration of comparator needed


## type 1 diabetes
dataset.elig_bin_dm_type1 = (
    (data_processed_dm_algo.cat_diabetes == "T1DM") & 
    (data_processed_dm_algo.t1dm_date <= index_date)
    )

## chronic kidney disease
dataset.elig_bin_ckd = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(ckd_snomed) & 
        clinical_events.date.is_before(index_date)
        )
        ).exists_for_patient() | (
    apcs.where(
        apcs.all_diagnoses.contains_any_of(ckd_apcs) & 
        apcs.admission_date.is_before(index_date)
        )
).exists_for_patient()

## familial hypercholesterolaemia
dataset.elig_bin_fh = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(fhypgen_snomed) & 
        clinical_events.date.is_before(index_date)
        )
        ).exists_for_patient()

## combined: eligible for statins use for primary prevention
dataset.elig_bin_prim_prevention = (
    dataset.elig_bin_qrisk |
     dataset.elig_bin_dm_type1 |
     dataset.elig_bin_ckd |
     dataset.elig_bin_fh
)




### secondary prevention

## coronary heat disease
elig_bin_chd = (
    last_matching_event_clinical_snomed_before(cvd_chd_snomed, index_date).exists_for_patient() | # primary care
    last_matching_event_apc_before(cvd_chd_icd10, index_date).exists_for_patient() # secondary care
) # not sure we need dates for eligibility criteria with "any history of"

## angina
elig_bin_angina = (
    last_matching_event_clinical_snomed_before(cvd_angina_snomed, index_date).exists_for_patient() | # primary care
    last_matching_event_apc_before(cvd_angina_icd10, index_date).exists_for_patient() # secondary care
) 

## acute miocardial infarction
elig_bin_ami = (
    last_matching_event_clinical_snomed_before(cvd_ami_snomed, index_date).exists_for_patient() | # primary care
    last_matching_event_apc_before(cvd_ami_icd10 + cvd_ami_prior_icd10, index_date).exists_for_patient() # secondary care
) 

## non-haemorrhagic stroke
elig_bin_stroke_nonhaemo = (
    last_matching_event_clinical_snomed_before(cvd_nonhaemorrhagic_stroke_snomed, index_date).exists_for_patient() | # primary care
    last_matching_event_apc_before(cvd_nonhaemorrhagic_stroke_icd10, index_date).exists_for_patient() # secondary care
) 

## peripheral arterial disease
elig_bin_pad = (
    last_matching_event_clinical_snomed_before(cvd_pad_snomed, index_date).exists_for_patient() 
    # |
    # last_matching_event_apc_before(cvd_pad_icd10, index_date).exists_for_patient()
) 

## combined: eligible for statins use for secondary prevention
elig_bin_second_prevention = (
    elig_bin_chd |
     elig_bin_angina |
     elig_bin_ami |
     elig_bin_stroke_nonhaemo |
     elig_bin_pad
)


# cvd_events = (
#     clinical_events
#     .where(clinical_events.date < index_date)
#     .where(
#         (clinical_events.snomedct_code.is_in(cvd_chd) | 
#          clinical_events.snomedct_code.is_in(cvd_nonhaemorrhagic_stroke) | 
#          clinical_events.snomedct_code.is_in(cvd_pad))
#     )
#     )

# cvd_events_first = (
#     cvd_events
#     .sort_by(cvd_events.date)
#     .first_for_patient()
# )

# cvd_established = (
#     cvd_events_first
#     .exists_for_patient()
# )

## combined criteria
# general_elig = (is_alive & is_registered)   # TODO: & not_pregnant & no_statin_ar
# statin_elig_primary = (aged_over_85 | dm_type1)            # TODO: | cvd_risk_high | ckd

# statin_elig_secondary = cvd_established
# statin_elig_secondary = elig_bin_chd | elig_bin_angina | elig_bin_ami | elig_bin_stroke_nonhaemo | elig_bin_pad

# is_elig = (general_elig &
  #             (statin_elig_primary | statin_elig_secondary)
# )

#######################################################################################
# EXPOSURE/TREATMENT
#######################################################################################
# First statin prescription on/after index_date
exp_bin_statin_first = first_matching_med_dmd_between(statins_dmd, index_date, index_date).exists_for_patient()
exp_date_statin_first = first_matching_med_dmd_between(statins_dmd, index_date, index_date).date



#######################################################################################
# SUBGROUPS
#######################################################################################

## Sex
dataset.cov_cat_sex = patients.sex

## Age category
dataset.cov_num_age = patients.age_on(index_date)
dataset.cov_cat_age = case(
    when((dataset.cov_num_age >= 40) & (dataset.cov_num_age < 55)).then("40-54"),
    when((dataset.cov_num_age >= 55) & (dataset.cov_num_age < 70)).then("55-69"),
    when((dataset.cov_num_age >= 70) & (dataset.cov_num_age < 85)).then("70-84"),
    otherwise="out of range"
)

## Index of Multiple Deprivation (IMD)
imd_rounded = addresses.for_patient_on(index_date).imd_rounded
dataset.cov_cat_deprivation_5 = case(
    when((imd_rounded >=0) & (imd_rounded < int(32844 * 1 / 5))).then("1 (most deprived)"),
    when(imd_rounded < int(32844 * 2 / 5)).then("2"),
    when(imd_rounded < int(32844 * 3 / 5)).then("3"),
    when(imd_rounded < int(32844 * 4 / 5)).then("4"),
    when(imd_rounded < int(32844 * 5 / 5)).then("5 (least deprived)"),
    otherwise="Unknown"
)

## BMI

## COVID-19 vaccination status

## Primary vs secondary prevention


dataset.elig_cat_dm = data_processed_dm_algo.cat_diabetes
dataset.elig_date_t1dm = data_processed_dm_algo.t1dm_date
# dataset.cvd = cvd_established # I think it will be important to know who has which CVD condition -> elig_bin_chd, etc.
dataset.elig_bin_chd = elig_bin_chd
dataset.elig_bin_angina = elig_bin_angina
dataset.elig_bin_ami = elig_bin_ami
dataset.elig_bin_stroke_nonhaemo = elig_bin_stroke_nonhaemo
dataset.elig_bin_pad = elig_bin_pad
dataset.elig_bin_second_prevention = elig_bin_second_prevention
# dataset.elig_num_qrisk = dataset.elig_num_qrisk
# dataset.elig_str_qrisk_comparator = dataset.elig_str_qrisk_comparator



# PRIMIS / JCVI
# see https://github.com/opensafely/reusable-variables/blob/main/analysis/PRIMIS/dataset_definition.py

primis_index_date = "2020-02-01"

dataset.immunosuppressed = is_immunosuppressed(primis_index_date) #immunosuppress grouped
dataset.ckd = has_ckd(primis_index_date) #chronic kidney disease
dataset.crd = has_crd(primis_index_date) # chronic respratory disease
dataset.diabetes = has_diabetes(primis_index_date) #diabetes
dataset.cld = has_prior_event(cld, primis_index_date) # chronic liver disease
dataset.chd = has_prior_event(chd_cov, primis_index_date) #chronic heart disease
dataset.cns = has_prior_event(cns_cov, primis_index_date) # chronic neurological disease
dataset.asplenia = has_prior_event(spln_cov, primis_index_date) # asplenia or dysfunction of the Spleen
dataset.learndis = has_prior_event(learndis, primis_index_date) # learning Disability
dataset.smi = has_smi(primis_index_date) #severe mental illness
dataset.severe_obesity = has_severe_obesity(primis_index_date) # severe obesity

dataset.primis_atrisk = primis_atrisk(primis_index_date) # at risk (at least one of the conditions above)


# show(dataset)








