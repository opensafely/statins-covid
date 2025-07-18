#######################################################################################
# IMPORT
#######################################################################################

## ehrQL functions
from ehrql import (create_dataset, show, months, years, create_measures, INTERVAL)

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
    clinical_events_ranges
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
# ELIGIBILITY
#######################################################################################

## People eligible for statins for primary or secondary prevention of CVD at any point after 1 Feb 2015
## Must fulfill: A & (B | C) 
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


## index date
# index_date = "2015-02-01" # let's see then how this works for the Measures table


## general criteria
is_alive = patients.is_alive_on(INTERVAL.start_date)

is_registered = practice_registrations.for_patient_on(INTERVAL.start_date).exists_for_patient()

aged_over_40 = patients.age_on(INTERVAL.start_date) >= 40 # combine with T1DM ?

# pregnancy

# no adverse reactions / contraindications

## primary prevention
aged_over_85 = patients.age_on(INTERVAL.start_date) >= 85
# cvd_risk_high = ...
dm_type1 = (
    (data_processed_dm_algo.cat_diabetes == "T1DM") & 
    (data_processed_dm_algo.t1dm_date <= INTERVAL.start_date)
    )
# ckd = ...

# Extract latest maximum QRISK value, measured in past 5 years, from event_clinical_ranges table
elig_num_qrisk = (
  clinical_events_ranges.where(
    clinical_events_ranges.snomedct_code.is_in(qrisk_snomed))
    .where(clinical_events_ranges.date.is_on_or_between(INTERVAL.start_date - months(60), INTERVAL.start_date))
    .numeric_value.maximum_for_patient()
)
# Extract its associated comparator (if there is any)
elig_str_qrisk_comparator = ( 
  clinical_events_ranges.where(
    clinical_events_ranges.snomedct_code.is_in(qrisk_snomed))
    .where(clinical_events_ranges.date.is_on_or_between(INTERVAL.start_date - months(60), INTERVAL.start_date))
    .where(clinical_events_ranges.numeric_value == elig_qrisk_value)
    .sort_by(clinical_events_ranges.date)
    .last_for_patient()
    .comparator
)




## secondary prevention
elig_bin_chd = (
    last_matching_event_clinical_snomed_before(cvd_chd_snomed, INTERVAL.start_date).exists_for_patient() | # primary care
    last_matching_event_apc_before(cvd_chd_icd10, INTERVAL.start_date).exists_for_patient() # secondary care
) # not sure we need dates for eligibility criteria with "any history of"

elig_bin_angina = (
    last_matching_event_clinical_snomed_before(cvd_angina_snomed, INTERVAL.start_date).exists_for_patient() | # primary care
    last_matching_event_apc_before(cvd_angina_icd10, INTERVAL.start_date).exists_for_patient() # secondary care
) 

elig_bin_ami = (
    last_matching_event_clinical_snomed_before(cvd_ami_snomed, INTERVAL.start_date).exists_for_patient() | # primary care
    last_matching_event_apc_before(cvd_ami_icd10 + cvd_ami_prior_icd10, INTERVAL.start_date).exists_for_patient() # secondary care
) 

elig_bin_stroke_nonhaemo = (
    last_matching_event_clinical_snomed_before(cvd_nonhaemorrhagic_stroke_snomed, INTERVAL.start_date).exists_for_patient() | # primary care
    last_matching_event_apc_before(cvd_nonhaemorrhagic_stroke_icd10, INTERVAL.start_date).exists_for_patient() # secondary care
) 

elig_bin_pad = (
    last_matching_event_clinical_snomed_before(cvd_pad_snomed, INTERVAL.start_date).exists_for_patient() 
    # |
    # last_matching_event_apc_before(cvd_pad_icd10, INTERVAL.start_date).exists_for_patient()
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
general_elig = (is_alive & is_registered)   # TODO: & not_pregnant & no_statin_ar
statin_elig_primary = (aged_over_85 | dm_type1)            # TODO: | cvd_risk_high | ckd

# statin_elig_secondary = cvd_established
statin_elig_secondary = elig_bin_chd | elig_bin_angina | elig_bin_ami | elig_bin_stroke_nonhaemo | elig_bin_pad

is_elig = (general_elig &
               (statin_elig_primary | statin_elig_secondary)
)

#######################################################################################
# EXPOSURE/TREATMENT
#######################################################################################
# First statin prescription on/after index_date/INTERVAL.start_date
exp_bin_statin_first = first_matching_med_dmd_between(statins_dmd, INTERVAL.start_date, INTERVAL.end_date).exists_for_patient()
exp_date_statin_first = first_matching_med_dmd_between(statins_dmd, INTERVAL.start_date, INTERVAL.end_date).date


#######################################################################################
# INITIALISE the dataset and set the dummy dataset size
#######################################################################################

# dataset = create_dataset()
# dataset.configure_dummy_data(population_size=1000)
# dataset.define_population(is_elig)

# dataset.cov_cat_sex = patients.sex
# dataset.cov_num_age = patients.age_on(INTERVAL.start_date)
# dataset.elig_cat_dm = data_processed_dm_algo.cat_diabetes
# dataset.elig_date_t1dm = data_processed_dm_algo.t1dm_date
# # dataset.cvd = cvd_established # I think it will be important to know who has which CVD condition -> elig_bin_chd, etc.
# dataset.elig_bin_chd = elig_bin_chd
# dataset.elig_bin_angina = elig_bin_angina
# dataset.elig_bin_ami = elig_bin_ami
# dataset.elig_bin_stroke_nonhaemo = elig_bin_stroke_nonhaemo
# dataset.elig_bin_pad = elig_bin_pad

# dataset.elig_num_qrisk = elig_num_qrisk
# dataset.elig_str_qrisk_comparator = elig_str_qrisk_comparator

# show(dataset)


#######################################################################################
# INITIALISE the measures and set the dummy dataset size
#######################################################################################

measures = create_measures()
measures.configure_dummy_data(population_size=1000)

cov_cat_sex = patients.sex

measures.define_measure(
    name="statin_prescr",
    numerator=exp_bin_statin_first,
    denominator=is_elig,
    group_by={"sex": cov_cat_sex},
    intervals=years(9).starting_on("2015-02-01"),
)






