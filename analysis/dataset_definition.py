#######################################################################################
# IMPORT
#######################################################################################

## ehrQL functions
from ehrql import (create_dataset,
                    show
)

## for import of diabetes algo created data
from ehrql.query_language import (
    table_from_file,
    PatientFrame,
    Series
)

## tables
from ehrql.tables.core import (
    patients, 
    practice_registrations, 
    clinical_events
    )

## codelists
from codelists import *

## for import of diabetes algo created data
from datetime import date

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
index_date = "2015-02-01"


## general criteria
is_alive = patients.is_alive_on(index_date)

is_registered = practice_registrations.for_patient_on(
    index_date
).exists_for_patient()

aged_over_40 = patients.age_on(index_date) >= 40

# pregnancy

# no adverse reactions / contraindications


## primary prevention
aged_over_85 = patients.age_on(index_date) >= 85
# cvd_risk_high = ...
dm_type1 = (
    (data_processed_dm_algo.cat_diabetes == "T1DM") & 
    (data_processed_dm_algo.t1dm_date <= index_date)
    )
# ckd = ...

## secondary prevention
cvd_events = (
    clinical_events
    .where(clinical_events.date < index_date)
    .where(
        (clinical_events.snomedct_code.is_in(cvd_chd) | 
         clinical_events.snomedct_code.is_in(cvd_nonhaemorrhagic_stroke) | 
         clinical_events.snomedct_code.is_in(cvd_pad))
    )
    )

cvd_events_first = (
    cvd_events
    .sort_by(cvd_events.date)
    .first_for_patient()
)

cvd_established = (
    cvd_events_first
    .exists_for_patient()
)

## combined criteria
general_elig = (is_alive & is_registered & aged_over_40)   # TODO: & not_pregnant & no_statin_ar
statin_elig_primary = (aged_over_85 | dm_type1)            # TODO: | cvd_risk_high | ckd
statin_elig_secondary = cvd_established

is_elig = (general_elig &
               (statin_elig_primary | statin_elig_secondary)
)



#######################################################################################
# INITIALISE the dataset and set the dummy dataset size
#######################################################################################

dataset = create_dataset()
dataset.configure_dummy_data(population_size=1000)
dataset.define_population(is_elig)

dataset.sex = patients.sex
dataset.age = patients.age_on(index_date)
dataset.cat_diabetes = data_processed_dm_algo.cat_diabetes
dataset.t1dm_date = data_processed_dm_algo.t1dm_date
dataset.cvd = cvd_established


show(dataset)






