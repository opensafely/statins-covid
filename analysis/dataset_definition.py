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
### Primary prevention: any of the following
### - >85 years old
### - Predicted 10-year cardiovascular risk score ≥10%
### - Type 1 diabetes
## Secondary prevention:
### - Established cardiovascular disease

## index date
index_date = "2015-02-01"

## alive and in care criteria
is_alive = patients.is_alive_on(index_date)
is_registered = practice_registrations.for_patient_on(
    index_date
).exists_for_patient()

## primary prevention
aged_over_85 = patients.age_on(index_date) >= 85
# cvd_risk_high = ...
dm_type1 = (
    (data_processed_dm_algo.cat_diabetes == "T1DM") & 
    (data_processed_dm_algo.t1dm_date <= index_date)
    )

## secondary prevention
# cvd_established = ...

## combined criteria
alive_registered = (is_alive & is_registered)
# statin_elig_primary = (aged_over_85 | cvd_risk_high | dm_type1)
# statin_elig_secondary = cvd_established

# is_elig = (alive_registered &
#                (statin_elig_primary | statin_elig_secondary)
# )

is_elig_temp = (
    alive_registered & 
    (aged_over_85 | dm_type1)                
)

#######################################################################################
# INITIALISE the dataset and set the dummy dataset size
#######################################################################################

dataset = create_dataset()
dataset.configure_dummy_data(population_size=1000)
dataset.define_population(is_elig_temp)

dataset.sex = patients.sex
dataset.age = patients.age_on(index_date)
dataset.cat_diabetes = data_processed_dm_algo.cat_diabetes
dataset.t1dm_date = data_processed_dm_algo.t1dm_date


show(dataset)






