from ehrql.tables.tpp import (
    apcs,
    clinical_events, 
    clinical_events_ranges,
    medications
    )

#######################################################################################
### ANY HISTORY of ... and give first ... (including baseline_date) 
#######################################################################################
## In PRIMARY CARE
# CTV3/Read
def first_matching_event_clinical_ctv3_before(codelist, baseline_date, where=True):
    return(
        clinical_events.where(where)
        .where(clinical_events.ctv3_code.is_in(codelist))
        .where(clinical_events.date.is_on_or_before(baseline_date))
        .sort_by(clinical_events.date)
        .first_for_patient()
    )

## In SECONDARY CARE (Hospital Episodes)
def first_matching_event_apc_before(codelist, baseline_date, only_prim_diagnoses=False, where=True):
    query = apcs.where(where).where(apcs.admission_date.is_on_or_before(baseline_date))
    if only_prim_diagnoses:
         # If set to True, then check only primary diagnosis field
        query = query.where(
            apcs.primary_diagnosis.is_in(codelist)
        )
    else:
        # Else, check all diagnoses (default, i.e. when only_prim_diagnoses argument not defined)
        query = query.where(apcs.all_diagnoses.contains_any_of(codelist))
    return query.sort_by(apcs.admission_date).first_for_patient()



#######################################################################################
### COUNT all prior events (including baseline_date)
#######################################################################################
## In PRIMARY CARE
# CTV3/Read
def count_matching_event_clinical_ctv3_before(codelist, baseline_date, where=True):
    return(
        clinical_events.where(where)
        .where(clinical_events.ctv3_code.is_in(codelist))
        .where(clinical_events.date.is_on_or_before(baseline_date))
        .count_for_patient()
    )

## In SECONDARY CARE (Hospital Episodes)
def count_matching_event_apc_before(codelist, baseline_date, only_prim_diagnoses=False, where=True):
    query = apcs.where(where).where(apcs.admission_date.is_on_or_before(baseline_date))
    if only_prim_diagnoses:
        # If set to True, then check only primary diagnosis field
        query = query.where(
            apcs.primary_diagnosis.is_in(codelist)
        )
    else:
        # Else, check all diagnoses (default, i.e. when only_prim_diagnoses argument not defined)
        query = query.where(apcs.all_diagnoses.contains_any_of(codelist))
    return query.count_for_patient()



#######################################################################################
### ANY HISTORY of ... and give first ... (including baseline_date) 
#######################################################################################
## In PRIMARY CARE
# Medication
def first_matching_med_dmd_before(codelist, baseline_date, where=True):
    return(
        medications.where(where)
        .where(medications.dmd_code.is_in(codelist))
        .where(medications.date.is_on_or_before(baseline_date))
        .sort_by(medications.date)
        .first_for_patient()
    )


#######################################################################################
### ANY HISTORY of ... and give latest ... (including baseline_date) 
#######################################################################################
## In PRIMARY CARE
# Snomed
def last_matching_event_clinical_snomed_before(codelist, baseline_date, where=True):
    return(
        clinical_events.where(where)
        .where(clinical_events.snomedct_code.is_in(codelist))
        .where(clinical_events.date.is_on_or_before(baseline_date))
        .sort_by(clinical_events.date)
        .last_for_patient()
    )

## In SECONDARY CARE (Hospital Episodes)
def last_matching_event_apc_before(codelist, baseline_date, only_prim_diagnoses=False, where=True):
    query = apcs.where(where).where(apcs.admission_date.is_on_or_before(baseline_date))
    if only_prim_diagnoses:
        # If set to True, then check only primary diagnosis field
        query = query.where(
            apcs.primary_diagnosis.is_in(codelist)
        )
    else:
        # Else, check all diagnoses (default, i.e. when only_prim_diagnoses argument not defined)
        query = query.where(apcs.all_diagnoses.contains_any_of(codelist))
    return query.sort_by(apcs.admission_date).last_for_patient()


#######################################################################################
### HISTORY of ... in past ... days/months/years ... and give latest (including baseline_date)
#######################################################################################
## In PRIMARY CARE, in clinical_events_ranges table
# Snomed
def last_matching_event_clinical_ranges_snomed_between(codelist, start_date, baseline_date, where=True):
    return(
        clinical_events_ranges.where(where)
        .where(clinical_events_ranges.snomedct_code.is_in(codelist))
        .where(clinical_events_ranges.date.is_on_or_between(start_date, baseline_date))
        .sort_by(clinical_events_ranges.date)
        .last_for_patient()
    )


#######################################################################################
### Any future events (including baseline_date and study end_date)
#######################################################################################
## In PRIMARY CARE
# CTV3/Read
def first_matching_event_clinical_ctv3_between(codelist, baseline_date, end_date, where=True):
    return(
        clinical_events.where(where)
        .where(clinical_events.ctv3_code.is_in(codelist))
        .where(clinical_events.date.is_on_or_between(baseline_date, end_date))
        .sort_by(clinical_events.date)
        .first_for_patient()
    )
# Snomed
def first_matching_event_clinical_snomed_between(codelist, baseline_date, end_date, where=True):
    return(
        clinical_events.where(where)
        .where(clinical_events.snomedct_code.is_in(codelist))
        .where(clinical_events.date.is_on_or_between(baseline_date, end_date))
        .sort_by(clinical_events.date)
        .first_for_patient()
    )
# Medication
def first_matching_med_dmd_between(codelist, baseline_date, end_date, where=True):
    return(
        medications.where(where)
        .where(medications.dmd_code.is_in(codelist))
        .where(medications.date.is_on_or_between(baseline_date, end_date))
        .sort_by(medications.date)
        .first_for_patient()
    )