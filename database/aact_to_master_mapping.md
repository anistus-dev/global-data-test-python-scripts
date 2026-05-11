# AACT (ClinicalTrials.gov) to Master Schema Mapping Specification

This document provides the definitive, exhaustive technical blueprint for mapping the AACT database dump into the Unified Clinical Intelligence Master Schema.

---

## 0. Global Identity & Data Lineage Strategy

This architecture follows a **"Source-First, Unified-Later"** approach:

1. **`clinical.trial`**: Every AACT record creates exactly one entry here. 
2. **Identification**: The record is identified by `source_id` (pointing to 'CTGOV') and `primary_registry_id` (the NCT ID).
3. **Redundancy Removal**: Redundant technical source URLs and IDs are removed in favor of dynamic generation via the `source_id`.
4. **Unification**: The `unified_trial_id` is a nullable foreign key. It is populated only when a trial is deduplicated against other sources (e.g., ISRCTN) and assigned a `unified_trial_code` (e.g., `GDCT-XXXX`).

---

## 1. Core Trial Mapping (`clinical.trial`)

**Target Table**: `clinical.trial`

| Master Column | AACT Source Table | AACT Source Column | Logic / Transformation |
|:---|:---|:---|:---|
| **source_id** | - | - | Lookup for 'CTGOV' in `ref.source`. |
| **primary_registry_id**| `studies` | `nct_id` | **Mandatory**. |
| **title** | `studies` | `brief_title` | |
| **official_title** | `studies` | `official_title` | |
| **acronym** | `studies` | `acronym` | |
| **brief_summary** | `brief_summaries` | `description` | One-to-one join on `nct_id`. |
| **detailed_description**| `detailed_descriptions`| `description` | One-to-one join on `nct_id`. |
| **study_type** | `studies` | `study_type` | Normalize to 'Interventional' or 'Observational'. |
| **therapy_type** | `interventions` | `count(*)` | 'Monotherapy' if 1, 'Combination' if > 1. |
| **phase** | `studies` | `phase` | Normalize: 'Phase 1', 'Phase 2', 'Phase 3', etc. |
| **status** | `studies` | `overall_status` | Map to unified set (Recruiting, Completed, etc.). |
| **has_results** | `studies` | `results_first_submitted_date` | `true` if NOT NULL. |
| **data_monitoring_committee** | `studies` | `has_dmc` | Boolean conversion. |
| **purpose** | `designs` | `primary_purpose` | |
| **sex** | `eligibilities` | `gender` | Normalize to 'All', 'Male', 'Female'. |
| **minimum_age** | `eligibilities` | `minimum_age` | |
| **maximum_age** | `eligibilities` | `maximum_age` | |
| **healthy_volunteers** | `eligibilities` | `healthy_volunteers` | Boolean. |
| **enrollment_planned** | `studies` | `enrollment` | Only if `enrollment_type` = 'ESTIMATED'. |
| **enrollment_actual** | `studies` | `enrollment` | Only if `enrollment_type` = 'ACTUAL'. |
| **number_of_sites** | `facilities` | `count(*)` | Distinct count of facility IDs per NCT. |
| **start_date** | `studies` | `start_date` | |
| **start_date_type** | `studies` | `start_date_type` | 'Actual' or 'Estimated'. |
| **primary_completion_date**| `studies` | `primary_completion_date` | |
| **completion_date** | `studies` | `completion_date` | |
| **completion_date_type**| `studies` | `completion_date_type` | |
| **source_last_updated**| `studies` | `last_update_submitted_date` | |
| **first_seen_at** | - | - | Set to current date on first insert. |

---

## 2. Secondary IDs (`clinical.secondary_identifier`)

**Source Table**: `id_information`

Map all non-primary IDs (Secondary, Sponsor, Other) to enable cross-reference searching.

| Master Column | AACT Source Column | Logic / Transformation |
|:---|:---|:---|
| **identifier_value** | `id_value` | |
| **identifier_type** | `id_type` | e.g., 'org_study_id', 'secondary_id', 'nci_id'. |

---

## 3. Clinical Components (Linked Tables)

### Indications (`clinical.trial_indication`)
- **Source Table**: `conditions`
- **Logic**: Join `nct_id`. Link to `ref.indication` using name matching. Standardize to lowercase before lookup.

### Sponsors & Collaborators (`clinical.trial_sponsor`)
- **Source Table**: `sponsors`
- **Logic**:
    - **`organization_id`**: Match/Create in `company.organization`.
    - **`sponsor_role`**: 'lead' maps to 'sponsor', 'collaborator' maps to 'collaborator'.
    - **`is_lead`**: `true` if `lead_or_collaborator` is 'lead'.

### Interventions & Drugs (`clinical.intervention`)
- **Source Table**: `interventions`
- **Logic**:
    - **`intervention_name`**: Raw name from AACT.
    - **`intervention_type`**: Drug, Biologic, Device, Procedure, Behavioral, etc.
    - **`product_id`**: Match/Create in `drug.product`.
    - **`route_id`**: Map from `intervention_other_names` or descriptions if possible.

### Arms & Regimens (`clinical.arm`)
- **Source Table**: `design_groups`
- **Logic**: Map `arm_code` (label), `arm_title`, and `description`. Link to interventions via `clinical.arm_intervention`.

### Outcomes & Endpoints (`clinical.outcome`)
- **Source Table**: `design_outcomes`
- **Logic**: 
    - **`outcome_type`**: Map `outcome_type` to 'primary', 'secondary', or 'other'.
    - **`measure`**: The actual endpoint measure description.
    - **`time_frame`**: The specific assessment time.

### Eligibility Criteria (`clinical.eligibility_criterion`)
- **Source Table**: `eligibilities.criteria`
- **Logic**: This is a TEXT field. Requires parsing into individual bullet points. 
- **Type**: Tag as 'inclusion' or 'exclusion' based on header parsing.

---

## 4. Operational Data (Sites & People)

### Sites & Recruitment (`clinical.trial_site` & `clinical.site`)
- **Source Table**: `facilities`
- **Logic**: 
    - First, ensure the `clinical.site` exists using `name`, `city`, `state`, and `country`.
    - Then link to the trial in `clinical.trial_site`.
    - **`recruitment_status`**: Map from `facility_investigators` or `facilities.status`.

### Investigators (`clinical.trial_investigator`)
- **Source Table**: `facility_investigators`
- **Logic**: Map to `company.person`. Assign roles like 'Principal Investigator' or 'Sub-Investigator'.

### Contacts (`clinical.contact`)
- **Source Table**: `central_contacts`
- **Logic**: Capture name, phone, and email for the trial's central point of contact.

---

## 5. Intelligence & Scientific Data

### Biomarkers (`clinical.trial_biomarker`)
- **Source**: Extract from `keywords` and `eligibility` criteria.
- **Logic**: Match against `scientific.biomarker` dictionary.

### Publications (`clinical.trial_publication`)
- **Source Tables**: `references` & `links`
- **Logic**: 
    - Capture PMID and DOI.
    - Match/Create in `scientific.publication`.
    - Link to trial as 'related_publication'.

### Events & Change History (`clinical.event` & `clinical.change_history`)
- **Source Table**: `calculated_values` or version snapshots.
- **Logic**: Track significant status changes or phase transitions over time.
