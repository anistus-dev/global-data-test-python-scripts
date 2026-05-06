"""
CTGov (AACT) → Unified Clinical Database Ingestion Script (Batch Version)
Uses execute_values for bulk inserts to maximize throughput.
"""
import os
import sys
import time
import random
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
from scripts.config import CTGOV_DB_CONFIG, UNIFIED_DB_CONFIG

# ---------------------------------------------------------------------------
# SQL: Extract from AACT
# ---------------------------------------------------------------------------
EXTRACT_STUDIES_SQL = """
SELECT s.nct_id, s.brief_title, s.official_title, s.acronym,
       s.study_type, s.phase, s.overall_status, s.why_stopped,
       s.has_dmc, s.source_class,
       s.enrollment, s.enrollment_type,
       s.start_date, s.start_date_type,
       s.primary_completion_date,
       s.completion_date, s.completion_date_type,
       s.plan_to_share_ipd, s.ipd_time_frame, s.ipd_access_criteria, s.ipd_url,
       s.last_update_submitted_date,
       s.study_first_posted_date,
       bs.description AS brief_summary,
       dd.description AS detailed_description,
       e.gender AS sex, e.minimum_age, e.maximum_age,
       e.healthy_volunteers, e.adult AS is_adult, e.child AS is_child,
       e.older_adult AS is_older_adult,
       e.sampling_method, e.population AS population_description,
       e.criteria AS eligibility_criteria,
       cv.number_of_facilities, cv.were_results_reported
FROM ctgov.studies s
LEFT JOIN ctgov.brief_summaries bs ON s.nct_id = bs.nct_id
LEFT JOIN ctgov.detailed_descriptions dd ON s.nct_id = dd.nct_id
LEFT JOIN ctgov.eligibilities e ON s.nct_id = e.nct_id
LEFT JOIN ctgov.calculated_values cv ON s.nct_id = cv.nct_id
{where_clause}
ORDER BY s.nct_id
"""

EXTRACT_DESIGNS_SQL = """
SELECT nct_id, allocation, intervention_model, observational_model,
       primary_purpose, time_perspective, masking, masking_description,
       intervention_model_description
FROM ctgov.designs WHERE nct_id = ANY(%s)
"""

EXTRACT_DESIGN_GROUPS_SQL = """
SELECT id, nct_id, group_type, title, description
FROM ctgov.design_groups WHERE nct_id = ANY(%s)
"""

EXTRACT_INTERVENTIONS_SQL = """
SELECT id, nct_id, intervention_type, name, description
FROM ctgov.interventions WHERE nct_id = ANY(%s)
"""

EXTRACT_DGI_SQL = """
SELECT nct_id, design_group_id, intervention_id
FROM ctgov.design_group_interventions WHERE nct_id = ANY(%s)
"""

EXTRACT_DESIGN_OUTCOMES_SQL = """
SELECT nct_id, outcome_type, measure, time_frame, description
FROM ctgov.design_outcomes WHERE nct_id = ANY(%s)
"""

EXTRACT_CONDITIONS_SQL = """
SELECT nct_id, name FROM ctgov.conditions WHERE nct_id = ANY(%s)
"""

EXTRACT_SPONSORS_SQL = """
SELECT nct_id, name, agency_class, lead_or_collaborator
FROM ctgov.sponsors WHERE nct_id = ANY(%s)
"""

EXTRACT_ID_INFO_SQL = """
SELECT nct_id, id_value, id_type
FROM ctgov.id_information WHERE nct_id = ANY(%s)
"""

EXTRACT_KEYWORDS_SQL = """
SELECT nct_id, name FROM ctgov.keywords WHERE nct_id = ANY(%s)
"""

EXTRACT_CONTACTS_SQL = """
SELECT nct_id, name, phone, email
FROM ctgov.central_contacts WHERE nct_id = ANY(%s)
"""

EXTRACT_OFFICIALS_SQL = """
SELECT nct_id, role, name, affiliation
FROM ctgov.overall_officials WHERE nct_id = ANY(%s)
"""

EXTRACT_DOCUMENTS_SQL = """
SELECT nct_id, document_type, document_date, url
FROM ctgov.provided_documents WHERE nct_id = ANY(%s)
"""

EXTRACT_FACILITIES_SQL = """
SELECT id, nct_id, status, name, city, state, zip, country
FROM ctgov.facilities WHERE nct_id = ANY(%s)
"""

EXTRACT_REFERENCES_SQL = """
SELECT nct_id, pmid, reference_type, citation
FROM ctgov.study_references WHERE nct_id = ANY(%s)
"""

# P2: Results tables
EXTRACT_RESULT_GROUPS_SQL = """
SELECT id, nct_id, ctgov_group_code, result_type, title, description
FROM ctgov.result_groups WHERE nct_id = ANY(%s)
"""

EXTRACT_RESULT_OUTCOMES_SQL = """
SELECT id, nct_id, outcome_type, title, description, time_frame,
       population, units, units_analyzed, dispersion_type, param_type
FROM ctgov.outcomes WHERE nct_id = ANY(%s)
"""

EXTRACT_OUTCOME_MEASUREMENTS_SQL = """
SELECT nct_id, outcome_id, result_group_id, ctgov_group_code,
       classification, category, title, description, units,
       param_type, param_value, param_value_num,
       dispersion_type, dispersion_value, dispersion_value_num,
       dispersion_lower_limit, dispersion_upper_limit, explanation_of_na
FROM ctgov.outcome_measurements WHERE nct_id = ANY(%s)
"""

EXTRACT_OUTCOME_ANALYSES_SQL = """
SELECT id, nct_id, outcome_id,
       non_inferiority_type, non_inferiority_description,
       param_type, param_value, dispersion_type, dispersion_value,
       p_value_modifier, p_value, ci_n_sides, ci_percent,
       ci_lower_limit, ci_upper_limit,
       method, method_description, estimate_description
FROM ctgov.outcome_analyses WHERE nct_id = ANY(%s)
"""

EXTRACT_OUTCOME_ANALYSIS_GROUPS_SQL = """
SELECT nct_id, outcome_analysis_id, result_group_id, ctgov_group_code
FROM ctgov.outcome_analysis_groups WHERE nct_id = ANY(%s)
"""

EXTRACT_REPORTED_EVENTS_SQL = """
SELECT nct_id, result_group_id, ctgov_group_code, time_frame,
       event_type, subjects_affected, subjects_at_risk,
       event_count, organ_system, adverse_event_term,
       frequency_threshold, vocab, assessment, description
FROM ctgov.reported_events WHERE nct_id = ANY(%s)
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_or_create_source(cur):
    cur.execute("SELECT source_id FROM ref.source WHERE source_code = 'CTGOV'")
    row = cur.fetchone()
    if row:
        return row[0]
    raise RuntimeError("CTGOV source not found in ref.source. Run init_db first.")


def group_by_nct(rows):
    result = {}
    for row in rows:
        nct = row['nct_id']
        result.setdefault(nct, []).append(row)
    return result


def upsert_organizations_bulk(cur, orgs):
    """
    orgs: set of tuples (name, org_type)
    Returns dict: {(name_stripped, org_type): organization_id}
    """
    cleaned_orgs = set()
    for name, org_type in orgs:
        if name:
            cleaned_orgs.add((name.strip(), name.strip().lower(), org_type))
            
    if not cleaned_orgs:
        return {}
        
    # Sort to prevent deadlocks
    sorted_orgs = sorted(list(cleaned_orgs), key=lambda x: (x[1], x[2] or ''))
    
    query = """
        INSERT INTO company.organization (organization_name, normalized_name, organization_type)
        VALUES %s
        ON CONFLICT (normalized_name, organization_type) DO UPDATE SET updated_at = now()
        RETURNING organization_name, organization_type, organization_id
    """
    results = execute_values(cur, query, sorted_orgs, fetch=True)
    return {(row[0], row[1]): row[2] for row in results}


def upsert_indications_bulk(cur, names):
    """
    names: set of indication names
    """
    cleaned_names = set()
    for n in names:
        if n:
            cleaned_names.add((n.strip(), n.strip().lower()))
            
    if not cleaned_names:
        return {}
        
    sorted_names = sorted(list(cleaned_names), key=lambda x: x[0])
    
    execute_values(cur, """
        INSERT INTO ref.indication (name, normalized_name)
        VALUES %s
        ON CONFLICT (name) DO NOTHING
    """, sorted_names)
    
    name_list = [row[0] for row in sorted_names]
    cur.execute("SELECT name, indication_id FROM ref.indication WHERE name = ANY(%s)", (name_list,))
    return {row[0]: row[1] for row in cur.fetchall()}


def upsert_persons_bulk(cur, names):
    cleaned = set(n.strip() for n in names if n)
    if not cleaned:
        return {}
    
    sorted_names = sorted(list(cleaned))
    values = [(n,) for n in sorted_names]
    
    execute_values(cur, """
        INSERT INTO company.person (full_name)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, values)
    
    cur.execute("SELECT full_name, person_id FROM company.person WHERE full_name = ANY(%s)", (sorted_names,))
    return {row[0]: row[1] for row in cur.fetchall()}


def upsert_sites_bulk(cur, sites):
    # sites: set of tuples (name, city, state, postal_code)
    cleaned = set()
    for s in sites:
        if s[0]: # name is required
            cleaned.add((s[0].strip(), s[1], s[2], s[3]))
            
    if not cleaned:
        return {}
        
    # Sort by all fields to ensure consistent lock acquisition
    sorted_sites = sorted(list(cleaned), key=lambda x: (x[0], x[1] or '', x[2] or '', x[3] or ''))
    
    execute_values(cur, """
        INSERT INTO clinical.site (site_name, city, state_province, postal_code)
        VALUES %s
        ON CONFLICT (site_name, city, state_province, postal_code) DO NOTHING
    """, sorted_sites)
    
    site_names = list(set(s[0] for s in sorted_sites))
    cur.execute("""
        SELECT site_name, city, state_province, postal_code, site_id 
        FROM clinical.site 
        WHERE site_name = ANY(%s)
    """, (site_names,))
    
    return {(row[0], row[1], row[2], row[3]): row[4] for row in cur.fetchall()}


def upsert_publications_bulk(cur, pubs):
    # pubs: set of tuples (pmid, citation)
    cleaned = set()
    for pmid, citation in pubs:
        if pmid:
            title = (citation or '')[:500]
            cleaned.add((pmid, title))
            
    if not cleaned:
        return {}
        
    sorted_pubs = sorted(list(cleaned), key=lambda x: x[0])
    
    execute_values(cur, """
        INSERT INTO scientific.publication (pmid, title)
        VALUES %s
        ON CONFLICT (pmid) DO NOTHING
    """, sorted_pubs)
    
    pmid_list = [row[0] for row in sorted_pubs]
    cur.execute("SELECT pmid, publication_id FROM scientific.publication WHERE pmid = ANY(%s)", (pmid_list,))
    return {row[0]: row[1] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Core: Upsert trial
# ---------------------------------------------------------------------------
def upsert_trials_bulk(cur, source_id, studies):
    """
    Returns dict: {nct_id: trial_id}
    """
    values = []
    for study in studies:
        nct_id = study['nct_id']
        enrollment = study['enrollment']
        etype = study['enrollment_type']
        planned = enrollment if etype and etype.upper() == 'ANTICIPATED' else None
        actual = enrollment if etype and etype.upper() == 'ACTUAL' else planned if not etype else None

        values.append((
            source_id, nct_id, study['brief_title'], study['official_title'], study['acronym'],
            study['brief_summary'], study['detailed_description'],
            study['study_type'], study['phase'], study['overall_status'], study['why_stopped'],
            study['were_results_reported'], study['has_dmc'],
            study['source_class'], study['sex'], study['minimum_age'], study['maximum_age'],
            study['healthy_volunteers'], study['is_adult'], study['is_child'], study['is_older_adult'],
            study['sampling_method'], study['population_description'],
            planned, actual, etype,
            study['number_of_facilities'],
            study['start_date'], study['start_date_type'], study['primary_completion_date'],
            study['completion_date'], study['completion_date_type'],
            study['plan_to_share_ipd'], study['ipd_time_frame'],
            study['ipd_access_criteria'], study['ipd_url'],
            study['last_update_submitted_date'], study['study_first_posted_date']
        ))

    if not values:
        return {}

    query = """
        INSERT INTO clinical.trial (
            source_id, primary_registry_id, title, official_title, acronym,
            brief_summary, detailed_description,
            study_type, phase, status, why_stopped,
            has_results, data_monitoring_committee,
            source_class, sex, minimum_age, maximum_age,
            healthy_volunteers, is_adult, is_child, is_older_adult,
            sampling_method, population_description,
            enrollment_planned, enrollment_actual, enrollment_type,
            number_of_sites,
            start_date, start_date_type, primary_completion_date,
            completion_date, completion_date_type,
            ipd_sharing_plan, ipd_time_frame, ipd_access_criteria, ipd_url,
            source_last_updated, first_seen_at
        ) VALUES %s
        ON CONFLICT (source_id, primary_registry_id) DO UPDATE SET
            title = EXCLUDED.title, official_title = EXCLUDED.official_title,
            acronym = EXCLUDED.acronym, brief_summary = EXCLUDED.brief_summary,
            detailed_description = EXCLUDED.detailed_description,
            study_type = EXCLUDED.study_type, phase = EXCLUDED.phase,
            status = EXCLUDED.status, why_stopped = EXCLUDED.why_stopped,
            has_results = EXCLUDED.has_results,
            data_monitoring_committee = EXCLUDED.data_monitoring_committee,
            source_class = EXCLUDED.source_class,
            sex = EXCLUDED.sex, minimum_age = EXCLUDED.minimum_age,
            maximum_age = EXCLUDED.maximum_age,
            healthy_volunteers = EXCLUDED.healthy_volunteers,
            is_adult = EXCLUDED.is_adult, is_child = EXCLUDED.is_child,
            is_older_adult = EXCLUDED.is_older_adult,
            sampling_method = EXCLUDED.sampling_method,
            population_description = EXCLUDED.population_description,
            enrollment_planned = EXCLUDED.enrollment_planned,
            enrollment_actual = EXCLUDED.enrollment_actual,
            enrollment_type = EXCLUDED.enrollment_type,
            number_of_sites = EXCLUDED.number_of_sites,
            start_date = EXCLUDED.start_date, start_date_type = EXCLUDED.start_date_type,
            primary_completion_date = EXCLUDED.primary_completion_date,
            completion_date = EXCLUDED.completion_date,
            completion_date_type = EXCLUDED.completion_date_type,
            ipd_sharing_plan = EXCLUDED.ipd_sharing_plan,
            ipd_time_frame = EXCLUDED.ipd_time_frame,
            ipd_access_criteria = EXCLUDED.ipd_access_criteria,
            ipd_url = EXCLUDED.ipd_url,
            source_last_updated = EXCLUDED.source_last_updated,
            updated_at = now()
        RETURNING primary_registry_id, trial_id
    """
    results = execute_values(cur, query, values, fetch=True)
    return {row[0]: row[1] for row in results}


# ---------------------------------------------------------------------------
# Child tables
# ---------------------------------------------------------------------------
def load_design_attributes_bulk(cur, trial_map, designs_dict):
    fields = [
        ('allocation', 'allocation'), ('intervention_model', 'intervention_model'),
        ('observational_model', 'observational_model'), ('primary_purpose', 'primary_purpose'),
        ('time_perspective', 'time_perspective'), ('masking', 'masking'),
    ]
    values = []
    for nct, trial_id in trial_map.items():
        for d in designs_dict.get(nct, []):
            for attr_type, col_name in fields:
                val = d[col_name]
                if val:
                    values.append((trial_id, attr_type, val))
    if values:
        execute_values(cur, """
            INSERT INTO clinical.design_attribute (trial_id, attribute_type, attribute_value)
            VALUES %s ON CONFLICT DO NOTHING
        """, values)


def load_arms_bulk(cur, trial_map, groups_dict):
    values = []
    for nct, trial_id in trial_map.items():
        for g in groups_dict.get(nct, []):
            values.append((trial_id, str(g['id']), g['title'], g['group_type'], g['description']))
            
    if not values:
        return {}
        
    query = """
        INSERT INTO clinical.arm (trial_id, arm_code, arm_title, arm_type, description)
        VALUES %s
        ON CONFLICT (trial_id, arm_code) DO UPDATE SET
            arm_title = EXCLUDED.arm_title, arm_type = EXCLUDED.arm_type,
            description = EXCLUDED.description
        RETURNING arm_code, arm_id
    """
    results = execute_values(cur, query, values, fetch=True)
    return {int(row[0]): row[1] for row in results}


def load_interventions_bulk(cur, trial_map, interventions_dict):
    values = []
    for nct, trial_id in trial_map.items():
        for i in interventions_dict.get(nct, []):
            values.append((trial_id, i['name'], i['intervention_type'], i['description'], i['id']))
            
    if not values:
        return {}
        
    # Deduplicate by unique constraint: (trial_id, intervention_name, intervention_type)
    dedup = {}
    for v in values:
        dedup[(v[0], v[1], v[2])] = v
        
    query = """
        INSERT INTO clinical.intervention (trial_id, intervention_name, intervention_type, description)
        VALUES %s
        ON CONFLICT (trial_id, intervention_name, intervention_type) DO UPDATE SET
            description = EXCLUDED.description
        RETURNING trial_id, intervention_name, intervention_type, intervention_id
    """
    insert_vals = [v[:-1] for v in dedup.values()]
    results = execute_values(cur, query, insert_vals, fetch=True)
    db_map = {(row[0], row[1], row[2]): row[3] for row in results}
    
    aact_to_uuid = {}
    for v in values:
        uuid = db_map.get((v[0], v[1], v[2]))
        if uuid:
            aact_to_uuid[v[4]] = uuid
    return aact_to_uuid


def load_arm_interventions_bulk(cur, dgi_dict, arm_map, intv_map):
    values = []
    for nct, rows in dgi_dict.items():
        for row in rows:
            arm_uuid = arm_map.get(row['design_group_id'])
            intv_uuid = intv_map.get(row['intervention_id'])
            if arm_uuid and intv_uuid:
                values.append((arm_uuid, intv_uuid))
    if values:
        execute_values(cur, """
            INSERT INTO clinical.arm_intervention (arm_id, intervention_id)
            VALUES %s ON CONFLICT DO NOTHING
        """, values)


def load_outcomes_bulk(cur, trial_map, outcomes_dict):
    values = []
    for nct, trial_id in trial_map.items():
        seq = 0
        for o in outcomes_dict.get(nct, []):
            seq += 1
            values.append((trial_id, o['outcome_type'], o['measure'], o['time_frame'], o['description'], seq))
    if values:
        execute_values(cur, """
            INSERT INTO clinical.outcome (trial_id, outcome_type, measure, time_frame, description, sequence_no)
            VALUES %s ON CONFLICT DO NOTHING
        """, values)


def load_conditions_bulk(cur, trial_map, conditions_dict, indication_map):
    values = []
    for nct, trial_id in trial_map.items():
        for c in conditions_dict.get(nct, []):
            if c['name']:
                ind_id = indication_map.get(c['name'].strip())
                if ind_id:
                    values.append((trial_id, ind_id, False))
    if values:
        execute_values(cur, """
            INSERT INTO clinical.trial_indication (trial_id, indication_id, is_primary)
            VALUES %s ON CONFLICT DO NOTHING
        """, values)


def load_sponsors_bulk(cur, trial_map, sponsors_dict, org_map):
    values = []
    for nct, trial_id in trial_map.items():
        for sp in sponsors_dict.get(nct, []):
            name = sp['name'].strip() if sp['name'] else None
            org_type = sp.get('agency_class')
            if name:
                org_id = org_map.get((name, org_type))
                if org_id:
                    role = (sp.get('lead_or_collaborator') or 'sponsor').lower()
                    is_lead = role == 'lead'
                    values.append((trial_id, org_id, role, is_lead))
    if values:
        execute_values(cur, """
            INSERT INTO clinical.trial_sponsor (trial_id, organization_id, sponsor_role, is_lead)
            VALUES %s ON CONFLICT DO NOTHING
        """, values)


def load_secondary_ids_bulk(cur, trial_map, ids_dict):
    values = []
    for nct, trial_id in trial_map.items():
        for i in ids_dict.get(nct, []):
            if i['id_value']:
                values.append((trial_id, i['id_value'], i['id_type'] or 'other'))
    if values:
        execute_values(cur, """
            INSERT INTO clinical.secondary_identifier (trial_id, identifier_value, identifier_type)
            VALUES %s ON CONFLICT DO NOTHING
        """, values)


def load_keywords_bulk(cur, trial_map, keywords_dict):
    values = []
    for nct, trial_id in trial_map.items():
        for k in keywords_dict.get(nct, []):
            if k['name']:
                values.append((trial_id, k['name']))
    if values:
        execute_values(cur, """
            INSERT INTO clinical.subject_tag (trial_id, subject_tag)
            VALUES %s ON CONFLICT DO NOTHING
        """, values)


def load_contacts_bulk(cur, trial_map, contacts_dict):
    values = []
    for nct, trial_id in trial_map.items():
        for c in contacts_dict.get(nct, []):
            values.append((trial_id, c['name'], c['phone'], c['email']))
    if values:
        execute_values(cur, """
            INSERT INTO clinical.contact (trial_id, contact_name, phone, email)
            VALUES %s
        """, values)


def load_officials_bulk(cur, trial_map, officials_dict, person_map, org_map):
    values = []
    for nct, trial_id in trial_map.items():
        for o in officials_dict.get(nct, []):
            name = o['name'].strip() if o['name'] else None
            if name:
                person_id = person_map.get(name)
                if person_id:
                    org_name = o['affiliation'].strip() if o['affiliation'] else None
                    org_id = org_map.get((org_name, None)) if org_name else None
                    is_pi = (o['role'] or '').upper() == 'PRINCIPAL_INVESTIGATOR'
                    values.append((trial_id, person_id, org_id, o['role'], is_pi))
    if values:
        execute_values(cur, """
            INSERT INTO clinical.trial_investigator (trial_id, person_id, organization_id, role, is_principal)
            VALUES %s ON CONFLICT DO NOTHING
        """, values)


def load_documents_bulk(cur, trial_map, source_id, docs_dict):
    values = []
    for nct, trial_id in trial_map.items():
        for d in docs_dict.get(nct, []):
            values.append((trial_id, d['document_type'], d['url'], d['document_date'], source_id))
    if values:
        execute_values(cur, """
            INSERT INTO clinical.document (trial_id, document_type, document_url, document_date, source_id)
            VALUES %s ON CONFLICT DO NOTHING
        """, values)


def load_facilities_bulk(cur, trial_map, facilities_dict, site_map):
    values = []
    for nct, trial_id in trial_map.items():
        for f in facilities_dict.get(nct, []):
            name = f['name'].strip() if f['name'] else None
            if name:
                site_id = site_map.get((name, f['city'], f['state'], f['zip']))
                if site_id:
                    values.append((trial_id, site_id, f['status'], f['status']))
    if values:
        execute_values(cur, """
            INSERT INTO clinical.trial_site (trial_id, site_id, recruitment_status, source_status)
            VALUES %s ON CONFLICT DO NOTHING
        """, values)


def load_references_bulk(cur, trial_map, refs_dict, pub_map):
    values = []
    for nct, trial_id in trial_map.items():
        for r in refs_dict.get(nct, []):
            pmid = r['pmid']
            if pmid:
                pub_id = pub_map.get(pmid)
                if pub_id:
                    rel_type = (r['reference_type'] or 'related').lower()
                    values.append((trial_id, pub_id, rel_type))
    if values:
        execute_values(cur, """
            INSERT INTO clinical.trial_publication (trial_id, publication_id, relationship_type)
            VALUES %s ON CONFLICT DO NOTHING
        """, values)


def load_eligibility_criteria_bulk(cur, trial_map, studies):
    # studies is the full list of study dictionaries
    values = []
    for study in studies:
        nct = study['nct_id']
        trial_id = trial_map.get(nct)
        criteria_text = study.get('eligibility_criteria')
        if trial_id and criteria_text:
            values.append((trial_id, 'full_text', criteria_text, 1))
            
    if values:
        # For bulk, we can delete all criteria for the trials in the batch first
        trial_ids = [v[0] for v in values]
        cur.execute("DELETE FROM clinical.eligibility_criterion WHERE trial_id = ANY(%s::uuid[])", (trial_ids,))
        execute_values(cur, """
            INSERT INTO clinical.eligibility_criterion (trial_id, criterion_type, criterion_text, sequence_no)
            VALUES %s ON CONFLICT DO NOTHING
        """, values)


# ---------------------------------------------------------------------------
# P2: Results loading
# ---------------------------------------------------------------------------
def load_results_bulk(cur, trial_map, child_data):
    """Load all P2 results data for the entire batch of trials."""
    
    # 1. Result Groups
    rg_values = []
    for nct, trial_id in trial_map.items():
        for rg in child_data['result_groups'].get(nct, []):
            rg_values.append((trial_id, rg['ctgov_group_code'], rg['result_type'], rg['title'] or 'Untitled', rg['description'], rg['id']))
            
    rg_map = {}
    if rg_values:
        # Deduplicate by unique constraint: (trial_id, group_code, result_type)
        dedup_rg = {}
        for v in rg_values:
            dedup_rg[(v[0], v[1], v[2])] = v
            
        query = """
            INSERT INTO clinical.result_group (trial_id, group_code, result_type, title, description)
            VALUES %s
            ON CONFLICT (trial_id, group_code, result_type) DO UPDATE SET
                title = EXCLUDED.title, description = EXCLUDED.description
            RETURNING trial_id, group_code, result_type, result_group_id
        """
        insert_rg = [v[:-1] for v in dedup_rg.values()]
        results = execute_values(cur, query, insert_rg, fetch=True)
        db_rg_map = {(row[0], row[1], row[2]): row[3] for row in results}
        for v in rg_values:
            uuid = db_rg_map.get((v[0], v[1], v[2]))
            if uuid:
                rg_map[v[5]] = uuid

    # 2. Outcomes
    outcome_values = []
    for nct, trial_id in trial_map.items():
        for o in child_data['result_outcomes'].get(nct, []):
            outcome_values.append((trial_id, o['outcome_type'], o['title'], o['time_frame'], o['description'], None, o['id']))
            
    outcome_map = {}
    if outcome_values:
        # Deduplicate by unique constraint: (trial_id, outcome_type, measure)
        dedup_outcomes = {}
        for v in outcome_values:
            dedup_outcomes[(v[0], v[1], v[2])] = v
            
        query = """
            INSERT INTO clinical.outcome (trial_id, outcome_type, measure, time_frame, description, sequence_no)
            VALUES %s
            ON CONFLICT (trial_id, outcome_type, measure) DO UPDATE SET
                time_frame = EXCLUDED.time_frame
            RETURNING trial_id, outcome_type, measure, outcome_id
        """
        insert_outcomes = [v[:-1] for v in dedup_outcomes.values()]
        results = execute_values(cur, query, insert_outcomes, fetch=True)
        db_outcome_map = {(row[0], row[1], row[2]): row[3] for row in results}
        for v in outcome_values:
            uuid = db_outcome_map.get((v[0], v[1], v[2]))
            if uuid:
                outcome_map[v[6]] = uuid
                
    # 3. Outcome Measurements
    om_values = []
    for nct, trial_id in trial_map.items():
        for m in child_data['outcome_measurements'].get(nct, []):
            oid = outcome_map.get(m['outcome_id'])
            rgid = rg_map.get(m['result_group_id'])
            if oid and rgid:
                om_values.append((oid, rgid, m['classification'], m['category'],
                      m['title'], m['description'], m['units'],
                      m['param_type'], m['param_value'], m['param_value_num'],
                      m['dispersion_type'], m['dispersion_value'],
                      m['dispersion_value_num'], m['dispersion_lower_limit'],
                      m['dispersion_upper_limit'], m['explanation_of_na']))
    if om_values:
        execute_values(cur, """
            INSERT INTO clinical.outcome_measurement
                (outcome_id, result_group_id, classification, category,
                 title, description, units, param_type, param_value,
                 param_value_num, dispersion_type, dispersion_value,
                 dispersion_value_num, dispersion_lower_limit,
                 dispersion_upper_limit, explanation_of_na)
            VALUES %s
        """, om_values)

    # 4. Outcome Analyses (per-row because it lacks a unique constraint to safely map back)
    analysis_map = {}
    for nct, trial_id in trial_map.items():
        for a in child_data['outcome_analyses'].get(nct, []):
            oid = outcome_map.get(a['outcome_id'])
            if not oid:
                continue
            p_val = str(a['p_value']) if a['p_value'] is not None else None
            cur.execute("""
                INSERT INTO clinical.outcome_analysis
                    (outcome_id, non_inferiority_type, non_inferiority_description,
                     param_type, param_value, dispersion_type, dispersion_value,
                     p_value, p_value_modifier, ci_n_sides, ci_percent,
                     ci_lower_limit, ci_upper_limit, method, method_description,
                     estimate_description)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING analysis_id
            """, (oid, a['non_inferiority_type'], a['non_inferiority_description'],
                  a['param_type'], a['param_value'], a['dispersion_type'],
                  a['dispersion_value'], p_val, a['p_value_modifier'],
                  a['ci_n_sides'], a['ci_percent'], a['ci_lower_limit'],
                  a['ci_upper_limit'], a['method'], a['method_description'],
                  a['estimate_description']))
            analysis_map[a['id']] = cur.fetchone()[0]

    # 5. Outcome analysis ↔ result group links
    oag_values = []
    for nct, trial_id in trial_map.items():
        for ag in child_data['outcome_analysis_groups'].get(nct, []):
            aid = analysis_map.get(ag['outcome_analysis_id'])
            rgid = rg_map.get(ag['result_group_id'])
            if aid and rgid:
                oag_values.append((aid, rgid))
    if oag_values:
        execute_values(cur, """
            INSERT INTO clinical.outcome_analysis_group (analysis_id, result_group_id)
            VALUES %s ON CONFLICT DO NOTHING
        """, oag_values)

    # 6. Adverse events (reported_events)
    re_values = []
    for nct, trial_id in trial_map.items():
        for ev in child_data['reported_events'].get(nct, []):
            rgid = rg_map.get(ev['result_group_id'])
            if not ev.get('adverse_event_term'):
                continue
            re_values.append((trial_id, rgid, ev['event_type'], ev['organ_system'],
                  ev['adverse_event_term'], ev['subjects_affected'],
                  ev['subjects_at_risk'], ev['event_count'],
                  ev['frequency_threshold'], ev['time_frame'],
                  ev['description'], ev['vocab'], ev['assessment']))
    if re_values:
        execute_values(cur, """
            INSERT INTO clinical.adverse_event
                (trial_id, result_group_id, event_type, organ_system,
                 adverse_event_term, subjects_affected, subjects_at_risk,
                 event_count, frequency_threshold, time_frame, description,
                 vocabulary, assessment)
            VALUES %s
        """, re_values)


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------
def fetch_child_data(src_cur, nct_ids):
    """Fetch all child table data for a batch of nct_ids."""
    data = {}
    queries = {
        'designs': EXTRACT_DESIGNS_SQL,
        'design_groups': EXTRACT_DESIGN_GROUPS_SQL,
        'interventions': EXTRACT_INTERVENTIONS_SQL,
        'dgi': EXTRACT_DGI_SQL,
        'design_outcomes': EXTRACT_DESIGN_OUTCOMES_SQL,
        'conditions': EXTRACT_CONDITIONS_SQL,
        'sponsors': EXTRACT_SPONSORS_SQL,
        'id_info': EXTRACT_ID_INFO_SQL,
        'keywords': EXTRACT_KEYWORDS_SQL,
        'contacts': EXTRACT_CONTACTS_SQL,
        'officials': EXTRACT_OFFICIALS_SQL,
        'documents': EXTRACT_DOCUMENTS_SQL,
        'facilities': EXTRACT_FACILITIES_SQL,
        'references': EXTRACT_REFERENCES_SQL,
        # P2: Results
        'result_groups': EXTRACT_RESULT_GROUPS_SQL,
        'result_outcomes': EXTRACT_RESULT_OUTCOMES_SQL,
        'outcome_measurements': EXTRACT_OUTCOME_MEASUREMENTS_SQL,
        'outcome_analyses': EXTRACT_OUTCOME_ANALYSES_SQL,
        'outcome_analysis_groups': EXTRACT_OUTCOME_ANALYSIS_GROUPS_SQL,
        'reported_events': EXTRACT_REPORTED_EVENTS_SQL,
    }
    for key, sql in queries.items():
        src_cur.execute(sql, (nct_ids,))
        cols = [desc[0] for desc in src_cur.description]
        rows = [dict(zip(cols, row)) for row in src_cur.fetchall()]
        data[key] = group_by_nct(rows)
    return data# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------
def run_bulk_pipeline(dst_cur, source_id, studies, child_data):
    """Executes the complete bulk-insert dependency tree for a set of studies."""
    if not studies:
        return 0

    # 1. Collect all lookup data across the given studies
    orgs = set()
    inds = set()
    persons = set()
    sites = set()
    pubs = set()
    
    nct_ids = [s['nct_id'] for s in studies]
    
    for nct in nct_ids:
        for sp in child_data['sponsors'].get(nct, []):
            if sp.get('name'): orgs.add((sp['name'], sp.get('agency_class')))
        for c in child_data['conditions'].get(nct, []):
            if c.get('name'): inds.add(c['name'])
        for o in child_data['officials'].get(nct, []):
            if o.get('name'): persons.add(o['name'])
            if o.get('affiliation'): orgs.add((o['affiliation'], None))
        for f in child_data['facilities'].get(nct, []):
            if f.get('name'): sites.add((f['name'], f.get('city'), f.get('state'), f.get('zip')))
        for r in child_data['references'].get(nct, []):
            if r.get('pmid'): pubs.add((r['pmid'], r.get('citation')))

    # 2. Bulk Upsert Lookups
    org_map = upsert_organizations_bulk(dst_cur, orgs)
    ind_map = upsert_indications_bulk(dst_cur, inds)
    person_map = upsert_persons_bulk(dst_cur, persons)
    site_map = upsert_sites_bulk(dst_cur, sites)
    pub_map = upsert_publications_bulk(dst_cur, pubs)

    # 3. Bulk Upsert Trials
    trial_map = upsert_trials_bulk(dst_cur, source_id, studies)
    if not trial_map:
        return 0

    # 4. Bulk Insert Simple Children
    load_design_attributes_bulk(dst_cur, trial_map, child_data['designs'])
    load_conditions_bulk(dst_cur, trial_map, child_data['conditions'], ind_map)
    load_sponsors_bulk(dst_cur, trial_map, child_data['sponsors'], org_map)
    load_secondary_ids_bulk(dst_cur, trial_map, child_data['id_info'])
    load_keywords_bulk(dst_cur, trial_map, child_data['keywords'])
    load_contacts_bulk(dst_cur, trial_map, child_data['contacts'])
    load_officials_bulk(dst_cur, trial_map, child_data['officials'], person_map, org_map)
    load_documents_bulk(dst_cur, trial_map, source_id, child_data['documents'])
    load_facilities_bulk(dst_cur, trial_map, child_data['facilities'], site_map)
    load_references_bulk(dst_cur, trial_map, child_data['references'], pub_map)
    load_eligibility_criteria_bulk(dst_cur, trial_map, studies)
    load_outcomes_bulk(dst_cur, trial_map, child_data['design_outcomes'])

    # 5. Bulk Insert Complex P1 Entities
    arm_map = load_arms_bulk(dst_cur, trial_map, child_data['design_groups'])
    intv_map = load_interventions_bulk(dst_cur, trial_map, child_data['interventions'])
    load_arm_interventions_bulk(dst_cur, child_data['dgi'], arm_map, intv_map)

    # 6. Bulk Insert P2 Results
    load_results_bulk(dst_cur, trial_map, child_data)
    
    return len(trial_map)


# ---------------------------------------------------------------------------
# Worker: Process a single batch
# ---------------------------------------------------------------------------
def process_single_batch(nct_ids, source_id, retry_errors=False):
    """Worker function to process one batch of NCT IDs."""
    import threading
    thread_name = threading.current_thread().name
    
    src_conn = psycopg2.connect(**CTGOV_DB_CONFIG)
    src_conn.set_session(readonly=True)
    dst_conn = psycopg2.connect(**UNIFIED_DB_CONFIG)
    
    src_cur = src_conn.cursor()
    dst_cur = dst_conn.cursor()
    
    # Performance Optimization: Asynchronous Commit for massive speed
    dst_cur.execute("SET synchronous_commit = off")
    
    try:
        # 1. Batch read: Fetch full study data from AACT
        where_clause = f"WHERE s.nct_id = ANY(%s)"
        full_sql = EXTRACT_STUDIES_SQL.format(where_clause=where_clause)
        src_cur.execute(full_sql, (nct_ids,))
        cols = [desc[0] for desc in src_cur.description]
        studies = [dict(zip(cols, row)) for row in src_cur.fetchall()]

        # 2. Batch read: Fetch all child data at once
        child_src_conn = psycopg2.connect(**CTGOV_DB_CONFIG)
        child_src_conn.set_session(readonly=True)
        child_src_cur = child_src_conn.cursor()
        child_data = fetch_child_data(child_src_cur, nct_ids)
        child_src_cur.close()
        child_src_conn.close()
        
        # 3. Try fast bulk execution for the entire batch
        batch_loaded = 0
        try:
            batch_loaded = run_bulk_pipeline(dst_cur, source_id, studies, child_data)
            
            dst_cur.execute("""
                UPDATE ingest.sync_log SET status = 'completed', processed_at = now(), error_message = NULL
                WHERE source_code = 'CTGOV' AND source_record_id = ANY(%s)
            """, (nct_ids,))
            dst_conn.commit()
            print(f"  [{thread_name}] Bulk batch complete: {batch_loaded}/{len(nct_ids)} loaded.")
            return batch_loaded
            
        except Exception as e_bulk:
            # Batch failed (likely constraint violation). Rollback and use fallback mode.
            dst_conn.rollback()
            
            # 4. Fallback to row-by-row (batch size of 1)
            for study in studies:
                nct = study['nct_id']
                try:
                    run_bulk_pipeline(dst_cur, source_id, [study], child_data)
                    dst_cur.execute("""
                        UPDATE ingest.sync_log SET status = 'completed', processed_at = now(), error_message = NULL
                        WHERE source_code = 'CTGOV' AND source_record_id = %s
                    """, (nct,))
                    dst_conn.commit()
                    batch_loaded += 1
                except Exception as e_single:
                    dst_conn.rollback()
                    error_msg = str(e_single)[:500]
                    dst_cur.execute("""
                        UPDATE ingest.sync_log SET status = 'error', error_message = %s, last_attempt_at = now()
                        WHERE source_code = 'CTGOV' AND source_record_id = %s
                    """, (error_msg, nct))
                    dst_conn.commit()
                    print(f"  [{thread_name}] ERROR on {nct}: {error_msg}")
                    
            print(f"  [{thread_name}] Fallback batch complete: {batch_loaded}/{len(nct_ids)} loaded.")
            return batch_loaded

    finally:
        src_cur.close()
        src_conn.close()
        dst_cur.close()
        dst_conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(batch_size=500, limit=None, retry_errors=False, num_workers=1):
    print("=" * 60)
    print(f"CTGov → Unified Database Ingestion (Parallel: {num_workers} workers)")
    print("=" * 60)

    # 1. Setup Control Connection
    dst_conn = psycopg2.connect(**UNIFIED_DB_CONFIG)
    dst_cur = dst_conn.cursor()
    source_id = get_or_create_source(dst_cur)
    print(f"Source ID (CTGOV): {source_id}")

    # 2. Startup Cleanup: Reset 'processing' records to 'pending'
    print("Checking for interrupted records...")
    dst_cur.execute("""
        UPDATE ingest.sync_log SET status = 'pending'
        WHERE source_code = 'CTGOV' AND status = 'processing'
    """)
    if dst_cur.rowcount > 0:
        print(f"  Reset {dst_cur.rowcount} 'processing' records back to 'pending'.")
    dst_conn.commit()

    total_processed = 0
    statuses_to_fetch = "('pending', 'error')" if retry_errors else "('pending')"
    
    # 3. Thread Pool execution
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        
        while True:
            # Check if we've reached the user-specified limit
            current_limit = batch_size
            if limit and (total_processed + batch_size > limit):
                current_limit = limit - total_processed
            
            if current_limit <= 0:
                break

            # A. Fetch next batch of pending/error IDs from sync_log
            dst_cur.execute(f"""
                SELECT source_record_id FROM ingest.sync_log
                WHERE source_code = 'CTGOV' AND status IN {statuses_to_fetch}
                ORDER BY created_at ASC
                LIMIT %s
            """, (current_limit,))
            
            pending_rows = dst_cur.fetchall()
            if not pending_rows:
                break
                
            nct_ids = [r[0] for r in pending_rows]
            
            # B. Mark them as 'processing' immediately and commit
            dst_cur.execute("""
                UPDATE ingest.sync_log SET status = 'processing', last_attempt_at = now()
                WHERE source_code = 'CTGOV' AND source_record_id = ANY(%s)
            """, (nct_ids,))
            dst_conn.commit()

            # C. Dispatch to worker
            futures.append(executor.submit(process_single_batch, nct_ids, source_id, retry_errors))
            total_processed += len(nct_ids)
            print(f"  Dispatched batch of {len(nct_ids)}... Total Dispatched: {total_processed}")

            # D. Maintenance: Remove completed futures to free memory
            for f in [f for f in futures if f.done()]:
                futures.remove(f)

            # E. Throttling: If we have many futures, wait for some to finish
            if len(futures) >= num_workers * 2:
                # print("  Waiting for worker capacity...")
                for f in as_completed(futures):
                    futures.remove(f)
                    break

        # Wait for remaining workers
        if futures:
            print(f"\nAll batches dispatched ({len(futures)} still active). Waiting for workers to finish...")
            for f in as_completed(futures):
                pass

    dst_cur.close()
    dst_conn.close()

    print(f"\n{'=' * 60}")
    print(f"Run complete. Total records handled: {total_processed}")
    print(f"{'=' * 60}")


def main():
    parser = argparse.ArgumentParser(description="Ingest CTGov (AACT) data into the Unified Clinical Database using Parallel Workers.")
    parser.add_argument("--batch-size", type=int, default=500, help="Studies per batch (default: 500)")
    parser.add_argument("--limit", type=int, default=None, help="Max studies to process in this run (default: all pending)")
    parser.add_argument("--retry", action="store_true", help="Retry records that are currently in 'error' status")
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel worker threads (default: 1)")
    args = parser.parse_args()
    run(batch_size=args.batch_size, limit=args.limit, retry_errors=args.retry, num_workers=args.workers)


if __name__ == "__main__":
    main()
