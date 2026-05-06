"""
CTGov (AACT) → Unified Clinical Database Ingestion Script
Phase: P0 - Core trial metadata, design, arms, interventions, outcomes, conditions, sponsors
"""
import os
import sys
import time
import random
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from psycopg2.errors import DeadlockDetected
import psycopg2.extras
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


def upsert_organization(cur, name, org_type=None):
    if not name:
        return None
    norm = name.strip().lower()
    cur.execute("""
        INSERT INTO company.organization (organization_name, normalized_name, organization_type)
        VALUES (%s, %s, %s)
        ON CONFLICT (normalized_name, organization_type) DO UPDATE SET updated_at = now()
        RETURNING organization_id
    """, (name.strip(), norm, org_type))
    return cur.fetchone()[0]


def upsert_indication(cur, name):
    if not name:
        return None
    norm = name.strip().lower()
    cur.execute("""
        INSERT INTO ref.indication (name, normalized_name)
        VALUES (%s, %s)
        ON CONFLICT (name) DO NOTHING
    """, (name.strip(), norm))
    cur.execute("SELECT indication_id FROM ref.indication WHERE name = %s", (name.strip(),))
    return cur.fetchone()[0]


def upsert_person(cur, full_name):
    if not full_name:
        return None
    cur.execute("""
        INSERT INTO company.person (full_name)
        VALUES (%s)
        ON CONFLICT DO NOTHING
    """, (full_name.strip(),))
    cur.execute("SELECT person_id FROM company.person WHERE full_name = %s", (full_name.strip(),))
    row = cur.fetchone()
    return row[0] if row else None


def upsert_site(cur, name, city, state, postal_code):
    if not name:
        return None
    cur.execute("""
        INSERT INTO clinical.site (site_name, city, state_province, postal_code)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (site_name, city, state_province, postal_code) DO NOTHING
    """, (name.strip(), city, state, postal_code))
    cur.execute("""
        SELECT site_id FROM clinical.site
        WHERE site_name = %s AND city IS NOT DISTINCT FROM %s
          AND state_province IS NOT DISTINCT FROM %s AND postal_code IS NOT DISTINCT FROM %s
    """, (name.strip(), city, state, postal_code))
    row = cur.fetchone()
    return row[0] if row else None


def upsert_publication(cur, pmid, citation):
    if not pmid:
        return None
    title = (citation or '')[:500]
    cur.execute("""
        INSERT INTO scientific.publication (pmid, title)
        VALUES (%s, %s)
        ON CONFLICT (pmid) DO NOTHING
    """, (pmid, title))
    cur.execute("SELECT publication_id FROM scientific.publication WHERE pmid = %s", (pmid,))
    row = cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Core: Upsert trial
# ---------------------------------------------------------------------------
def upsert_trial(cur, source_id, study):
    nct_id = study['nct_id']
    enrollment = study['enrollment']
    etype = study['enrollment_type']
    planned = enrollment if etype and etype.upper() == 'ANTICIPATED' else None
    actual = enrollment if etype and etype.upper() == 'ACTUAL' else planned if not etype else None

    cur.execute("""
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
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
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
        RETURNING trial_id
    """, (
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
        study['last_update_submitted_date'], study['study_first_posted_date'],
    ))
    return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Child tables
# ---------------------------------------------------------------------------
def load_design_attributes(cur, trial_id, designs):
    fields = [
        ('allocation', 'allocation'), ('intervention_model', 'intervention_model'),
        ('observational_model', 'observational_model'), ('primary_purpose', 'primary_purpose'),
        ('time_perspective', 'time_perspective'), ('masking', 'masking'),
    ]
    for d in designs:
        for attr_type, col_name in fields:
            val = d[col_name]
            if val:
                cur.execute("""
                    INSERT INTO clinical.design_attribute (trial_id, attribute_type, attribute_value)
                    VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                """, (trial_id, attr_type, val))


def load_arms(cur, trial_id, groups):
    aact_to_uuid = {}
    for g in groups:
        aact_id = g['id']
        cur.execute("""
            INSERT INTO clinical.arm (trial_id, arm_code, arm_title, arm_type, description)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (trial_id, arm_code) DO UPDATE SET
                arm_title = EXCLUDED.arm_title, arm_type = EXCLUDED.arm_type,
                description = EXCLUDED.description
            RETURNING arm_id
        """, (trial_id, str(aact_id), g['title'], g['group_type'], g['description']))
        aact_to_uuid[aact_id] = cur.fetchone()[0]
    return aact_to_uuid


def load_interventions(cur, trial_id, interventions):
    aact_to_uuid = {}
    for i in interventions:
        aact_id = i['id']
        cur.execute("""
            INSERT INTO clinical.intervention (trial_id, intervention_name, intervention_type, description)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (trial_id, intervention_name, intervention_type) DO UPDATE SET
                description = EXCLUDED.description
            RETURNING intervention_id
        """, (trial_id, i['name'], i['intervention_type'], i['description']))
        aact_to_uuid[aact_id] = cur.fetchone()[0]
    return aact_to_uuid


def load_arm_interventions(cur, dgi_rows, arm_map, intv_map):
    for row in dgi_rows:
        arm_uuid = arm_map.get(row['design_group_id'])
        intv_uuid = intv_map.get(row['intervention_id'])
        if arm_uuid and intv_uuid:
            cur.execute("""
                INSERT INTO clinical.arm_intervention (arm_id, intervention_id)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """, (arm_uuid, intv_uuid))


def load_outcomes(cur, trial_id, outcomes):
    seq = 0
    for o in outcomes:
        seq += 1
        cur.execute("""
            INSERT INTO clinical.outcome (trial_id, outcome_type, measure, time_frame, description, sequence_no)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
        """, (trial_id, o['outcome_type'], o['measure'], o['time_frame'], o['description'], seq))


def load_conditions(cur, trial_id, conditions):
    for c in conditions:
        ind_id = upsert_indication(cur, c['name'])
        if ind_id:
            cur.execute("""
                INSERT INTO clinical.trial_indication (trial_id, indication_id, is_primary)
                VALUES (%s, %s, false) ON CONFLICT DO NOTHING
            """, (trial_id, ind_id))


def load_sponsors(cur, trial_id, sponsors):
    for sp in sponsors:
        org_id = upsert_organization(cur, sp['name'], sp.get('agency_class'))
        if org_id:
            role = (sp.get('lead_or_collaborator') or 'sponsor').lower()
            is_lead = role == 'lead'
            cur.execute("""
                INSERT INTO clinical.trial_sponsor (trial_id, organization_id, sponsor_role, is_lead)
                VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
            """, (trial_id, org_id, role, is_lead))


def load_secondary_ids(cur, trial_id, ids):
    for i in ids:
        if i['id_value']:
            cur.execute("""
                INSERT INTO clinical.secondary_identifier (trial_id, identifier_value, identifier_type)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
            """, (trial_id, i['id_value'], i['id_type'] or 'other'))


def load_keywords(cur, trial_id, keywords):
    for k in keywords:
        if k['name']:
            cur.execute("""
                INSERT INTO clinical.subject_tag (trial_id, subject_tag)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """, (trial_id, k['name']))


def load_contacts(cur, trial_id, contacts):
    for c in contacts:
        cur.execute("""
            INSERT INTO clinical.contact (trial_id, contact_name, phone, email)
            VALUES (%s, %s, %s, %s)
        """, (trial_id, c['name'], c['phone'], c['email']))


def load_officials(cur, trial_id, officials):
    for o in officials:
        person_id = upsert_person(cur, o['name'])
        if person_id:
            org_id = upsert_organization(cur, o['affiliation']) if o['affiliation'] else None
            is_pi = (o['role'] or '').upper() == 'PRINCIPAL_INVESTIGATOR'
            cur.execute("""
                INSERT INTO clinical.trial_investigator (trial_id, person_id, organization_id, role, is_principal)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
            """, (trial_id, person_id, org_id, o['role'], is_pi))


def load_documents(cur, trial_id, source_id, docs):
    for d in docs:
        cur.execute("""
            INSERT INTO clinical.document (trial_id, document_type, document_url, document_date, source_id)
            VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
        """, (trial_id, d['document_type'], d['url'], d['document_date'], source_id))


def load_facilities(cur, trial_id, facilities):
    for f in facilities:
        site_id = upsert_site(cur, f['name'], f['city'], f['state'], f['zip'])
        if site_id:
            cur.execute("""
                INSERT INTO clinical.trial_site (trial_id, site_id, recruitment_status, source_status)
                VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
            """, (trial_id, site_id, f['status'], f['status']))


def load_references(cur, trial_id, refs):
    for r in refs:
        pub_id = upsert_publication(cur, r['pmid'], r['citation'])
        if pub_id:
            rel_type = (r['reference_type'] or 'related').lower()
            cur.execute("""
                INSERT INTO clinical.trial_publication (trial_id, publication_id, relationship_type)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
            """, (trial_id, pub_id, rel_type))


def load_eligibility_criteria(cur, trial_id, criteria_text):
    if not criteria_text:
        return
    # Delete old criteria for this trial to avoid duplicates on re-run
    cur.execute("DELETE FROM clinical.eligibility_criterion WHERE trial_id = %s", (trial_id,))
    cur.execute("""
        INSERT INTO clinical.eligibility_criterion (trial_id, criterion_type, criterion_text, sequence_no)
        VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
    """, (trial_id, 'full_text', criteria_text, 1))


# ---------------------------------------------------------------------------
# P2: Results loading
# ---------------------------------------------------------------------------
def load_results(cur, trial_id, child_data, nct):
    """Load all P2 results data for a single trial."""
    rg_rows = child_data['result_groups'].get(nct, [])
    if not rg_rows:
        return  # No results for this trial

    # 1. Result groups — build AACT id → UUID map
    rg_map = {}  # aact result_group.id → unified result_group_id
    for rg in rg_rows:
        cur.execute("""
            INSERT INTO clinical.result_group
                (trial_id, group_code, result_type, title, description)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (trial_id, group_code, result_type) DO UPDATE SET
                title = EXCLUDED.title, description = EXCLUDED.description
            RETURNING result_group_id
        """, (trial_id, rg['ctgov_group_code'], rg['result_type'],
              rg['title'] or 'Untitled', rg['description']))
        rg_map[rg['id']] = cur.fetchone()[0]

    # 2. Result-level outcomes — map AACT outcome.id → unified outcome_id
    outcome_map = {}  # aact outcomes.id → unified outcome_id
    for o in child_data['result_outcomes'].get(nct, []):
        cur.execute("""
            INSERT INTO clinical.outcome
                (trial_id, outcome_type, measure, time_frame, description, sequence_no)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (trial_id, outcome_type, measure) DO NOTHING
        """, (trial_id, o['outcome_type'], o['title'],
              o['time_frame'], o['description'], None))
        cur.execute("""
            SELECT outcome_id FROM clinical.outcome
            WHERE trial_id = %s AND outcome_type = %s AND measure = %s
        """, (trial_id, o['outcome_type'], o['title']))
        row = cur.fetchone()
        if row:
            outcome_map[o['id']] = row[0]

    # 3. Outcome measurements
    for m in child_data['outcome_measurements'].get(nct, []):
        oid = outcome_map.get(m['outcome_id'])
        rgid = rg_map.get(m['result_group_id'])
        if oid and rgid:
            cur.execute("""
                INSERT INTO clinical.outcome_measurement
                    (outcome_id, result_group_id, classification, category,
                     title, description, units, param_type, param_value,
                     param_value_num, dispersion_type, dispersion_value,
                     dispersion_value_num, dispersion_lower_limit,
                     dispersion_upper_limit, explanation_of_na)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (oid, rgid, m['classification'], m['category'],
                  m['title'], m['description'], m['units'],
                  m['param_type'], m['param_value'], m['param_value_num'],
                  m['dispersion_type'], m['dispersion_value'],
                  m['dispersion_value_num'], m['dispersion_lower_limit'],
                  m['dispersion_upper_limit'], m['explanation_of_na']))

    # 4. Outcome analyses
    analysis_map = {}  # aact outcome_analyses.id → unified analysis_id
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
    for ag in child_data['outcome_analysis_groups'].get(nct, []):
        aid = analysis_map.get(ag['outcome_analysis_id'])
        rgid = rg_map.get(ag['result_group_id'])
        if aid and rgid:
            cur.execute("""
                INSERT INTO clinical.outcome_analysis_group (analysis_id, result_group_id)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """, (aid, rgid))

    # 6. Adverse events (reported_events)
    for ev in child_data['reported_events'].get(nct, []):
        rgid = rg_map.get(ev['result_group_id'])
        if not ev.get('adverse_event_term'):
            continue
        cur.execute("""
            INSERT INTO clinical.adverse_event
                (trial_id, result_group_id, event_type, organ_system,
                 adverse_event_term, subjects_affected, subjects_at_risk,
                 event_count, frequency_threshold, time_frame, description,
                 vocabulary, assessment)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (trial_id, rgid, ev['event_type'], ev['organ_system'],
              ev['adverse_event_term'], ev['subjects_affected'],
              ev['subjects_at_risk'], ev['event_count'],
              ev['frequency_threshold'], ev['time_frame'],
              ev['description'], ev['vocab'], ev['assessment']))


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
    return data



# ---------------------------------------------------------------------------
# Worker: Process a single batch
# ---------------------------------------------------------------------------
def process_single_batch(nct_ids, source_id, retry_errors=False):
    """Worker function to process one batch of NCT IDs.
    
    Reads are batched (efficient), but each study is committed individually
    to minimize lock duration and prevent deadlocks during parallel execution.
    """
    import threading
    thread_name = threading.current_thread().name
    
    src_conn = psycopg2.connect(**CTGOV_DB_CONFIG)
    src_conn.set_session(readonly=True)
    dst_conn = psycopg2.connect(**UNIFIED_DB_CONFIG)
    
    src_cur = src_conn.cursor()
    dst_cur = dst_conn.cursor()
    
    # Performance Optimization: Asynchronous Commit
    dst_cur.execute("SET synchronous_commit = off")
    
    try:
        # 1. Batch read: Fetch full study data from AACT for all IDs at once
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
        
        # 3. Per-study commit: Write each study individually
        batch_loaded = 0
        for study in studies:
            nct = study['nct_id']
            
            try:
                trial_id = upsert_trial(dst_cur, source_id, study)
                
                # Load all child data
                load_design_attributes(dst_cur, trial_id, child_data['designs'].get(nct, []))
                arm_map = load_arms(dst_cur, trial_id, child_data['design_groups'].get(nct, []))
                intv_map = load_interventions(dst_cur, trial_id, child_data['interventions'].get(nct, []))
                load_arm_interventions(dst_cur, child_data['dgi'].get(nct, []), arm_map, intv_map)
                load_outcomes(dst_cur, trial_id, child_data['design_outcomes'].get(nct, []))
                load_conditions(dst_cur, trial_id, child_data['conditions'].get(nct, []))
                load_sponsors(dst_cur, trial_id, child_data['sponsors'].get(nct, []))
                load_secondary_ids(dst_cur, trial_id, child_data['id_info'].get(nct, []))
                load_keywords(dst_cur, trial_id, child_data['keywords'].get(nct, []))
                load_contacts(dst_cur, trial_id, child_data['contacts'].get(nct, []))
                load_officials(dst_cur, trial_id, child_data['officials'].get(nct, []))
                load_documents(dst_cur, trial_id, source_id, child_data['documents'].get(nct, []))
                load_facilities(dst_cur, trial_id, child_data['facilities'].get(nct, []))
                load_references(dst_cur, trial_id, child_data['references'].get(nct, []))
                load_eligibility_criteria(dst_cur, trial_id, study.get('eligibility_criteria'))
                load_results(dst_cur, trial_id, child_data, nct)

                # Update sync_log to completed and commit this study
                dst_cur.execute("""
                    UPDATE ingest.sync_log SET status = 'completed', processed_at = now(), error_message = NULL
                    WHERE source_code = 'CTGOV' AND source_record_id = %s
                """, (nct,))
                dst_conn.commit()
                batch_loaded += 1
                
            except Exception as e:
                # Rollback only the current (failed) study's uncommitted work
                dst_conn.rollback()
                error_msg = str(e)[:500]
                dst_cur.execute("""
                    UPDATE ingest.sync_log SET status = 'error', error_message = %s, last_attempt_at = now()
                    WHERE source_code = 'CTGOV' AND source_record_id = %s
                """, (error_msg, nct))
                dst_conn.commit()
                print(f"  [{thread_name}] ERROR on {nct}: {error_msg}")

        print(f"  [{thread_name}] Batch complete: {batch_loaded}/{len(nct_ids)} loaded.")
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
