-- ==============================================================================
-- CTGov -> Unified DB Migration Procedure (High-Performance FDW Version)
-- ==============================================================================
-- This procedure maximizes throughput by first materializing all foreign data
-- into local temporary tables to avoid FDW network latency during complex JOINs.
-- It also eliminates slow row-by-row operations by generating deterministic UUIDs.
-- ==============================================================================

CREATE OR REPLACE PROCEDURE ingest.migrate_ctgov_batch(p_batch_nct_ids text[], p_source_id uuid)
LANGUAGE plpgsql
AS $$
BEGIN
    -- ========================================================================
    -- 0. MATERIALIZE REMOTE DATA LOCALLY
    -- ========================================================================
    -- Fetching everything locally FIRST drastically reduces FDW network chatter
    
    CREATE TEMP TABLE tmp_sponsors ON COMMIT DROP AS SELECT * FROM aact_foreign.sponsors WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_officials ON COMMIT DROP AS SELECT * FROM aact_foreign.overall_officials WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_conditions ON COMMIT DROP AS SELECT * FROM aact_foreign.conditions WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_facilities ON COMMIT DROP AS SELECT * FROM aact_foreign.facilities WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_references ON COMMIT DROP AS SELECT * FROM aact_foreign.study_references WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_studies ON COMMIT DROP AS SELECT * FROM aact_foreign.studies WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_brief_summaries ON COMMIT DROP AS SELECT * FROM aact_foreign.brief_summaries WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_detailed_descriptions ON COMMIT DROP AS SELECT * FROM aact_foreign.detailed_descriptions WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_eligibilities ON COMMIT DROP AS SELECT * FROM aact_foreign.eligibilities WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_calculated_values ON COMMIT DROP AS SELECT * FROM aact_foreign.calculated_values WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_designs ON COMMIT DROP AS SELECT * FROM aact_foreign.designs WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_id_info ON COMMIT DROP AS SELECT * FROM aact_foreign.id_information WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_keywords ON COMMIT DROP AS SELECT * FROM aact_foreign.keywords WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_contacts ON COMMIT DROP AS SELECT * FROM aact_foreign.central_contacts WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_documents ON COMMIT DROP AS SELECT * FROM aact_foreign.provided_documents WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_design_groups ON COMMIT DROP AS SELECT * FROM aact_foreign.design_groups WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_interventions ON COMMIT DROP AS SELECT * FROM aact_foreign.interventions WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_design_group_interventions ON COMMIT DROP AS SELECT * FROM aact_foreign.design_group_interventions WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_result_groups ON COMMIT DROP AS SELECT * FROM aact_foreign.result_groups WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_design_outcomes ON COMMIT DROP AS SELECT * FROM aact_foreign.design_outcomes WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_outcomes ON COMMIT DROP AS SELECT * FROM aact_foreign.outcomes WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_outcome_measurements ON COMMIT DROP AS SELECT * FROM aact_foreign.outcome_measurements WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_reported_events ON COMMIT DROP AS SELECT * FROM aact_foreign.reported_events WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_outcome_analyses ON COMMIT DROP AS SELECT * FROM aact_foreign.outcome_analyses WHERE nct_id = ANY(p_batch_nct_ids);
    CREATE TEMP TABLE tmp_outcome_analysis_groups ON COMMIT DROP AS SELECT * FROM aact_foreign.outcome_analysis_groups WHERE nct_id = ANY(p_batch_nct_ids);

    -- Create local indexes on tmp tables to accelerate the JOINs
    CREATE INDEX ON tmp_studies (nct_id);
    CREATE INDEX ON tmp_sponsors (nct_id);
    CREATE INDEX ON tmp_officials (nct_id);
    CREATE INDEX ON tmp_conditions (nct_id);
    CREATE INDEX ON tmp_facilities (nct_id);
    CREATE INDEX ON tmp_references (nct_id);
    CREATE INDEX ON tmp_brief_summaries (nct_id);
    CREATE INDEX ON tmp_detailed_descriptions (nct_id);
    CREATE INDEX ON tmp_eligibilities (nct_id);
    CREATE INDEX ON tmp_calculated_values (nct_id);
    CREATE INDEX ON tmp_designs (nct_id);
    CREATE INDEX ON tmp_id_info (nct_id);
    CREATE INDEX ON tmp_keywords (nct_id);
    CREATE INDEX ON tmp_contacts (nct_id);
    CREATE INDEX ON tmp_documents (nct_id);
    CREATE INDEX ON tmp_design_groups (nct_id);
    CREATE INDEX ON tmp_interventions (nct_id);
    CREATE INDEX ON tmp_design_group_interventions (nct_id);
    CREATE INDEX ON tmp_result_groups (nct_id);
    CREATE INDEX ON tmp_design_outcomes (nct_id);
    CREATE INDEX ON tmp_outcomes (nct_id);
    CREATE INDEX ON tmp_outcome_measurements (nct_id);
    CREATE INDEX ON tmp_reported_events (nct_id);
    CREATE INDEX ON tmp_outcome_analyses (nct_id);
    CREATE INDEX ON tmp_outcome_analysis_groups (nct_id);


    -- ========================================================================
    -- 1. LOOKUPS (UPSERT)
    -- ========================================================================
    
    INSERT INTO company.organization (organization_name, normalized_name, organization_type)
    SELECT DISTINCT name, lower(trim(name)), agency_class
    FROM tmp_sponsors
    WHERE name IS NOT NULL AND trim(name) != ''
    UNION
    SELECT DISTINCT affiliation, lower(trim(affiliation)), NULL
    FROM tmp_officials
    WHERE affiliation IS NOT NULL AND trim(affiliation) != ''
    ON CONFLICT (normalized_name, organization_type) DO UPDATE SET updated_at = now();

    INSERT INTO company.person (full_name)
    SELECT DISTINCT trim(name)
    FROM tmp_officials
    WHERE name IS NOT NULL AND trim(name) != ''
    ON CONFLICT DO NOTHING;

    INSERT INTO ref.indication (name, normalized_name)
    SELECT DISTINCT trim(name), lower(trim(name))
    FROM tmp_conditions
    WHERE name IS NOT NULL AND trim(name) != ''
    ON CONFLICT DO NOTHING;

    INSERT INTO clinical.site (site_name, city, state_province, postal_code)
    SELECT DISTINCT trim(name), city, state, zip
    FROM tmp_facilities
    WHERE name IS NOT NULL AND trim(name) != ''
    ON CONFLICT DO NOTHING;

    INSERT INTO scientific.publication (pmid, title)
    SELECT DISTINCT pmid, left(citation, 500)
    FROM tmp_references
    WHERE pmid IS NOT NULL
    ON CONFLICT DO NOTHING;


    -- ========================================================================
    -- 2. CORE TRIAL UPSERT
    -- ========================================================================
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
    )
    SELECT 
        p_source_id, s.nct_id, s.brief_title, s.official_title, s.acronym,
        bs.description AS brief_summary, dd.description AS detailed_description,
        s.study_type, s.phase, s.overall_status, s.why_stopped,
        cv.were_results_reported, s.has_dmc,
        s.source_class, e.gender AS sex, e.minimum_age, e.maximum_age,
        e.healthy_volunteers, e.adult AS is_adult, e.child AS is_child, e.older_adult AS is_older_adult,
        e.sampling_method, e.population AS population_description,
        CASE WHEN upper(s.enrollment_type) = 'ANTICIPATED' THEN s.enrollment ELSE NULL END,
        CASE WHEN upper(s.enrollment_type) = 'ACTUAL' THEN s.enrollment ELSE CASE WHEN s.enrollment_type IS NULL THEN s.enrollment ELSE NULL END END,
        s.enrollment_type,
        cv.number_of_facilities,
        s.start_date, s.start_date_type, s.primary_completion_date,
        s.completion_date, s.completion_date_type,
        s.plan_to_share_ipd, s.ipd_time_frame, s.ipd_access_criteria, s.ipd_url,
        s.last_update_submitted_date, s.study_first_posted_date
    FROM tmp_studies s
    LEFT JOIN tmp_brief_summaries bs ON s.nct_id = bs.nct_id
    LEFT JOIN tmp_detailed_descriptions dd ON s.nct_id = dd.nct_id
    LEFT JOIN tmp_eligibilities e ON s.nct_id = e.nct_id
    LEFT JOIN tmp_calculated_values cv ON s.nct_id = cv.nct_id
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
        updated_at = now();


    -- ========================================================================
    -- 3. SIMPLE CHILD TABLES
    -- ========================================================================

    INSERT INTO clinical.design_attribute (trial_id, attribute_type, attribute_value)
    SELECT DISTINCT t.trial_id, attr_type, attr_val
    FROM tmp_designs d
    JOIN clinical.trial t ON t.primary_registry_id = d.nct_id
    CROSS JOIN LATERAL (
        VALUES 
            ('allocation', allocation),
            ('intervention_model', intervention_model),
            ('observational_model', observational_model),
            ('primary_purpose', primary_purpose),
            ('time_perspective', time_perspective),
            ('masking', masking)
    ) AS a(attr_type, attr_val)
    WHERE attr_val IS NOT NULL
    ON CONFLICT DO NOTHING;

    INSERT INTO clinical.trial_indication (trial_id, indication_id, is_primary)
    SELECT DISTINCT t.trial_id, i.indication_id, false
    FROM tmp_conditions c
    JOIN clinical.trial t ON t.primary_registry_id = c.nct_id
    JOIN ref.indication i ON i.normalized_name = lower(trim(c.name))
    WHERE c.name IS NOT NULL AND trim(c.name) != ''
    ON CONFLICT DO NOTHING;

    INSERT INTO clinical.trial_sponsor (trial_id, organization_id, sponsor_role, is_lead)
    SELECT DISTINCT t.trial_id, o.organization_id, COALESCE(lower(s.lead_or_collaborator), 'sponsor'), lower(s.lead_or_collaborator) = 'lead'
    FROM tmp_sponsors s
    JOIN clinical.trial t ON t.primary_registry_id = s.nct_id
    JOIN company.organization o ON o.normalized_name = lower(trim(s.name)) AND (o.organization_type = s.agency_class OR (o.organization_type IS NULL AND s.agency_class IS NULL))
    WHERE s.name IS NOT NULL AND trim(s.name) != ''
    ON CONFLICT DO NOTHING;

    INSERT INTO clinical.secondary_identifier (trial_id, identifier_value, identifier_type)
    SELECT DISTINCT t.trial_id, i.id_value, COALESCE(i.id_type, 'other')
    FROM tmp_id_info i
    JOIN clinical.trial t ON t.primary_registry_id = i.nct_id
    WHERE i.id_value IS NOT NULL
    ON CONFLICT DO NOTHING;

    INSERT INTO clinical.subject_tag (trial_id, subject_tag)
    SELECT DISTINCT t.trial_id, k.name
    FROM tmp_keywords k
    JOIN clinical.trial t ON t.primary_registry_id = k.nct_id
    WHERE k.name IS NOT NULL
    ON CONFLICT DO NOTHING;

    INSERT INTO clinical.contact (trial_id, contact_name, phone, email)
    SELECT DISTINCT t.trial_id, c.name, c.phone, c.email
    FROM tmp_contacts c
    JOIN clinical.trial t ON t.primary_registry_id = c.nct_id
    WHERE c.name IS NOT NULL; 

    INSERT INTO clinical.trial_investigator (trial_id, person_id, organization_id, role, is_principal)
    SELECT DISTINCT t.trial_id, p.person_id, o.organization_id, f.role, upper(f.role) = 'PRINCIPAL_INVESTIGATOR'
    FROM tmp_officials f
    JOIN clinical.trial t ON t.primary_registry_id = f.nct_id
    JOIN company.person p ON p.full_name = trim(f.name)
    LEFT JOIN company.organization o ON o.normalized_name = lower(trim(f.affiliation)) AND o.organization_type IS NULL
    WHERE f.name IS NOT NULL AND trim(f.name) != ''
    ON CONFLICT DO NOTHING;

    INSERT INTO clinical.document (trial_id, document_type, document_url, document_date, source_id)
    SELECT DISTINCT t.trial_id, d.document_type, d.url, d.document_date, p_source_id
    FROM tmp_documents d
    JOIN clinical.trial t ON t.primary_registry_id = d.nct_id
    ON CONFLICT DO NOTHING;

    INSERT INTO clinical.trial_site (trial_id, site_id, recruitment_status, source_status)
    SELECT DISTINCT t.trial_id, s.site_id, f.status, f.status
    FROM tmp_facilities f
    JOIN clinical.trial t ON t.primary_registry_id = f.nct_id
    JOIN clinical.site s ON s.site_name = trim(f.name) AND s.city IS NOT DISTINCT FROM f.city AND s.state_province IS NOT DISTINCT FROM f.state AND s.postal_code IS NOT DISTINCT FROM f.zip
    WHERE f.name IS NOT NULL AND trim(f.name) != ''
    ON CONFLICT DO NOTHING;

    INSERT INTO clinical.trial_publication (trial_id, publication_id, relationship_type)
    SELECT DISTINCT t.trial_id, p.publication_id, lower(COALESCE(r.reference_type, 'related'))
    FROM tmp_references r
    JOIN clinical.trial t ON t.primary_registry_id = r.nct_id
    JOIN scientific.publication p ON p.pmid = r.pmid
    WHERE r.pmid IS NOT NULL
    ON CONFLICT DO NOTHING;

    DELETE FROM clinical.eligibility_criterion
    WHERE trial_id IN (SELECT trial_id FROM clinical.trial WHERE primary_registry_id = ANY(p_batch_nct_ids));

    INSERT INTO clinical.eligibility_criterion (trial_id, criterion_type, criterion_text, sequence_no)
    SELECT t.trial_id, 'full_text', e.criteria, 1
    FROM tmp_eligibilities e
    JOIN clinical.trial t ON t.primary_registry_id = e.nct_id
    WHERE e.criteria IS NOT NULL;


    -- ========================================================================
    -- 4. COMPLEX P1 ENTITIES
    -- ========================================================================
    
    INSERT INTO clinical.arm (trial_id, arm_code, arm_title, arm_type, description)
    SELECT DISTINCT ON (t.trial_id, a.id::text) 
        t.trial_id, a.id::text, a.title, a.group_type, a.description
    FROM tmp_design_groups a
    JOIN clinical.trial t ON t.primary_registry_id = a.nct_id
    ON CONFLICT (trial_id, arm_code) DO UPDATE SET
        arm_title = EXCLUDED.arm_title,
        arm_type = EXCLUDED.arm_type,
        description = EXCLUDED.description;

    INSERT INTO clinical.intervention (trial_id, intervention_name, intervention_type, description)
    SELECT DISTINCT ON (t.trial_id, COALESCE(NULLIF(trim(i.name), ''), 'SERVERGENERATED: Unnamed Intervention'), i.intervention_type)
        t.trial_id, COALESCE(NULLIF(trim(i.name), ''), 'SERVERGENERATED: Unnamed Intervention'), i.intervention_type, i.description
    FROM tmp_interventions i
    JOIN clinical.trial t ON t.primary_registry_id = i.nct_id
    ON CONFLICT (trial_id, intervention_name, intervention_type) DO UPDATE SET
        description = EXCLUDED.description;

    INSERT INTO clinical.arm_intervention (arm_id, intervention_id)
    SELECT DISTINCT a.arm_id, intv.intervention_id
    FROM tmp_design_group_interventions dgi
    JOIN clinical.trial t ON t.primary_registry_id = dgi.nct_id
    JOIN tmp_design_groups dg ON dg.id = dgi.design_group_id
    JOIN clinical.arm a ON a.trial_id = t.trial_id AND a.arm_code = dg.id::text
    JOIN tmp_interventions i ON i.id = dgi.intervention_id
    JOIN clinical.intervention intv ON intv.trial_id = t.trial_id 
        AND intv.intervention_name = COALESCE(NULLIF(trim(i.name), ''), 'SERVERGENERATED: Unnamed Intervention')
        AND intv.intervention_type IS NOT DISTINCT FROM i.intervention_type
    ON CONFLICT DO NOTHING;


    -- ========================================================================
    -- 5. RESULTS DATA (P2)
    -- ========================================================================

    INSERT INTO clinical.result_group (trial_id, group_code, result_type, title, description)
    SELECT DISTINCT ON (t.trial_id, rg.ctgov_group_code, rg.result_type)
        t.trial_id, rg.ctgov_group_code, rg.result_type, COALESCE(rg.title, 'Untitled'), rg.description
    FROM tmp_result_groups rg
    JOIN clinical.trial t ON t.primary_registry_id = rg.nct_id
    ON CONFLICT (trial_id, group_code, result_type) DO UPDATE SET
        title = EXCLUDED.title, description = EXCLUDED.description;

    INSERT INTO clinical.outcome (trial_id, outcome_type, measure, time_frame, description, sequence_no)
    SELECT DISTINCT ON (t.trial_id, o.outcome_type, COALESCE(NULLIF(trim(o.measure), ''), 'SERVERGENERATED: Unnamed Outcome'))
        t.trial_id, o.outcome_type, COALESCE(NULLIF(trim(o.measure), ''), 'SERVERGENERATED: Unnamed Outcome'), o.time_frame, o.description, 1
    FROM tmp_design_outcomes o
    JOIN clinical.trial t ON t.primary_registry_id = o.nct_id
    ON CONFLICT (trial_id, outcome_type, measure) DO NOTHING;

    INSERT INTO clinical.outcome (trial_id, outcome_type, measure, time_frame, description, sequence_no)
    SELECT DISTINCT ON (t.trial_id, ro.outcome_type, COALESCE(NULLIF(trim(ro.title), ''), 'SERVERGENERATED: Unnamed Outcome'))
        t.trial_id, ro.outcome_type, COALESCE(NULLIF(trim(ro.title), ''), 'SERVERGENERATED: Unnamed Outcome'), ro.time_frame, ro.description, NULL
    FROM tmp_outcomes ro
    JOIN clinical.trial t ON t.primary_registry_id = ro.nct_id
    ON CONFLICT (trial_id, outcome_type, measure) DO UPDATE SET
        time_frame = EXCLUDED.time_frame;

    INSERT INTO clinical.outcome_measurement (
        outcome_id, result_group_id, classification, category,
        title, description, units, param_type, param_value,
        param_value_num, dispersion_type, dispersion_value,
        dispersion_value_num, dispersion_lower_limit,
        dispersion_upper_limit, explanation_of_na
    )
    SELECT 
        o.outcome_id, rg.result_group_id, om.classification, om.category,
        om.title, om.description, om.units, om.param_type, om.param_value,
        om.param_value_num, om.dispersion_type, om.dispersion_value,
        om.dispersion_value_num, om.dispersion_lower_limit,
        om.dispersion_upper_limit, om.explanation_of_na
    FROM tmp_outcome_measurements om
    JOIN clinical.trial t ON t.primary_registry_id = om.nct_id
    JOIN tmp_outcomes s_ro ON s_ro.id = om.outcome_id
    JOIN clinical.outcome o ON o.trial_id = t.trial_id 
        AND o.outcome_type = s_ro.outcome_type 
        AND o.measure = COALESCE(NULLIF(trim(s_ro.title), ''), 'SERVERGENERATED: Unnamed Outcome')
    JOIN tmp_result_groups s_rg ON s_rg.id = om.result_group_id
    JOIN clinical.result_group rg ON rg.trial_id = t.trial_id 
        AND rg.group_code = s_rg.ctgov_group_code 
        AND rg.result_type = s_rg.result_type;

    INSERT INTO clinical.adverse_event (
        trial_id, result_group_id, event_type, organ_system,
        adverse_event_term, subjects_affected, subjects_at_risk,
        event_count, frequency_threshold, time_frame, description,
        vocabulary, assessment
    )
    SELECT 
        t.trial_id, rg.result_group_id, ae.event_type, ae.organ_system,
        ae.adverse_event_term, ae.subjects_affected, ae.subjects_at_risk,
        ae.event_count, ae.frequency_threshold, ae.time_frame, ae.description,
        ae.vocab, ae.assessment
    FROM tmp_reported_events ae
    JOIN clinical.trial t ON t.primary_registry_id = ae.nct_id
    JOIN tmp_result_groups s_rg ON s_rg.id = ae.result_group_id
    JOIN clinical.result_group rg ON rg.trial_id = t.trial_id 
        AND rg.group_code = s_rg.ctgov_group_code 
        AND rg.result_type = s_rg.result_type
    WHERE ae.adverse_event_term IS NOT NULL;


    -- ========================================================================
    -- 6. MD5 DETERMINISTIC OUTCOME ANALYSES
    -- ========================================================================
    -- By hashing the foreign ID, we bypass row-by-row mapping loops!

    DELETE FROM clinical.outcome_analysis
    WHERE outcome_id IN (SELECT outcome_id FROM clinical.outcome o JOIN clinical.trial t ON t.trial_id = o.trial_id WHERE t.primary_registry_id = ANY(p_batch_nct_ids));

    INSERT INTO clinical.outcome_analysis (
        analysis_id, outcome_id, non_inferiority_type, non_inferiority_description,
        param_type, param_value, dispersion_type, dispersion_value,
        p_value, p_value_modifier, ci_n_sides, ci_percent,
        ci_lower_limit, ci_upper_limit, method, method_description,
        estimate_description
    )
    SELECT DISTINCT ON (oa.id)
        CAST(md5('outcome_analysis_' || oa.id::text) AS UUID),
        o.outcome_id, oa.non_inferiority_type, oa.non_inferiority_description,
        oa.param_type, oa.param_value, oa.dispersion_type, oa.dispersion_value,
        oa.p_value::text, oa.p_value_modifier, oa.ci_n_sides, oa.ci_percent,
        oa.ci_lower_limit, oa.ci_upper_limit, oa.method, oa.method_description,
        oa.estimate_description
    FROM tmp_outcome_analyses oa
    JOIN clinical.trial t ON t.primary_registry_id = oa.nct_id
    JOIN tmp_outcomes s_ro ON s_ro.id = oa.outcome_id
    JOIN clinical.outcome o ON o.trial_id = t.trial_id 
        AND o.outcome_type = s_ro.outcome_type 
        AND o.measure = COALESCE(NULLIF(trim(s_ro.title), ''), 'SERVERGENERATED: Unnamed Outcome');

    INSERT INTO clinical.outcome_analysis_group (analysis_id, result_group_id)
    SELECT DISTINCT
        CAST(md5('outcome_analysis_' || oag.outcome_analysis_id::text) AS UUID),
        rg.result_group_id
    FROM tmp_outcome_analysis_groups oag
    JOIN tmp_result_groups s_rg ON s_rg.id = oag.result_group_id
    JOIN clinical.trial t ON t.primary_registry_id = s_rg.nct_id
    JOIN clinical.result_group rg ON rg.trial_id = t.trial_id 
        AND rg.group_code = s_rg.ctgov_group_code 
        AND rg.result_type = s_rg.result_type
    ON CONFLICT DO NOTHING;

END;
$$;
