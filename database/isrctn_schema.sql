-- ISRCTN Repository Comprehensive Schema
-- Mirrors 100% of XML data fields for deep clinical analysis

-- 1. Trial Queue (Staging)
CREATE TABLE IF NOT EXISTS trial_queue (
    isrctn_id VARCHAR(50) PRIMARY KEY,
    retrieval_status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'completed', 'failed'
    last_attempt TIMESTAMP WITH TIME ZONE,
    error_log TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Core Trials Table
CREATE TABLE IF NOT EXISTS trials (
    isrctn_id VARCHAR(50) PRIMARY KEY REFERENCES trial_queue(isrctn_id),
    
    -- XML Attributes
    last_updated_xml TIMESTAMP WITH TIME ZONE,
    version_xml INTEGER,
    is_visible_to_public BOOLEAN,
    public_id_type VARCHAR(50),
    public_id_canonical VARCHAR(50),
    public_id_date DATE,
    isrctn_date_assigned DATE,
    
    -- Descriptions
    acknowledgment BOOLEAN,
    title TEXT NOT NULL,
    scientific_title TEXT,
    acronym VARCHAR(255),
    study_hypothesis TEXT,
    plain_english_summary TEXT,
    
    -- Design & Dates
    study_design TEXT,
    primary_study_design TEXT,
    secondary_study_design TEXT,
    trial_types TEXT,
    overall_end_date DATE,
    overall_status_override VARCHAR(255), -- New from Master XML
    reason_abandoned TEXT, -- New from Master XML
    
    -- Selection Criteria
    inclusion_criteria TEXT,
    exclusion_criteria TEXT,
    ethics_approval_required VARCHAR(255),
    ethics_approval_text TEXT, 
    
    -- Status Overrides (from participants section)
    rect_start_status_override VARCHAR(255),
    rect_status_override VARCHAR(255),
    
    -- Outcomes (Text Fallback)
    primary_outcome_text TEXT,
    secondary_outcome_text TEXT,
    
    -- Results & Metadata
    publication_details TEXT,
    publication_stage VARCHAR(255),
    basic_report TEXT,
    plain_english_report TEXT,
    ipd_sharing_plan VARCHAR(255),
    ipd_sharing_statement TEXT,
    data_policy TEXT,
    
    raw_xml XML,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Participant Details
CREATE TABLE IF NOT EXISTS participant_details (
    isrctn_id VARCHAR(50) PRIMARY KEY REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    age_range VARCHAR(255),
    lower_age_limit_value DECIMAL,
    lower_age_limit_unit VARCHAR(50),
    upper_age_limit_value DECIMAL,
    upper_age_limit_unit VARCHAR(50),
    gender VARCHAR(50),
    healthy_volunteers_allowed BOOLEAN,
    target_enrolment INTEGER,
    total_final_enrolment INTEGER,
    recruitment_start DATE,
    recruitment_end DATE
);

CREATE TABLE IF NOT EXISTS participant_types (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    participant_type VARCHAR(255)
);

-- 4. External Identifiers
CREATE TABLE IF NOT EXISTS external_identifiers (
    isrctn_id VARCHAR(50) PRIMARY KEY REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    doi VARCHAR(255),
    eudract_number VARCHAR(255),
    iras_number VARCHAR(255),
    clinicaltrials_gov_number VARCHAR(255),
    protocol_serial_number TEXT
);

CREATE TABLE IF NOT EXISTS secondary_identifiers (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    internal_id UUID, -- 'id' attribute from XML
    number_type VARCHAR(255),
    canonical_number VARCHAR(255),
    value TEXT
);

-- 5. Outcome Measures
CREATE TABLE IF NOT EXISTS outcomes (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    measure_id UUID, -- From XML attribute 'id'
    outcome_type VARCHAR(20), -- 'primary' or 'secondary'
    variable TEXT,
    method TEXT,
    timepoints TEXT
);

-- 6. Ethics Committees
CREATE TABLE IF NOT EXISTS ethics_committees (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    committee_id UUID, -- From XML attribute 'id'
    approval_status VARCHAR(255),
    status_date TIMESTAMP WITH TIME ZONE,
    committee_name TEXT,
    address TEXT,
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    zip VARCHAR(255),
    telephone VARCHAR(255),
    email VARCHAR(255),
    committee_reference TEXT
);

-- 7. Trials Centres
CREATE TABLE IF NOT EXISTS trial_centres (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    centre_id UUID, -- From XML attribute 'id'
    rts_id VARCHAR(255),
    name TEXT,
    address TEXT,
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    zip VARCHAR(255)
);

-- 8. Recruitment Countries
CREATE TABLE IF NOT EXISTS recruitment_countries (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    country VARCHAR(255)
);

-- 9. Medical Details (Conditions & Interventions)
CREATE TABLE IF NOT EXISTS conditions (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    description TEXT,
    disease_class1 TEXT,
    disease_class2 TEXT
);

CREATE TABLE IF NOT EXISTS interventions (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    description TEXT,
    intervention_type VARCHAR(255),
    phase VARCHAR(255),
    drug_names TEXT
);

-- 10. Design Details
CREATE TABLE IF NOT EXISTS interventional_designs (
    isrctn_id VARCHAR(50) PRIMARY KEY REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    allocation VARCHAR(255),
    masking VARCHAR(255),
    control VARCHAR(255),
    assignment TEXT
);

CREATE TABLE IF NOT EXISTS trial_purposes (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    purpose TEXT
);

-- 11. Data Outputs & Files
CREATE TABLE IF NOT EXISTS data_outputs (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    output_xml_id UUID,
    output_type VARCHAR(255),
    artefact_type VARCHAR(255),
    date_created TIMESTAMP WITH TIME ZONE,
    date_uploaded TIMESTAMP WITH TIME ZONE,
    peer_reviewed BOOLEAN,
    patient_facing BOOLEAN,
    created_by TEXT,
    file_id UUID,
    file_version VARCHAR(50),
    original_filename TEXT,
    download_filename TEXT,
    mime_type VARCHAR(255),
    file_length BIGINT,
    md5sum VARCHAR(50),
    description TEXT,
    production_notes TEXT,
    external_url TEXT
);

CREATE TABLE IF NOT EXISTS attached_files (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    file_id UUID,
    name TEXT,
    description TEXT,
    download_url TEXT,
    is_public BOOLEAN,
    mime_type VARCHAR(255),
    file_length BIGINT,
    md5sum VARCHAR(50)
);

-- 12. Organizations (Sponsors & Funders)
CREATE TABLE IF NOT EXISTS organizations (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    org_id UUID,
    name TEXT,
    org_role VARCHAR(50), -- 'SPONSOR', 'FUNDER'
    org_type VARCHAR(255),
    ror_id VARCHAR(255),
    commercial_status VARCHAR(255),
    fund_ref TEXT
);

-- 13. Contacts
CREATE TABLE IF NOT EXISTS contacts (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    contact_id UUID,
    title VARCHAR(50),
    forename VARCHAR(255),
    surname VARCHAR(255),
    orcid VARCHAR(255),
    address TEXT,
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    zip VARCHAR(255),
    telephone VARCHAR(255),
    email VARCHAR(255),
    privacy VARCHAR(50)
);

-- 14. Contact Roles/Types
CREATE TABLE IF NOT EXISTS contact_types (
    id SERIAL PRIMARY KEY,
    contact_record_id INTEGER REFERENCES contacts(id) ON DELETE CASCADE,
    type_name VARCHAR(255)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_queue_isrctn ON trial_queue(isrctn_id);
CREATE INDEX IF NOT EXISTS idx_queue_status ON trial_queue(retrieval_status);
CREATE INDEX IF NOT EXISTS idx_trials_updated ON trials(updated_at);
