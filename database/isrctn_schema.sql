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
    
    -- Selection Criteria
    inclusion_criteria TEXT,
    exclusion_criteria TEXT,
    ethics_approval_required VARCHAR(50),
    
    -- Overrides & Metadata
    rect_start_status_override TEXT,
    rect_status_override TEXT,
    ipd_sharing_plan VARCHAR(50),
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

-- 4. External Identifiers
CREATE TABLE IF NOT EXISTS external_identifiers (
    isrctn_id VARCHAR(50) PRIMARY KEY REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    doi VARCHAR(255),
    eudract_number VARCHAR(255),
    iras_number VARCHAR(255),
    clinicaltrials_gov_number VARCHAR(255),
    protocol_serial_number VARCHAR(255),
    secondary_numbers TEXT 
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
    approval_status VARCHAR(100),
    status_date TIMESTAMP WITH TIME ZONE,
    committee_name TEXT,
    address TEXT,
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    zip VARCHAR(50),
    telephone VARCHAR(100),
    email VARCHAR(255),
    committee_reference TEXT
);

-- 7. Trials Centres
CREATE TABLE IF NOT EXISTS trial_centres (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    centre_id UUID, -- From XML attribute 'id'
    name TEXT,
    address TEXT,
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    zip VARCHAR(50)
);

-- 8. Recruitment Countries
CREATE TABLE IF NOT EXISTS recruitment_countries (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    country VARCHAR(100)
);

-- 9. Medical Details (Conditions & Interventions)
CREATE TABLE IF NOT EXISTS conditions (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    description TEXT,
    disease_class1 VARCHAR(255),
    disease_class2 VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS interventions (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    description TEXT,
    intervention_type VARCHAR(255),
    phase VARCHAR(100),
    drug_names TEXT
);

-- 10. Organizations (Sponsors & Funders)
CREATE TABLE IF NOT EXISTS organizations (
    id SERIAL PRIMARY KEY,
    isrctn_id VARCHAR(50) REFERENCES trials(isrctn_id) ON DELETE CASCADE,
    org_id UUID,
    name TEXT,
    org_role VARCHAR(50), -- 'SPONSOR', 'FUNDER'
    org_type VARCHAR(100),
    ror_id VARCHAR(100),
    commercial_status VARCHAR(100),
    fund_ref TEXT
);

-- 11. Contacts
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
    zip VARCHAR(50),
    telephone VARCHAR(100),
    email VARCHAR(255),
    privacy VARCHAR(50)
);

-- 12. Contact Roles/Types
CREATE TABLE IF NOT EXISTS contact_types (
    id SERIAL PRIMARY KEY,
    contact_record_id INTEGER REFERENCES contacts(id) ON DELETE CASCADE,
    type_name VARCHAR(100)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_queue_isrctn ON trial_queue(isrctn_id);
CREATE INDEX IF NOT EXISTS idx_queue_status ON trial_queue(retrieval_status);
CREATE INDEX IF NOT EXISTS idx_trials_updated ON trials(updated_at);
