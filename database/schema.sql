-- Clinical Trial Repository Schema
-- Optimized for multi-source integration (CT.gov, EU Clinical Trials, ISRCTN, etc.)
-- Goal: Simplified "GlobalData-style" repository

-- Enable UUID extension if not present
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Main Trials Table (The Hub)
CREATE TABLE IF NOT EXISTS trials (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    primary_id VARCHAR(50) UNIQUE NOT NULL, -- e.g., NCT06439277
    source_name VARCHAR(50) NOT NULL,       -- e.g., 'CTGOV', 'EUTRIAL', 'ISRCTN'
    
    -- Identification
    title TEXT NOT NULL,
    official_title TEXT,
    acronym VARCHAR(100),
    
    -- Status & Design
    overall_status VARCHAR(100),            -- Standardized values
    phase VARCHAR(50),                      -- Standardized values (Phase 1, Phase 2...)
    study_type VARCHAR(100),               -- Interventional, Observational...
    last_known_status VARCHAR(100),
    
    -- Dates (Standardized to DATE type)
    start_date DATE,
    primary_completion_date DATE,
    completion_date DATE,
    first_posted_date DATE,
    results_first_posted_date DATE,
    last_update_posted_date DATE,
    
    -- Metrics
    enrollment INTEGER,
    enrollment_type VARCHAR(50),           -- Actual vs Anticipated
    
    -- Curated / GlobalData Style Fields
    brief_summary TEXT,
    detailed_description TEXT,
    eligibility_criteria TEXT,
    minimum_age VARCHAR(50),
    maximum_age VARCHAR(50),
    gender VARCHAR(50),
    
    -- Mechanism of Action / Target (Curated Data)
    moa TEXT,
    therapeutic_areas TEXT[],               -- Array for multiple areas (Oncology, etc.)
    
    -- Raw Data Vault
    raw_metadata JSONB DEFAULT '{}',       -- Catch-all for source-specific unstructured data
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Trial Identifiers (Cross-referencing)
CREATE TABLE IF NOT EXISTS trial_identifiers (
    id SERIAL PRIMARY KEY,
    trial_id UUID REFERENCES trials(id) ON DELETE CASCADE,
    identifier_type VARCHAR(50) NOT NULL,   -- 'NCT', 'EudraCT', 'ISRCTN', 'ORG_ID'
    identifier_value VARCHAR(100) NOT NULL,
    UNIQUE(trial_id, identifier_type, identifier_value)
);

-- 3. Indications (1-to-Many)
CREATE TABLE IF NOT EXISTS indications (
    id SERIAL PRIMARY KEY,
    trial_id UUID REFERENCES trials(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    is_primary BOOLEAN DEFAULT FALSE,
    source_term VARCHAR(255),               -- Original term from source
    mapped_term VARCHAR(255)                -- Standardized/MeSH term
);

-- 4. Interventions (1-to-Many)
CREATE TABLE IF NOT EXISTS interventions (
    id SERIAL PRIMARY KEY,
    trial_id UUID REFERENCES trials(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    intervention_type VARCHAR(100),         -- Drug, Device, Biotech, etc.
    description TEXT,
    moa TEXT,                               -- Specific Mechanism if known
    target VARCHAR(255)                     -- Specific Target if known
);

-- 5. Organizations (Sponsors & Collaborators)
CREATE TABLE IF NOT EXISTS trial_organizations (
    id SERIAL PRIMARY KEY,
    trial_id UUID REFERENCES trials(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    role VARCHAR(100) NOT NULL,             -- 'LEAD_SPONSOR', 'COLLABORATOR', 'FUNDER'
    org_type VARCHAR(100)                   -- 'INDUSTRY', 'NIH', 'OTHER'
);

-- 6. Locations (Sites)
CREATE TABLE IF NOT EXISTS trial_locations (
    id SERIAL PRIMARY KEY,
    trial_id UUID REFERENCES trials(id) ON DELETE CASCADE,
    facility_name TEXT,
    city VARCHAR(255),
    state VARCHAR(255),
    zip VARCHAR(50),
    country VARCHAR(100),
    status VARCHAR(100)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_trials_primary_id ON trials(primary_id);
CREATE INDEX IF NOT EXISTS idx_trials_overall_status ON trials(overall_status);
CREATE INDEX IF NOT EXISTS idx_trials_phase ON trials(phase);
CREATE INDEX IF NOT EXISTS idx_indications_name ON indications(name);
CREATE INDEX IF NOT EXISTS idx_interventions_name ON interventions(name);
CREATE INDEX IF NOT EXISTS idx_trial_identifiers_value ON trial_identifiers(identifier_value);
CREATE INDEX IF NOT EXISTS idx_trial_meta_gin ON trials USING GIN (raw_metadata);
