-- Master Pharmaceutical Intelligence Database - PostgreSQL
-- Version: 0.1 MVP master model
-- Scope: clinical trial registries, drug & pipeline, scientific/medical data, news/company data
-- Requires: PostgreSQL 14+

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS citext;

CREATE SCHEMA IF NOT EXISTS ref;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS clinical;
CREATE SCHEMA IF NOT EXISTS drug;
CREATE SCHEMA IF NOT EXISTS scientific;
CREATE SCHEMA IF NOT EXISTS company;
CREATE SCHEMA IF NOT EXISTS ingest;

-- =========================================================
-- Shared reference / governance
-- =========================================================
CREATE TABLE ref.source (
  source_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_code text NOT NULL UNIQUE,               -- CTGOV, ISRCTN, EUCTR, GD_MANUAL, etc.
  source_name text NOT NULL,
  source_url text,
  source_type text NOT NULL DEFAULT 'registry',   -- registry, company, news, publication, manual, vendor
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ref.country (
  country_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  iso2 char(2) UNIQUE,
  iso3 char(3) UNIQUE,
  country_name text NOT NULL UNIQUE,
  region text,
  sub_region text
);

CREATE TABLE ref.therapeutic_area (
  therapeutic_area_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL UNIQUE,
  parent_therapeutic_area_id uuid REFERENCES ref.therapeutic_area(therapeutic_area_id)
);

CREATE TABLE ref.indication (
  indication_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL UNIQUE,
  normalized_name text,
  therapeutic_area_id uuid REFERENCES ref.therapeutic_area(therapeutic_area_id)
);

CREATE TABLE ref.route_of_administration (
  route_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  route_name text NOT NULL UNIQUE
);

CREATE TABLE audit.etl_batch (
  etl_batch_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pipeline_name text NOT NULL,
  source_id uuid REFERENCES ref.source(source_id),
  source_file_name text,
  source_file_hash text,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  status text NOT NULL DEFAULT 'running',
  row_count int,
  error_message text
);

CREATE TABLE audit.record_lineage (
  lineage_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_schema text NOT NULL,
  entity_table text NOT NULL,
  entity_id uuid NOT NULL,
  source_id uuid REFERENCES ref.source(source_id),
  source_record_id text,
  source_url text,
  etl_batch_id uuid REFERENCES audit.etl_batch(etl_batch_id),
  source_payload jsonb,
  extracted_at timestamptz,
  loaded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(entity_schema, entity_table, entity_id, source_id, source_record_id)
);

-- Raw landing table for current MVP sources / future data lake feeds
CREATE TABLE ingest.raw_record (
  raw_record_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id uuid NOT NULL REFERENCES ref.source(source_id),
  source_record_id text NOT NULL,
  source_url text,
  payload jsonb NOT NULL,
  payload_hash text NOT NULL,
  etl_batch_id uuid REFERENCES audit.etl_batch(etl_batch_id),
  ingested_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(source_id, source_record_id, payload_hash)
);

-- =========================================================
-- Company / organization data
-- =========================================================
CREATE TABLE company.organization (
  organization_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_name text NOT NULL,
  normalized_name text,
  organization_type text,                         -- sponsor, cro, collaborator, hospital, university, company
  website_url text,
  headquarters_country_id uuid REFERENCES ref.country(country_id),
  description text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(normalized_name, organization_type)
);

CREATE TABLE company.person (
  person_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  full_name text NOT NULL,
  specialty text,
  designation text,
  email citext,
  phone text,
  country_id uuid REFERENCES ref.country(country_id),
  state_province text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE company.news_article (
  news_article_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  title text NOT NULL,
  summary text,
  body text,
  published_at timestamptz,
  source_id uuid REFERENCES ref.source(source_id),
  source_name text,
  source_url text UNIQUE,
  language_code text DEFAULT 'en',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE company.news_organization (
  news_article_id uuid REFERENCES company.news_article(news_article_id) ON DELETE CASCADE,
  organization_id uuid REFERENCES company.organization(organization_id) ON DELETE CASCADE,
  PRIMARY KEY(news_article_id, organization_id)
);

-- =========================================================
-- Drug / pipeline data
-- =========================================================
CREATE TABLE drug.product (
  product_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  drug_name text NOT NULL,
  generic_name text,
  brand_name text,
  normalized_name text,
  drug_type text,                                 -- small molecule, biologic, peptide, vaccine, device, combo
  development_status text,                       -- pipeline, marketed, discontinued, withdrawn
  description text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(normalized_name, development_status)
);

CREATE TABLE drug.alias (
  alias_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  product_id uuid NOT NULL REFERENCES drug.product(product_id) ON DELETE CASCADE,
  alias_name text NOT NULL,
  alias_type text,                                -- brand, generic, code, INN, synonym
  source_id uuid REFERENCES ref.source(source_id),
  UNIQUE(product_id, alias_name)
);

CREATE TABLE drug.target (
  target_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  target_name text NOT NULL,
  official_symbol text,
  target_type text,
  description text,
  UNIQUE(target_name, official_symbol)
);

CREATE TABLE drug.moa (
  moa_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  moa_name text NOT NULL UNIQUE,
  description text
);

CREATE TABLE drug.product_target (
  product_id uuid REFERENCES drug.product(product_id) ON DELETE CASCADE,
  target_id uuid REFERENCES drug.target(target_id) ON DELETE CASCADE,
  relationship_type text DEFAULT 'acts_on',
  PRIMARY KEY(product_id, target_id)
);

CREATE TABLE drug.product_moa (
  product_id uuid REFERENCES drug.product(product_id) ON DELETE CASCADE,
  moa_id uuid REFERENCES drug.moa(moa_id) ON DELETE CASCADE,
  PRIMARY KEY(product_id, moa_id)
);

CREATE TABLE drug.atc_classification (
  atc_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  atc_code text NOT NULL UNIQUE,
  atc_name text NOT NULL,
  level_no int
);

CREATE TABLE drug.product_atc (
  product_id uuid REFERENCES drug.product(product_id) ON DELETE CASCADE,
  atc_id uuid REFERENCES drug.atc_classification(atc_id),
  PRIMARY KEY(product_id, atc_id)
);

CREATE TABLE drug.pipeline_event (
  pipeline_event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  product_id uuid REFERENCES drug.product(product_id),
  indication_id uuid REFERENCES ref.indication(indication_id),
  phase text,
  status text,
  event_date date,
  event_title text NOT NULL,
  description text,
  source_url text,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- =========================================================
-- Scientific / medical data
-- =========================================================
CREATE TABLE scientific.biomarker (
  biomarker_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  biomarker_name text NOT NULL,
  external_identifier text,
  official_symbol text,
  biomarker_type text,
  description text,
  UNIQUE(biomarker_name, external_identifier)
);

CREATE TABLE scientific.publication (
  publication_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  title text NOT NULL,
  abstract text,
  doi text UNIQUE,
  pmid text UNIQUE,
  journal text,
  publication_date date,
  source_url text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE scientific.medical_term (
  medical_term_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  term_name text NOT NULL UNIQUE,
  vocabulary text,                                -- MedDRA, ICD10, MeSH, SNOMED, custom
  code text,
  parent_term_id uuid REFERENCES scientific.medical_term(medical_term_id)
);

-- =========================================================
-- Clinical trial registry master
-- =========================================================
CREATE TABLE clinical.trial (
  trial_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  master_trial_code text UNIQUE,                  -- internal ID e.g. GDCT0523097-style
  primary_registry_id text,                       -- e.g. NCT06439277
  title text NOT NULL,
  official_title text,
  acronym text,
  brief_summary text,
  detailed_description text,
  study_type text,                                -- Interventional, Observational
  therapy_type text,                              -- Monotherapy, Combination, etc.
  phase text,
  status text,
  has_results boolean,
  data_monitoring_committee boolean,
  decentralized_trial boolean,
  purpose text,
  sex text,
  minimum_age text,
  maximum_age text,
  healthy_volunteers boolean,
  enrollment_planned int,
  enrollment_actual int,
  number_of_sites int,
  start_date date,
  start_date_type text,                           -- actual, estimated
  primary_completion_date date,
  completion_date date,
  completion_date_type text,
  trial_duration_months numeric(10,2),
  enrollment_period_months numeric(10,2),
  treatment_period_months numeric(10,2),
  conclusion text,
  source_last_updated date,
  last_reviewed_at date,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE clinical.identifier (
  identifier_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trial_id uuid NOT NULL REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  identifier_value text NOT NULL,
  identifier_type text NOT NULL,                  -- NCT, ISRCTN, EUCT, sponsor_protocol, internal, other
  source_id uuid REFERENCES ref.source(source_id),
  is_primary boolean NOT NULL DEFAULT false,
  UNIQUE(identifier_value, identifier_type)
);

CREATE TABLE clinical.source_link (
  source_link_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trial_id uuid NOT NULL REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  source_id uuid NOT NULL REFERENCES ref.source(source_id),
  source_record_id text NOT NULL,
  source_url text,
  source_version text,
  first_seen_at timestamptz,
  last_seen_at timestamptz,
  UNIQUE(source_id, source_record_id)
);

CREATE TABLE clinical.trial_indication (
  trial_id uuid REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  indication_id uuid REFERENCES ref.indication(indication_id),
  is_primary boolean DEFAULT false,
  PRIMARY KEY(trial_id, indication_id)
);

CREATE TABLE clinical.trial_sponsor (
  trial_id uuid REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  organization_id uuid REFERENCES company.organization(organization_id),
  sponsor_role text NOT NULL,                     -- sponsor, collaborator, CRO, funder
  is_lead boolean DEFAULT false,
  PRIMARY KEY(trial_id, organization_id, sponsor_role)
);

CREATE TABLE clinical.design_attribute (
  design_attribute_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trial_id uuid NOT NULL REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  attribute_type text NOT NULL,                   -- allocation, intervention_model, masking, primary_purpose, geography, objective
  attribute_value text NOT NULL,
  UNIQUE(trial_id, attribute_type, attribute_value)
);

CREATE TABLE clinical.virtual_component (
  trial_id uuid REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  component_name text NOT NULL,                   -- eCOA, eCRF, Telemedicine, Electronic Data Capture
  PRIMARY KEY(trial_id, component_name)
);

CREATE TABLE clinical.arm (
  arm_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trial_id uuid NOT NULL REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  arm_code text,
  arm_title text,
  arm_type text,
  planned_enrollment int,
  description text,
  UNIQUE(trial_id, arm_code)
);

CREATE TABLE clinical.intervention (
  intervention_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trial_id uuid NOT NULL REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  product_id uuid REFERENCES drug.product(product_id),
  intervention_name text NOT NULL,
  intervention_type text,                         -- Drug, Biological, Device, Procedure, Placebo
  is_primary boolean DEFAULT false,
  route_id uuid REFERENCES ref.route_of_administration(route_id),
  dose_regimen text,
  description text,
  UNIQUE(trial_id, intervention_name, intervention_type)
);

CREATE TABLE clinical.arm_intervention (
  arm_id uuid REFERENCES clinical.arm(arm_id) ON DELETE CASCADE,
  intervention_id uuid REFERENCES clinical.intervention(intervention_id) ON DELETE CASCADE,
  PRIMARY KEY(arm_id, intervention_id)
);

CREATE TABLE clinical.outcome (
  outcome_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trial_id uuid NOT NULL REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  outcome_type text NOT NULL,                     -- primary, secondary, other
  measure text NOT NULL,
  description text,
  time_frame text,
  sequence_no int,
  UNIQUE(trial_id, outcome_type, measure)
);

CREATE TABLE clinical.endpoint_classification (
  trial_id uuid REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  classification text NOT NULL,                   -- Efficacy, Safety, Pharmacokinetics
  PRIMARY KEY(trial_id, classification)
);

CREATE TABLE clinical.eligibility_criterion (
  criterion_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trial_id uuid NOT NULL REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  criterion_type text NOT NULL,                   -- inclusion, exclusion
  criterion_text text NOT NULL,
  sequence_no int
);

CREATE TABLE clinical.subject_tag (
  trial_id uuid REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  subject_tag text NOT NULL,                      -- Adolescents, Prediabetic, Hypertension, etc.
  PRIMARY KEY(trial_id, subject_tag)
);

CREATE TABLE clinical.trial_biomarker (
  trial_id uuid REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  biomarker_id uuid REFERENCES scientific.biomarker(biomarker_id),
  biomarker_role text,                            -- exclusion criteria, monitoring treatment response
  PRIMARY KEY(trial_id, biomarker_id, biomarker_role)
);

CREATE TABLE clinical.site (
  site_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  site_name text NOT NULL,
  organization_id uuid REFERENCES company.organization(organization_id),
  address_text text,
  city text,
  state_province text,
  postal_code text,
  country_id uuid REFERENCES ref.country(country_id),
  region text,
  latitude numeric(10,7),
  longitude numeric(10,7),
  UNIQUE(site_name, city, state_province, postal_code)
);

CREATE TABLE clinical.trial_site (
  trial_site_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trial_id uuid NOT NULL REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  site_id uuid NOT NULL REFERENCES clinical.site(site_id),
  recruitment_status text,
  is_investigator_affiliated_site boolean DEFAULT false,
  source_status text,
  UNIQUE(trial_id, site_id, is_investigator_affiliated_site)
);

CREATE TABLE clinical.trial_investigator (
  trial_investigator_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trial_id uuid NOT NULL REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  person_id uuid NOT NULL REFERENCES company.person(person_id),
  organization_id uuid REFERENCES company.organization(organization_id),
  role text,
  is_principal boolean DEFAULT false,
  UNIQUE(trial_id, person_id, role)
);

CREATE TABLE clinical.contact (
  contact_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trial_id uuid NOT NULL REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  contact_name text,
  phone text,
  email citext,
  address_text text,
  state_province text,
  country_id uuid REFERENCES ref.country(country_id),
  region text
);

CREATE TABLE clinical.event (
  event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trial_id uuid NOT NULL REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  event_date date,
  event_type text,
  event_brief text,
  source_id uuid REFERENCES ref.source(source_id),
  source_url text
);

CREATE TABLE clinical.change_history (
  change_history_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trial_id uuid NOT NULL REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  modified_date date,
  update_type text,
  description text,
  from_data text,
  to_data text,
  source_date date,
  source_type text,
  source_url text,
  source_id uuid REFERENCES ref.source(source_id),
  raw_change jsonb
);

CREATE TABLE clinical.cost_estimate (
  cost_estimate_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trial_id uuid NOT NULL REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  cost_category text,
  cost_year int,
  component_name text,
  cost_usd_millions numeric(14,4),
  assumption_text text,
  source_id uuid REFERENCES ref.source(source_id)
);

CREATE TABLE clinical.trial_publication (
  trial_id uuid REFERENCES clinical.trial(trial_id) ON DELETE CASCADE,
  publication_id uuid REFERENCES scientific.publication(publication_id) ON DELETE CASCADE,
  relationship_type text DEFAULT 'related',
  PRIMARY KEY(trial_id, publication_id)
);

-- =========================================================
-- Helpful indexes
-- =========================================================
CREATE INDEX idx_trial_primary_registry_id ON clinical.trial(primary_registry_id);
CREATE INDEX idx_trial_status_phase ON clinical.trial(status, phase);
CREATE INDEX idx_trial_dates ON clinical.trial(start_date, completion_date);
CREATE INDEX idx_identifier_value ON clinical.identifier(identifier_value);
CREATE INDEX idx_source_link_record ON clinical.source_link(source_id, source_record_id);
CREATE INDEX idx_outcome_measure_fts ON clinical.outcome USING gin(to_tsvector('english', coalesce(measure,'') || ' ' || coalesce(description,'')));
CREATE INDEX idx_criteria_fts ON clinical.eligibility_criterion USING gin(to_tsvector('english', criterion_text));
CREATE INDEX idx_product_name ON drug.product(normalized_name);
CREATE INDEX idx_org_name ON company.organization(normalized_name);
CREATE INDEX idx_site_country ON clinical.site(country_id, state_province, city);
CREATE INDEX idx_raw_payload_gin ON ingest.raw_record USING gin(payload jsonb_path_ops);
CREATE INDEX idx_change_history_date ON clinical.change_history(trial_id, modified_date DESC);

-- Basic source seeds
INSERT INTO ref.source (source_code, source_name, source_url, source_type) VALUES
('CTGOV', 'ClinicalTrials.gov', 'https://clinicaltrials.gov', 'registry'),
('ISRCTN', 'ISRCTN registry', 'https://www.isrctn.com', 'registry'),
('EUCTR', 'European Clinical Trials Information System', 'https://euclinicaltrials.eu', 'registry'),
('REEC', 'Spanish Clinical Trial Registry', 'https://reec.aemps.es', 'registry'),
('WHO_ICTRP', 'WHO International Clinical Trials Registry Platform', 'https://trialsearch.who.int', 'registry'),
('GLOBALDATA_REF', 'GlobalData reference profile', 'https://www.globaldata.com/', 'vendor')
ON CONFLICT (source_code) DO NOTHING;
