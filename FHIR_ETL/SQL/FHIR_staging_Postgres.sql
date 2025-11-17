-- ============================
-- FHIR Staging Tables
-- ============================

-- Practitioners
--TRUNCATE TABLE fhir_staging.practitioners_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.practitioners_fhir_raw (
    id SERIAL PRIMARY KEY,
    practitioner_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Practitioner Roles
--TRUNCATE TABLE fhir_staging.practitioner_roles_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.practitioner_roles_fhir_raw (
    id SERIAL PRIMARY KEY,
    practitioner_role_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Patients
--TRUNCATE TABLE fhir_staging.patients_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.patients_fhir_raw (
    id SERIAL PRIMARY KEY,
    patients_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Encounters
--TRUNCATE TABLE fhir_staging.encounters_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.encounters_fhir_raw (
    id SERIAL PRIMARY KEY,
    encounter_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Observations
--TRUNCATE TABLE fhir_staging.observations_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.observations_fhir_raw (
    id SERIAL PRIMARY KEY,
    observation_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Conditions
--TRUNCATE TABLE fhir_staging.conditions_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.conditions_fhir_raw (
    id SERIAL PRIMARY KEY,
    condition_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Claims
--TRUNCATE TABLE fhir_staging.claims_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.claims_fhir_raw (
    id SERIAL PRIMARY KEY,
    claim_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Diagnostics
--TRUNCATE TABLE fhir_staging.diagnostics_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.diagnostics_fhir_raw (
    id SERIAL PRIMARY KEY,
    diagnostic_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Document References
--TRUNCATE TABLE fhir_staging.document_references_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.document_references_fhir_raw (
    id SERIAL PRIMARY KEY,
    document_reference_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- EOBs
--TRUNCATE TABLE fhir_staging.explanationofbenefits_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.explanationofbenefits_fhir_raw (
    id SERIAL PRIMARY KEY,
    explanationofbenefit_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Careplans
--TRUNCATE TABLE fhir_staging.careplans_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.careplans_fhir_raw (
    id SERIAL PRIMARY KEY,
    careplan_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Immunizations
--TRUNCATE TABLE fhir_staging.immunizations_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.immunizations_fhir_raw (
    id SERIAL PRIMARY KEY,
    immunization_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Devices
--TRUNCATE TABLE fhir_staging.devices_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.devices_fhir_raw (
    id SERIAL PRIMARY KEY,
    device_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Supply Deliveries
--TRUNCATE TABLE fhir_staging.supplydeliveries_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.supplydeliveries_fhir_raw (
    id SERIAL PRIMARY KEY,
    supplydelivery_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Medications
--TRUNCATE TABLE fhir_staging.medications_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.medications_fhir_raw (
    id SERIAL PRIMARY KEY,
    medication_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Medication Requests
--TRUNCATE TABLE fhir_staging.medicationrequests_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.medicationrequests_fhir_raw (
    id SERIAL PRIMARY KEY,
    medicationrequest_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Medication Administrations
--TRUNCATE TABLE fhir_staging.medicationadministrations_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.medicationadministrations_fhir_raw (
    id SERIAL PRIMARY KEY,
    medicationadministration_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Imaging Studies
--TRUNCATE TABLE fhir_staging.imagingstudies_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.imagingstudies_fhir_raw (
    id SERIAL PRIMARY KEY,
    imagingstudy_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Procedures
--TRUNCATE TABLE fhir_staging.procedures_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.procedures_fhir_raw (
    id SERIAL PRIMARY KEY,
    procedure_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Organizations
--TRUNCATE TABLE fhir_staging.organizations_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.organizations_fhir_raw (
    id SERIAL PRIMARY KEY,
    organization_id TEXT UNIQUE,
    resource JSONB NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--TRUNCATE TABLE fhir_staging.provenances_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.provenances_fhir_raw (
    id SERIAL PRIMARY KEY,
    provenance_id TEXT UNIQUE,          -- "Provenance/123"
    resource JSONB NOT NULL,            -- full json blob
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--TRUNCATE TABLE fhir_staging.careteams_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.careteams_fhir_raw (
    id SERIAL PRIMARY KEY,
    careteam_id TEXT UNIQUE,          -- "Provenance/123"
    resource JSONB NOT NULL,            -- full json blob
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--TRUNCATE TABLE fhir_staging.allergyintolerances_fhir_raw RESTART IDENTITY CASCADE;
CREATE TABLE IF NOT EXISTS fhir_staging.allergyintolerances_fhir_raw (
    id SERIAL PRIMARY KEY,
    allergyintolerance_id TEXT UNIQUE,          -- "Provenance/123"
    resource JSONB NOT NULL,            -- full json blob
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================
-- Optional Drop Statements
-- Uncomment only if you want to completely remove a table
-- ============================
/***
DROP TABLE IF EXISTS fhir_staging.practitioners_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.practitioner_roles_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.patients_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.encounters_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.observations_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.conditions_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.claims_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.diagnostics_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.document_references_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.explanationofbenefits_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.careplans_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.immunizations_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.devices_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.supplydeliveries_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.medications_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.medicationrequests_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.medicationadministrations_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.imagingstudies_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.procedures_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.organizations_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.provenances_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.careteams_fhir_raw CASCADE;
DROP TABLE IF EXISTS fhir_staging.allergyintolerances_fhir_raw CASCADE;
***/