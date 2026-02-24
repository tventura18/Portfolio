Data Engineering Project: Patient Encounter ETL
Project Goal

Build an end-to-end ETL pipeline that transforms patient and encounter data from FHIR-like sources into a star schema for analytics. This project demonstrates best practices in data ingestion, normalization, transformation, and modeling, with a focus on healthcare data.

Data Sources

Source Data: Synthetic FHIR JSON files for patients, practitioners, encounters, claims, and other healthcare resources.

Staging Tables: All FHIR resources are loaded into PostgreSQL staging tables (e.g., patients_fhir_raw, encounters_fhir_raw) with their original IDs and JSON content.

Target: PostgreSQL or BigQuery star schema for analytics-ready data.

FHIR Staging Table Mapping
FHIR Resource	Staging Table	Source ID Column
Practitioner	practitioners_fhir_raw	practitioner_id
Patient	patients_fhir_raw	patient_id
Encounter	encounters_fhir_raw	encounter_id
Observation	observations_fhir_raw	observation_id
Condition	conditions_fhir_raw	condition_id
Claim	claims_fhir_raw	claim_id
ExplanationOfBenefit	explanationofbenefits_fhir_raw	explanationofbenefit_id
Organization	organizations_fhir_raw	organization_id
…	…	…

Why staging tables?
Staging tables act as a controlled landing zone between raw ingestion and transformations. They provide error isolation, traceability, schema flexibility, and simplified transformations. This ensures robust and auditable ETL pipelines, particularly important in healthcare analytics.

ETL Pipeline Steps

Extract

Read raw FHIR data from the staging tables in PostgreSQL.

Maintain source IDs for traceability.

Transform

Normalize data into structured dimension tables: dim_patient, dim_provider, dim_encounter, dim_item_code, dim_organizations, dim_date, dim_payer, dim_diagnosis.

Apply data cleaning, type conversions, and relationship mapping.

Handle many-to-many relationships with bridge tables (e.g., bridge_claim_diagnosis).

Load

Populate the fact table fact_claim_item with measures such as submitted_amount, allowed_amount, payer_paid_amount, coinsurance_amount, and deductible_amount.

Ensure all surrogate keys link to their respective dimensions for referential integrity.

Star Schema Overview

Dimensions:

dim_patient – patient demographics

dim_encounter – encounter metadata linked to patients

dim_item_code – service/procedure codes

dim_organizations – provider organizations

dim_date – dates for service, encounter, and claims

dim_payer – payer information

dim_diagnosis (optional) – diagnosis codes

Fact Table:

fact_claim_item – central fact table capturing claims, line items, financial amounts, and links to all dimensions

Bridge Table (for many-to-many relationships):

bridge_claim_diagnosis – maps claims to diagnoses

All dimensions use surrogate keys for referential integrity, while maintaining source IDs for traceability to the raw FHIR data.

Tools & Technologies

Python: pandas, datetime, psycopg2 (PostgreSQL) / google-cloud-bigquery

SQL: DDL for schema creation, transformation queries

Docker: Optional, for containerized execution

Deliverables

ETL scripts (Python & SQL)

Star schema SQL scripts

ERD diagram visualizing the schema

Optional sample queries demonstrating fact/dimension joins

Outcome

This project demonstrates a full healthcare data engineering workflow:

FHIR JSON → staging tables (raw ingestion)

Staging tables → normalized dimensions (transformations)

Dimensions → star schema fact table (analytics-ready)

It highlights your ability to build robust, auditable, and maintainable ETL pipelines using Python, SQL, and PostgreSQL/BigQuery — essential skills for any data engineering role.

