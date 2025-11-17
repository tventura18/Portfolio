# FHIR_project
FHIR_data engineering
# Data Engineering: Patient Encounter ETL

## Goal
Build an ETL pipeline that ingests FHIR-like patient and encounter data into a warehouse and organizes it into a star schema.

## Data
- Source: Synthetic FHIR JSON (patients, practitioners, encounters)
- Target: PostgreSQL / BigQuery

## Steps
1. Extract JSON data
2. Transform into normalized tables
3. Load into a star schema (patients, providers, encounters, diagnoses)

## Tools
- Python (pandas, datetime, psycopg2/bigquery)
- SQL (DDL for schema, transformations)
- Docker (optional, containerized environment)

## Deliverables
- ETL scripts
- Star schema SQL
- ERD diagram
