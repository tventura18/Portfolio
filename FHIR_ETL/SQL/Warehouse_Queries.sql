-- Create a new dataset
CREATE SCHEMA `fhir-synthea-data.fhir_warehouse`;

CREATE OR REPLACE TABLE `fhir-synthea-data.fhir_warehouse.dim_organizations` AS
SELECT DISTINCT
    organization_id,
    organization_name,
    organization_type,
    active
FROM `fhir-synthea-data.fhir_curated_sample.organizations`;

/*** need to compare to conditions.  May need to add conditions***/
CREATE OR REPLACE TABLE `fhir-synthea-data.fhir_warehouse.dim_diagnosis` AS
SELECT DISTINCT
    d.diagnosis AS diagnosis_code
FROM `fhir-synthea-data.fhir_curated_sample.claims` c,
UNNEST(c.diagnoses) d;

CREATE OR REPLACE TABLE `fhir-synthea-data.fhir_warehouse.dim_item_code` AS
SELECT DISTINCT
    i.item_code AS item_code,
    i.item_display AS item_description,
    i.item_system AS item_code_system
FROM `fhir-synthea-data.fhir_curated_sample.claim_items` i;

CREATE OR REPLACE TABLE `fhir-synthea-data.fhir_warehouse.dim_encounter` AS
SELECT DISTINCT
    encounter_id,
    encounter_class,
    start_datetime,
    end_datetime,
    patient_id
FROM `fhir-synthea-data.fhir_curated_sample.encounter`;


--Creating fact table from curated normalized tables
CREATE OR REPLACE TABLE `fhir-synthea-data.fhir_warehouse.fact_claim_item` AS
SELECT
    c.claim_id,
    c.patient_id,
    c.provider_reference_id AS provider_id,
    o.organization_id,
    o.organization_name,
    o.organization_type, 
    
    -- Item-level info
    i.item_sequence AS item_sequence,
    i.item_type,
    i.item_code AS item_code,
    i.item_display AS item_description,
    SAFE_CAST(i.service_start AS DATE) AS service_start_date,
    SAFE_CAST(i.service_end AS DATE) AS service_end_date,
    i.net_value,
    i.net_currency,

    -- Diagnosis (exploded)
    d.diagnosis AS diagnosis_code,
    d.sequence AS diagnosis_sequence,

    -- Encounter info
    i.encounter AS encounter_id,
    e.encounter_class AS encounter_type,

    -- Measures
    c.total_value AS claim_total_value,
    1 AS item_count

FROM `fhir-synthea-data.fhir_curated_sample.claims` c
LEFT JOIN `fhir-synthea-data.fhir_curated_sample.organizations` o
    ON c.provider_reference_id = o.organization_id
LEFT JOIN `fhir-synthea-data.fhir_curated_sample.claim_items` i
    ON i.claim_id = c.claim_id
LEFT JOIN UNNEST(c.diagnoses) AS d
LEFT JOIN `fhir-synthea-data.fhir_curated_sample.encounter` e
    ON e.encounter_id = i.encounter;
--3,531,857 records from select

SELECT *
FROM fhir-synthea-data.fhir_warehouse.fact_claim_item;
--3,531,857 records

--double checking for 3,531,857 records
SELECT
    c.claim_id,
    c.patient_id,
    c.provider_reference_id AS provider_id,
    o.organization_id,
    
    -- Item-level info
    i.item_sequence AS item_sequence,
    i.item_type,
    i.item_code AS item_code,
    i.item_display AS item_description,
    SAFE_CAST(i.service_start AS DATE) AS service_start_date,
    SAFE_CAST(i.service_end AS DATE) AS service_end_date,
    i.net_value,
    i.net_currency,

    -- Diagnosis (exploded)
    d.diagnosis AS diagnosis_code,
    d.sequence AS diagnosis_sequence,

    -- Encounter info
    i.encounter AS encounter_id,
    e.encounter_class AS encounter_type,

    -- Measures
    c.total_value AS claim_total_value,
    1 AS item_count

FROM `fhir-synthea-data.fhir_warehouse.fact_claim_items` c
LEFT JOIN `fhir-synthea-data.fhir_warehouse.dim_organizations` o
    ON c.provider_reference_id = o.organization_id
LEFT JOIN `fhir-synthea-data.fhir_warehouse.dim_diagnosis` d
    ON d.diagnosis_code = c.
LEFT JOIN `fhir-synthea-data.fhir_warehouse.dim_encounter` e
    ON e.encounter_id = i.encounter;

WITH encounter_counts as(
    SELECT organization_name,
    count(*) as encounter_count
    FROM fhir-synthea-data.fhir_warehouse.fact_claim_item
    group by organization_name
)
SELECT
    organization_name,
    encounter_count
FROM (
    SELECT 
        organization_name,
        encounter_count,
        ROW_NUMBER() OVER (ORDER BY encounter_count DESC) AS RN
        FROM encounter_counts)
ORDER BY encounter_count DESC;








