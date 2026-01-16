-- ============================================
-- Création de la Storage Integration (S3 - Snowflake)
-- ============================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::328263827382:role/snowflake-s3-integration-role'  -- VOTRE ARN ICI
    STORAGE_ALLOWED_LOCATIONS = ('s3://streamvision-data-raw/raw/');  -- VOTRE BUCKET ICI

-- Récupération des informations pour AWS
DESC INTEGRATION s3_integration;