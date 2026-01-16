-- ============================================
-- Initialisation Snowflake - StreamVision
-- ============================================

USE DATABASE STREAMVISION_WH;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS CORE;
CREATE SCHEMA IF NOT EXISTS MARTS;

SHOW SCHEMAS;

SELECT 'Schémas StreamVision créés avec succès' AS message;
