-- SQL Script to Create Tables for CSV Data
-- Generated for UT Data Operations Project
-- 
-- This script creates tables that correspond to the CSV files:
-- - BIO_EMAIL.csv
-- - BIO_EMAIL_FOR_VIP.csv  
-- - BIO_EMAIL_FOR_VIP_TAGS.csv
-- - BIO_NAME.csv
-- - BIO_VIP.csv

-- Drop tables if they exist (for clean recreation)
DROP TABLE IF EXISTS bio_email_for_vip_tags CASCADE;
DROP TABLE IF EXISTS bio_email_for_vip CASCADE;
DROP TABLE IF EXISTS bio_email CASCADE;
DROP TABLE IF EXISTS bio_name CASCADE;
DROP TABLE IF EXISTS bio_vip CASCADE;

-- Create BIO_VIP table (parent table for VIP information)
CREATE TABLE bio_vip (
    id BIGINT PRIMARY KEY,
    ut_eid VARCHAR(50),
    created_on TIMESTAMP,
    updated_on TIMESTAMP,
    entity_id VARCHAR(10),
    status_id VARCHAR(10),
    created_by_id VARCHAR(10),
    updated_by_id VARCHAR(10)
);

-- Create BIO_EMAIL table (email addresses)
CREATE TABLE bio_email (
    id BIGINT PRIMARY KEY,
    address VARCHAR(255),
    updated_on TIMESTAMP
);

-- Create BIO_NAME table (name and organization information)
CREATE TABLE bio_name (
    id BIGINT PRIMARY KEY,
    first_name VARCHAR(100),
    middle_name VARCHAR(100),
    last_name VARCHAR(100),
    organization_name VARCHAR(255),
    vip_id BIGINT,
    created_on TIMESTAMP,
    updated_on TIMESTAMP,
    created_by_id VARCHAR(10),
    updated_by_id VARCHAR(10),
    FOREIGN KEY (vip_id) REFERENCES bio_vip(id)
);

-- Create BIO_EMAIL_FOR_VIP table (junction table linking emails to VIPs)
CREATE TABLE bio_email_for_vip (
    id BIGINT PRIMARY KEY,
    is_bad INTEGER,
    email_id BIGINT,
    vip_id BIGINT,
    FOREIGN KEY (email_id) REFERENCES bio_email(id),
    FOREIGN KEY (vip_id) REFERENCES bio_vip(id)
);

-- Create BIO_EMAIL_FOR_VIP_TAGS table (tags for email-VIP relationships)
CREATE TABLE bio_email_for_vip_tags (
    id BIGINT PRIMARY KEY,
    emailforvip_id BIGINT,
    emailtag_id VARCHAR(10),
    FOREIGN KEY (emailforvip_id) REFERENCES bio_email_for_vip(id)
);

-- Create indexes for better performance
CREATE INDEX idx_bio_name_vip_id ON bio_name(vip_id);
CREATE INDEX idx_bio_email_for_vip_email_id ON bio_email_for_vip(email_id);
CREATE INDEX idx_bio_email_for_vip_vip_id ON bio_email_for_vip(vip_id);
CREATE INDEX idx_bio_email_for_vip_tags_emailforvip_id ON bio_email_for_vip_tags(emailforvip_id);

-- Add comments to tables for documentation
COMMENT ON TABLE bio_vip IS 'VIP user information and status';
COMMENT ON TABLE bio_email IS 'Email addresses for users';
COMMENT ON TABLE bio_name IS 'Name and organization information for VIPs';
COMMENT ON TABLE bio_email_for_vip IS 'Junction table linking emails to VIP users';
COMMENT ON TABLE bio_email_for_vip_tags IS 'Tags associated with email-VIP relationships';

-- Add column comments
COMMENT ON COLUMN bio_vip.id IS 'Unique VIP identifier';
COMMENT ON COLUMN bio_vip.ut_eid IS 'University of Texas EID';
COMMENT ON COLUMN bio_vip.entity_id IS 'Entity type (IN=Individual, CO=Company, UN=Unknown)';
COMMENT ON COLUMN bio_vip.status_id IS 'Status (A=Active, I=Inactive)';

COMMENT ON COLUMN bio_email.address IS 'Email address';
COMMENT ON COLUMN bio_email_for_vip.is_bad IS 'Flag indicating if email is bad (0=good, 1=bad)';

COMMENT ON COLUMN bio_name.first_name IS 'First name';
COMMENT ON COLUMN bio_name.middle_name IS 'Middle name';
COMMENT ON COLUMN bio_name.last_name IS 'Last name';
COMMENT ON COLUMN bio_name.organization_name IS 'Organization name for company entities';

COMMENT ON COLUMN bio_email_for_vip_tags.emailtag_id IS 'Tag type (PRF=Personal, BUS=Business, RES=Residential, ALT=Alternative)';

-- Grant permissions (adjust as needed for your environment)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO your_user;
