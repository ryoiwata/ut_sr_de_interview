

WITH vip_profiles AS (
    SELECT 
        v.id AS vip_id,
        v.ut_eid,
        v.entity_id,
        v.status_id,
        n.first_name,
        n.last_name,
        n.organization_name
    FROM bio_vip v
    FULL OUTER JOIN bio_name n
        ON v.id = n.vip_id
    ),

vip_email_relationships AS (
    SELECT 
        vp.*,
        efv.id AS email_relationship_id,
        efv.email_id,
        efv.is_bad AS email_is_bad
    FROM vip_profiles vp
    FULL OUTER JOIN bio_email_for_vip efv
        ON vp.vip_id = efv.vip_id
),

vip_emails_with_addresses AS (
    SELECT 
        ver.*,
        e.address AS email_address
    FROM vip_email_relationships ver
    FULL OUTER JOIN bio_email e
        ON ver.email_id = e.id
),

vip_emails_with_tags AS (
    SELECT 
        vewa.*,
        et.id AS tag_id,
        et.emailtag_id AS email_tag_type
    FROM vip_emails_with_addresses vewa
    FULL OUTER JOIN bio_email_for_vip_tags et
        ON vewa.email_relationship_id = et.emailforvip_id
)


-- more_than_email_CTE AS (
-- SELECT ut_eid, first_name, last_name, COUNT(*) FROM vip_emails_with_tags
-- GROUP BY ut_eid, first_name, last_name
-- HAVING COUNT(*) >= 2
-- ORDER BY COUNT(*) DESC)

-- SELECT *, first_name || ' ' || last_name AS full_name FROM more_than_email_CTE

-- SELECT * FROM vip_emails_with_tags
-- WHERE status_id = 'A'
-- AND email_is_bad = 0 
-- CASE 
--     WHEN email_tag_type = 'PRF' THEN 1
--     ELSE CASE 

SELECT * FROM vip_emails_with_tags
WHERE email_address NOT LIKE '%@%'


-- SELECT 
--     vip_id,
--     ut_eid,
--     entity_id,
--     status_id,
--     first_name,
--     last_name,
--     organization_name,
--     email_relationship_id,
--     email_id,
--     email_address,
--     email_is_bad,
--     tag_id,
--     email_tag_type
-- FROM vip_emails_with_tags
