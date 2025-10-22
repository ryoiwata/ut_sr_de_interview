

WITH vip_name_combined AS (
SELECT 
    v.id AS vip_id,
    v.ut_eid,
    v.created_on AS vip_created_on,
    v.updated_on AS vip_updated_on,
    n.first_name,
    n.middle_name,
    n.last_name,
    n.organization_name,
    n.created_on AS name_created_on,
    n.updated_on AS name_updated_on

FROM bio_vip v
FULL OUTER JOIN bio_name n
    ON v.id = n.vip_id
),
email_combined AS (
SELECT * FROM vip_name_combined
FULL OUTER JOIN bio_email_for_vip efv
    ON vip_name_combined.vip_id = efv.vip_id)

SELECT * FROM email_combined 
FULL OUTER JOIN bio_email 
ON email_combined.email_id = bio_email.id



--  FROM bio_vip
-- FULL OUTER JOIN bio_name 
-- ON bio_vip.id = bio_name.vip_id



-- ID,	
-- FIRST_NAME,
-- MIDDLE_NAME,
-- LAST_NAME,
-- ORGANIZATION_NAME,
-- VIP_ID,
-- CREATED_ON,
-- UPDATED_ON,
-- CREATED_BY_ID,
-- UPDATED_BY_ID