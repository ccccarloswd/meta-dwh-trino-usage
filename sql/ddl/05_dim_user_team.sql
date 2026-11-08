-- Team membership over time, derived from the HR export.
-- Grain: one row per user x validity period [valid_from, valid_to).
-- valid_to is the next assignment's valid_from; the current row ends on 9999-12-31.
CREATE OR REPLACE VIEW dim_user_team AS
SELECT
    user_name,
    team_name,
    valid_from,
    COALESCE(
        LEAD(valid_from) OVER (PARTITION BY user_name ORDER BY valid_from),
        DATE '9999-12-31'
    ) AS valid_to,
    LEAD(valid_from) OVER (PARTITION BY user_name ORDER BY valid_from) IS NULL AS is_current
FROM raw_team_assignments;