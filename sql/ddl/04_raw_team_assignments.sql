-- Raw landing table for the HR export.
-- Grain: one row per user x team assignment. Fully replaced on every load.
CREATE TABLE IF NOT EXISTS raw_team_assignments (
    user_name  VARCHAR NOT NULL,
    team_name  VARCHAR NOT NULL,
    valid_from DATE    NOT NULL   -- first day (UTC) the user belongs to the team
);