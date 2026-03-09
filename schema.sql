-- Cricket Agent Schema
-- Idempotent: all CREATE IF NOT EXISTS

-- Matches table
CREATE TABLE IF NOT EXISTS matches (
    match_id TEXT PRIMARY KEY,
    match_type TEXT NOT NULL CHECK (match_type IN ('ODI', 'T20I')),
    match_date DATE NOT NULL,
    venue TEXT,
    city TEXT,
    team1 TEXT NOT NULL,
    team2 TEXT NOT NULL,
    winner TEXT,
    win_by_runs INT,
    win_by_wickets INT,
    win_method TEXT
);

-- Innings table
CREATE TABLE IF NOT EXISTS innings (
    innings_id SERIAL PRIMARY KEY,
    match_id TEXT NOT NULL REFERENCES matches(match_id),
    innings_number INT NOT NULL,
    batting_team TEXT NOT NULL,
    bowling_team TEXT NOT NULL,
    is_super_over BOOLEAN DEFAULT FALSE,
    UNIQUE (match_id, innings_number)
);

-- Deliveries table
CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id SERIAL PRIMARY KEY,
    match_id TEXT NOT NULL REFERENCES matches(match_id),
    innings_number INT NOT NULL,
    over_number INT NOT NULL,
    ball_number INT NOT NULL,
    batter TEXT NOT NULL,
    bowler TEXT,
    non_striker TEXT,
    runs_batter INT NOT NULL DEFAULT 0,
    runs_extras INT NOT NULL DEFAULT 0,
    runs_total INT NOT NULL DEFAULT 0,
    extras_type TEXT,
    is_wide BOOLEAN DEFAULT FALSE,
    is_noball BOOLEAN DEFAULT FALSE,
    wicket_player_out TEXT,
    wicket_kind TEXT,
    wicket_fielders TEXT[],
    UNIQUE (match_id, innings_number, over_number, ball_number)
);

-- Players table
CREATE TABLE IF NOT EXISTS players (
    player_id SERIAL PRIMARY KEY,
    match_id TEXT NOT NULL REFERENCES matches(match_id),
    team TEXT NOT NULL,
    player_name TEXT NOT NULL,
    UNIQUE (match_id, team, player_name)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_deliveries_batter ON deliveries(batter);
CREATE INDEX IF NOT EXISTS idx_deliveries_bowler ON deliveries(bowler);
CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date);
CREATE INDEX IF NOT EXISTS idx_matches_format ON matches(match_type);
CREATE INDEX IF NOT EXISTS idx_deliveries_wicket ON deliveries(wicket_kind) WHERE wicket_kind IS NOT NULL;

-- Batting career stats view (excludes super overs)
CREATE OR REPLACE VIEW batting_career_stats AS
SELECT
    d.batter AS player,
    m.match_type AS format,
    COUNT(DISTINCT d.match_id) AS matches,
    COUNT(DISTINCT (d.match_id, d.innings_number)) AS innings,
    SUM(d.runs_batter) AS total_runs,
    COUNT(*) FILTER (WHERE d.wicket_player_out = d.batter) AS dismissals,
    CASE
        WHEN COUNT(*) FILTER (WHERE d.wicket_player_out = d.batter) > 0
        THEN ROUND(SUM(d.runs_batter)::NUMERIC / COUNT(*) FILTER (WHERE d.wicket_player_out = d.batter), 2)
        ELSE NULL
    END AS average,
    CASE
        WHEN COUNT(*) FILTER (WHERE d.is_wide = FALSE OR d.is_wide IS NULL) > 0
        THEN ROUND(
            SUM(d.runs_batter)::NUMERIC * 100.0
            / COUNT(*) FILTER (WHERE d.is_wide = FALSE OR d.is_wide IS NULL),
            2
        )
        ELSE NULL
    END AS strike_rate
FROM deliveries d
JOIN matches m USING (match_id)
JOIN innings i ON d.match_id = i.match_id AND d.innings_number = i.innings_number
WHERE i.is_super_over = FALSE
GROUP BY d.batter, m.match_type;

-- Bowling career stats view (excludes run out, retired hurt, obstructing the field from wicket count)
CREATE OR REPLACE VIEW bowling_career_stats AS
SELECT
    d.bowler AS player,
    m.match_type AS format,
    COUNT(*) FILTER (
        WHERE d.wicket_kind IS NOT NULL
        AND d.wicket_kind NOT IN ('run out', 'retired hurt', 'obstructing the field')
    ) AS wickets,
    CASE
        WHEN COUNT(*) FILTER (WHERE d.is_wide = FALSE AND d.is_noball = FALSE) > 0
        THEN ROUND(
            (SUM(d.runs_total) - SUM(CASE WHEN d.extras_type IN ('byes', 'legbyes') THEN d.runs_extras ELSE 0 END))::NUMERIC
            / (COUNT(*) FILTER (WHERE d.is_wide = FALSE AND d.is_noball = FALSE) / 6.0),
            2
        )
        ELSE NULL
    END AS economy,
    CASE
        WHEN COUNT(*) FILTER (
            WHERE d.wicket_kind IS NOT NULL
            AND d.wicket_kind NOT IN ('run out', 'retired hurt', 'obstructing the field')
        ) > 0
        THEN ROUND(
            (SUM(d.runs_total) - SUM(CASE WHEN d.extras_type IN ('byes', 'legbyes') THEN d.runs_extras ELSE 0 END))::NUMERIC
            / COUNT(*) FILTER (
                WHERE d.wicket_kind IS NOT NULL
                AND d.wicket_kind NOT IN ('run out', 'retired hurt', 'obstructing the field')
            ),
            2
        )
        ELSE NULL
    END AS average
FROM deliveries d
JOIN matches m USING (match_id)
JOIN innings i ON d.match_id = i.match_id AND d.innings_number = i.innings_number
WHERE i.is_super_over = FALSE
AND d.bowler IS NOT NULL
GROUP BY d.bowler, m.match_type;

-- Read-only role for the agent's DB connection
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'cricket_reader') THEN
        CREATE ROLE cricket_reader WITH LOGIN PASSWORD 'readonlypass';
    END IF;
END
$$;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cricket_reader;
