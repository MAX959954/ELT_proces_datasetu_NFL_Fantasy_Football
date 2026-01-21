
SELECT 
    p.gsis_id,
    r.full_name,
    r.team,
    COUNT(*) as pass_attempts,
    SUM(p.yards_gained) as total_passing_yards,
    SUM(p.touchdown) as total_passing_tds,
    SUM(p.interception) as total_interceptions,
    SUM(p.sack) as total_sacks,
    ROUND(SUM(p.yards_gained)::DECIMAL / COUNT(*), 2) as yards_per_attempt,
    ROUND((SUM(p.touchdown)::DECIMAL / COUNT(*)) * 100, 2) as td_percentage
FROM DIM_PBP_PASSING p
LEFT JOIN DIM_ROSTER r ON p.gsis_id = r.gsis_id
WHERE p.gsis_id IS NOT NULL
GROUP BY p.gsis_id, r.full_name, r.team
HAVING COUNT(*) >= 50  
ORDER BY total_passing_yards DESC
LIMIT 10;

SELECT 
    rec.gsis_id,
    r.full_name,
    r.team,
    r.position,
    COUNT(*) as receptions,
    SUM(rec.yards_gained) as total_receiving_yards,
    SUM(rec.touchdown) as total_receiving_tds,
    ROUND(SUM(rec.yards_gained)::DECIMAL / COUNT(*), 2) as yards_per_reception,
    SUM(rec.incomplete_pass) as targets_missed
FROM DIM_PBP_RECEVING rec
LEFT JOIN DIM_ROSTER r ON rec.gsis_id = r.gsis_id
WHERE rec.gsis_id IS NOT NULL
GROUP BY rec.gsis_id, r.full_name, r.team, r.position
ORDER BY receptions DESC
LIMIT 10;

SELECT 
    position,
    team,
    COUNT(*) as player_count,
    ROUND(AVG(years_exp), 2) as avg_experience,
    ROUND(AVG(height), 2) as avg_height_inches,
    ROUND(AVG(weight), 2) as avg_weight_lbs,
    COUNT(CASE WHEN status = 'ACT' THEN 1 END) as active_players
FROM DIM_ROSTER
WHERE position IN ('QB', 'RB', 'WR', 'TE', 'OL', 'DL', 'LB', 'DB')
GROUP BY position, team
ORDER BY position, player_count DESC;

SELECT 
    r.full_name as receiver,
    t.team_name as team,
    COUNT(*) as targets,
    SUM(recv.yards_gained) as total_yards,
    SUM(recv.touchdown) as touchdowns
FROM DIM_PBP_RECEVING recv
JOIN DIM_ROSTER r ON recv.gsis_id = r.gsis_id
JOIN DIM_TEAMS t ON r.team = t.team_abbr
GROUP BY r.full_name, t.team_name
ORDER BY total_yards DESC
LIMIT 10;

SELECT 
    s.week,
    s.gameday,
    home.team_name as home_team,
    away.team_name as away_team,
    s.home_score,
    s.away_score,
    COUNT(f.pbp_id) as total_plays
FROM FACT_PBP f
JOIN DIM_SCHEDULE s ON f.game_id = s.game_id
JOIN DIM_TEAMS home ON s.home_team = home.team_abbr
JOIN DIM_TEAMS away ON s.away_team = away.team_abbr
GROUP BY s.week, s.gameday, home.team_name, away.team_name, s.home_score, s.away_score
ORDER BY total_plays DESC
LIMIT 10;





