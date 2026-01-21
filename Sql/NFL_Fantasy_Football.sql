USE WAREHOUSE VIPER_WH;
USE DATABASE VIPER_DB;

CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS PUBLIC;

USE SCHEMA STAGING;

CREATE OR REPLACE TABLE STG_PBP AS
SELECT *
FROM NFL_FANTASY_FOOTBALL.NFL2022.PBP;

CREATE OR REPLACE TABLE STG_PBP_PASSING AS
SELECT *
FROM NFL_FANTASY_FOOTBALL.NFL2022.PBP_PASSING;

CREATE OR REPLACE TABLE STG_PBP_RECEIVING AS
SELECT *
FROM NFL_FANTASY_FOOTBALL.NFL2022.PBP_RECEIVING;

CREATE OR REPLACE TABLE STG_ROSTER AS
SELECT *
FROM NFL_FANTASY_FOOTBALL.NFL2022.ROSTER;

CREATE OR REPLACE TABLE STG_SCHEDULE AS
SELECT *
FROM NFL_FANTASY_FOOTBALL.NFL2022.SCHEDULE;

CREATE OR REPLACE TABLE STG_TEAMS AS
SELECT *
FROM NFL_FANTASY_FOOTBALL.NFL2022.TEAMS;

CREATE OR REPLACE TABLE STG_PBP_PUSHING AS
SELECT *
FROM NFL_FANTASY_FOOTBALL.NFL2022.PBP_RUSHING;

SHOW TABLES IN VIPER_DB.STAGING;

USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE FACT_PBP (
    pbp_id VARCHAR(100) PRIMARY KEY,
    play_id INT NOT NULL,
    game_id VARCHAR(50) NOT NULL,
    home_team VARCHAR(3) NOT NULL,
    away_team VARCHAR(3) NOT NULL,
    posteam VARCHAR(3),  
    defteam VARCHAR(3),  
    week INT NOT NULL,
    game_date DATE NOT NULL, 
    qtr INT,  
    down INT, 
    play_type VARCHAR(50),  
    desc TEXT, 
    score_differential INT ,
    play_sequence INT,
    previous_score INT
);

INSERT INTO FACT_PBP
SELECT 
    pbp_id,
    play_id,
    game_id,
    home_team,
    away_team,
    posteam,
    defteam,
    week,
    game_date,
    qtr,
    down,
    play_type,
    desc,
    score_differential,
    
    
    ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY play_id) as play_sequence,
    LAG(score_differential) OVER (PARTITION BY game_id ORDER BY play_id) as previous_score
FROM STG_PBP;

CREATE OR REPLACE TABLE DIM_PBP_PASSING (
    pbp_id VARCHAR(100) PRIMARY KEY,
    gsis_id VARCHAR(20),
    yards_gained INT DEFAULT 0,
    touchdown INT DEFAULT 0, 
    incomplete_pass INT DEFAULT 0, 
    fumble_lost INT DEFAULT 0,  
    interception INT DEFAULT 0,  
    sack INT DEFAULT 0
);

INSERT INTO DIM_PBP_PASSING
SELECT DISTINCT
    PBP_ID,
    gsis_id,
    COALESCE(yards_gained, 0),
    COALESCE(touchdown, 0),
    COALESCE(incomplete_pass, 0),
    COALESCE(FUMBLE_LOST, 0),
    COALESCE(INTERCEPTION, 0),
    COALESCE(SACK, 0)
FROM VIPER_DB.STAGING.STG_PBP_PASSING  
WHERE gsis_id IS NOT NULL;


CREATE OR REPLACE TABLE DIM_PBP_RECEVING (
    pbp_id VARCHAR(100) PRIMARY KEY,
    gsis_id VARCHAR(20),
    yards_gained INT DEFAULT 0,
    touchdown INT DEFAULT 0, 
    incomplete_pass INT DEFAULT 0,  
    fumble_lost INT DEFAULT 0, 
    interception INT DEFAULT 0,  
    target INT DEFAULT 0
);

INSERT INTO DIM_PBP_RECEVING
SELECT 
    pbp_id,
    gsis_id,
    COALESCE(yards_gained, 0) ,
    COALESCE(touchdown, 0) , 
    COALESCE(incomplete_pass, 0) ,
    COALESCE(fumble_lost, 0) ,
    COALESCE(interception, 0) ,
    1 as target 
FROM STAGING.STG_PBP_RECEIVING
WHERE gsis_id IS NOT NULL;


CREATE OR REPLACE TABLE DIM_PBP_RUSHING (
    pbp_id VARCHAR(100) PRIMARY KEY,
    gsis_id VARCHAR(20),
    yards_gained INT DEFAULT 0,  
    touchdown INT DEFAULT 0, 
    fumble_lost INT DEFAULT 0
);

INSERT INTO DIM_PBP_RUSHING
SELECT DISTINCT
    PBP_ID,
    gsis_id,
    COALESCE(yards_gained, 0),
    COALESCE(touchdown, 0),
    COALESCE(FUMBLE_LOST, 0)
FROM VIPER_DB.STAGING.STG_PBP
WHERE RUSHER_PLAYER_ID IS NOT NULL;


CREATE OR REPLACE TABLE DIM_ROSTER (
    gsis_id VARCHAR(20) PRIMARY KEY,
    full_name VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    birth_date DATE,
    height INT,  
    weight INT,  
    college VARCHAR(100),
    espn_id VARCHAR(20),
    yahoo_id VARCHAR(20),
    rotowire_id VARCHAR(20),
    pff_id VARCHAR(20),
    fantasy_data_id VARCHAR(20),
    sleeper_id VARCHAR(20),
    sportradar_id VARCHAR(100),
    years_exp INT, 
    season INT,
    team VARCHAR(3),
    position VARCHAR(10),
    depth_chart_position VARCHAR(10),
    jersey_number INT,
    status VARCHAR(20), 
    headshot_url TEXT
);


INSERT INTO DIM_ROSTER
SELECT 
    gsis_id,
    full_name,
    first_name,
    last_name,
    birth_date,
    height,
    weight,
    college,
    espn_id,
    yahoo_id,
    rotowire_id,
    pff_id,
    fantasy_data_id,
    sleeper_id,
    sportradar_id,
    years_exp,
    season,
    team,
    position,
    depth_chart_position,
    jersey_number,
    status,
    headshot_url
FROM STAGING.STG_ROSTER;

CREATE OR REPLACE TABLE DIM_SCHEDULE (
    game_id VARCHAR(50) PRIMARY KEY,
    season INT NOT NULL,
    game_type VARCHAR(10),  
    week INT,
    gameday DATE,
    weekday VARCHAR(10),
    gametime TIME,
    

    away_team VARCHAR(3) NOT NULL,
    home_team VARCHAR(3) NOT NULL,
    
    away_score INT,
    home_score INT,
    result INT,  
    total INT,  
    overtime INT, 
    
    
    location VARCHAR(20),  
    roof VARCHAR(20),  
    surface VARCHAR(20),  
    temp INT, 
    wind INT, 
    
    stadium_id VARCHAR(10),
    stadium VARCHAR(100),
    
    
    away_rest INT,
    home_rest INT, 
    away_moneyline INT,
    home_moneyline INT,
    spread_line DECIMAL(4,1),
    away_spread_odds INT,
    home_spread_odds INT,
    total_line DECIMAL(4,1),
    under_odds INT,
    over_odds INT,
    div_game INT, 
    
    away_qb_id VARCHAR(20),
    home_qb_id VARCHAR(20),
    away_qb_name VARCHAR(100),
    home_qb_name VARCHAR(100),
    away_coach VARCHAR(100),
    home_coach VARCHAR(100),
    referee VARCHAR(100),
    
    old_game_id VARCHAR(20),
    gsis VARCHAR(20),
    nfl_detail_id VARCHAR(20),
    pfr VARCHAR(20),
    pff VARCHAR(20),
    espn INT
);

INSERT INTO DIM_SCHEDULE
SELECT 
    game_id,
    season,
    game_type,
    week,
    gameday,
    weekday,
    gametime,
    
    away_team,
    home_team,
    
    -- Handle 'NA' values in numeric columns
    TRY_CAST(NULLIF(away_score, 'NA') AS INT),
    TRY_CAST(NULLIF(home_score, 'NA') AS INT),
    TRY_CAST(NULLIF(result, 'NA') AS INT),
    TRY_CAST(NULLIF(total, 'NA') AS INT),
    TRY_CAST(NULLIF(overtime, 'NA') AS INT),
    
    location,
    roof,
    surface,
    TRY_CAST(NULLIF(temp, 'NA') AS INT),
    TRY_CAST(NULLIF(wind, 'NA') AS INT),
    
    stadium_id,
    stadium,
    
    TRY_CAST(NULLIF(away_rest, 'NA') AS INT),
    TRY_CAST(NULLIF(home_rest, 'NA') AS INT),
    TRY_CAST(NULLIF(away_moneyline, 'NA') AS INT),
    TRY_CAST(NULLIF(home_moneyline, 'NA') AS INT),
    TRY_CAST(NULLIF(spread_line, 'NA') AS DECIMAL(4,1)),
    TRY_CAST(NULLIF(away_spread_odds, 'NA') AS INT),
    TRY_CAST(NULLIF(home_spread_odds, 'NA') AS INT),
    TRY_CAST(NULLIF(total_line, 'NA') AS DECIMAL(4,1)),
    TRY_CAST(NULLIF(under_odds, 'NA') AS INT),
    TRY_CAST(NULLIF(over_odds, 'NA') AS INT),
    TRY_CAST(NULLIF(div_game, 'NA') AS INT),
    
    away_qb_id,
    home_qb_id,
    away_qb_name,
    home_qb_name,
    away_coach,
    home_coach,
    referee,
    
    old_game_id,
    gsis,
    nfl_detail_id,
    pfr,
    pff,
    TRY_CAST(NULLIF(espn, 'NA') AS INT)
FROM VIPER_DB.STAGING.STG_SCHEDULE;

CREATE OR REPLACE TABLE DIM_TEAMS (
    team_abbr VARCHAR(3) PRIMARY KEY,
    team_name VARCHAR(100) NOT NULL,
    team_id VARCHAR(10),
    team_nick VARCHAR(50),
    team_conf VARCHAR(3),  
    team_division VARCHAR(20), 
    
    team_color VARCHAR(7),
    team_color2 VARCHAR(7),
    team_color3 VARCHAR(7),
    team_color4 VARCHAR(7)
);

INSERT INTO DIM_TEAMS 
SELECT 
    team_abbr,
    team_name,
    team_id,
    team_nick,
    team_conf,
    team_division,
    team_color,
    team_color2,
    team_color3,
    team_color4
FROM STG_TEAMS;

CREATE OR REPLACE TABLE DIM_PBP_PUSHING (
    pbp_id VARCHAR(100),  
    gsis_id VARCHAR(45),
    yards_gained VARCHAR(45),
    touchdown VARCHAR(45),
    fumble_lost VARCHAR(45)
);

INSERT INTO DIM_PBP_PUSHING
SELECT 
    pbp_id,
    gsis_id,
    yards_gained,
    touchdown,
    fumble_lost
FROM NFL_FANTASY_FOOTBALL.NFL2022.PBP_RUSHING
WHERE gsis_id IS NOT NULL;  
























