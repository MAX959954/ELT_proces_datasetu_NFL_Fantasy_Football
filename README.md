# ELT proces datasetu NFL Fantasy Football
## Maksym Veselov 

Tento repozitár predstavuje implementáciu ELT procesu v Snowflake a vytvorenie dátového skladu so schémou Star Schema. Projekt pracuje s datasetom NFL Fantasy Football 2022 zo Snowflake Marketplace. Projekt sa zameriava na analýzu herných štatistík amerického futbalu, výkonnosti hráčov a tímov počas sezóny 2022.

## Obsah

- [Úvod a popis zdrojových dát](#úvod-a-popis-zdrojových-dát)
- [Návrh dimenzionálneho modelu](#návrh-dimenzionálneho-modelu)
- [ELT proces v Snowflake](#elt-proces-v-snowflake)
- [Vizualizácia dát](#vizualizácia-dát)
- [Záver](#záver)

---

## Úvod a popis zdrojových dát

### Účel analýzy

Dataset bol vybraný pre potreby analýzy výkonnosti hráčov a tímov v americkom futbale NFL. Analyzujeme dáta zo sezóny 2022 s cieľom porozumieť:

- Výkonnosti jednotlivých hráčov (quarterbackov, running backov, wide receiverov)
- Herným stratégiám tímov (pomer pasov vs. behov)
- Trendov vo výkone počas sezóny
- Faktorov ovplyvňujúcich výsledky zápasov

### Biznis proces

Dáta podporujú nasledujúce biznis procesy:

- Fantasy Football Analytics - vyhodnocovanie výkonnosti hráčov pre fantasy ligy
- Team Performance Analysis - analýza taktík a stratégií tímov
- Player Scouting - identifikácia talentovaných hráčov pre nábor
- Game Strategy Planning - optimalizácia herných plánov na základe historických dát

### Zdrojové dáta

Zdrojové dáta pochádzajú z Snowflake Marketplace datasetu:

- Databáza: NFL_FANTASY_FOOTBALL
- Schéma: NFL2022
- Sezóna: 2022

Dataset obsahuje sedem hlavných tabuliek:

| Tabuľka | Popis | Význam |
|---------|-------|--------|
| PBP | Play-by-play dáta všetkých hier v sezóne | Centrálna tabuľka s detailmi každej hry |
| PBP_PASSING | Štatistiky pasovacích hier | Dáta o quarterbackoch a pasovacích pokusoch |
| PBP_RECEIVING | Štatistiky prijímacích hier | Výkony wide receiverov a tight endov |
| PBP_RUSHING | Štatistiky bežeckých hier | Výkony running backov |
| ROSTER | Informácie o hráčoch | Demografické údaje, tím, pozícia |
| SCHEDULE | Herný rozvrh a výsledky | Dátumy zápasov, skóre, podmienky |
| TEAMS | Informácie o tímoch NFL | Názvy, divízie, farby tímov |

### ERD diagram pôvodnej štruktúry

Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na entitno-relačnom diagrame:

<img width="882" height="1401" alt="Erd_schema" src="https://github.com/user-attachments/assets/7320b820-ba35-4e54-bf5f-7319ed4a92ec" />

*Obrázok 1: Entitno-relačná schéma zdrojových dát*

---

## Návrh dimenzionálneho modelu

### Star Schema

Navrhnutá schéma hviezdy (Star Schema) obsahuje 1 faktovú tabuľku FACT_PBP prepojenú s 6 dimenziami:

<img width="897" height="1401" alt="Star_schema" src="https://github.com/user-attachments/assets/a925bf7f-eb8c-4453-8c36-530c2be3d639" />


*Obrázok 2: Schéma hviezdy pre NFL Fantasy Football dataset*

### Dimenzie

#### DIM_TEAMS (SCD Type 0)

Účel: Katalóg všetkých 32 NFL tímov s ich metadátami

Atribúty:
- team_abbr (PK) - skratka tímu (napr. KC, SF, PHI)
- team_name - celý názov tímu
- team_conf - konferencia (AFC/NFC)
- team_division - divízia (East, West, North, South)
- team_color, team_color2 - oficiálne farby tímu

SCD Type 0: Statická referenčná tabuľka, údaje sa nemenia

Vzťah: 1:N k faktovej tabuľke (jeden tím má mnoho hier)

#### DIM_ROSTER (SCD Type 1)

Účel: Katalóg všetkých hráčov v NFL sezóne 2022

Atribúty:
- gsis_id (PK) - jedinečný identifikátor hráča
- full_name - celé meno hráča
- position - herná pozícia (QB, RB, WR, TE, atď.)
- team - tím hráča
- height, weight - fyzické parametre
- college - univerzita
- years_exp - roky skúseností

SCD Type 1: Jednoduché prepisovanie pri zmenách (napr. zmena tímu) bez histórie

Vzťah: 1:N k play-specific dimenziám (jeden hráč má mnoho hier)

#### DIM_SCHEDULE (SCD Type 0)

Účel: Informácie o zápasoch a ich podmienkach

Atribúty:
- game_id (PK) - jedinečný identifikátor zápasu
- season, week - sezóna a týždeň
- gameday, gametime - dátum a čas
- home_team, away_team - domáci a hosťujúci tím
- home_score, away_score - výsledné skóre
- stadium - názov štadióna
- temp, wind, roof, surface - podmienky zápasu

SCD Type 0: Historické údaje, nemenia sa po zápase

Vzťah: 1:N k faktovej tabuľke (jeden zápas má mnoho hier) 

#### DIM_PBP_PASSING (SCD Type 0)

Účel: Detailné štatistiky pasovacích hier

Atribúty:
- pbp_id (PK) - identifikátor hry
- gsis_id (FK) - odkaz na hráča (QB)
- yards_gained - získané yardy pasom
- touchdown - indikátor touchdown
- interception - indikátor interceptu
- sack - indikátor sacku
- incomplete_pass - nedokončený pas

SCD Type 0: Historické herné dáta, nemenné

Vzťah: 1:1 k faktovej tabuľke, N:1 k DIM_ROSTER

#### DIM_PBP_RECEIVING (SCD Type 0)

Účel: Detailné štatistiky prijímacích hier

Atribúty:
- pbp_id (PK) - identifikátor hry
- gsis_id (FK) - odkaz na hráča (WR/TE)
- yards_gained - získané yardy chytením
- touchdown - indikátor touchdown
- target - indikátor cielenia
- fumble_lost - stratený fumble

SCD Type 0: Historické herné dáta, nemenné

Vzťah: 1:1 k faktovej tabuľke, N:1 k DIM_ROSTER

#### DIM_PBP_PUSHING (SCD Type 0)

Účel: Detailné štatistiky bežeckých hier

Atribúty:
- pbp_id (PK) - identifikátor hry
- gsis_id (FK) - odkaz na hráča (RB)
- yards_gained - získané yardy behom
- touchdown - indikátor touchdown
- fumble_lost - stratený fumble

SCD Type 0: Historické herné dáta, nemenné

Vzťah: 1:1 k faktovej tabuľke, N:1 k DIM_ROSTER

### Faktová tabuľka FACT_PBP

Primárny kľúč: pbp_id (VARCHAR)

Cudzie kľúče:
- game_id → DIM_SCHEDULE
- home_team → DIM_TEAMS
- away_team → DIM_TEAMS
- posteam → DIM_TEAMS (tím s loptou)
- defteam → DIM_TEAMS (brániaci tím)

Metriky a atribúty:
- play_id - poradové číslo hry
- week - týždeň sezóny
- game_date - dátum zápasu
- qtr - štvrťčas
- down - down (1-4)
- play_type - typ hry (pass, run, punt, atď.)
- score_differential - rozdiel v skóre
- play_sequence - poradie hry v zápase (ROW_NUMBER)
- previous_score - skóre z predchádzajúcej hry (LAG)

Window Functions:
- ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY play_id) - číslovanie hier v rámci zápasu
- LAG(score_differential) OVER (PARTITION BY game_id ORDER BY play_id) - získanie skóre z predchádzajúcej hry pre analýzu momentum

---

## ELT proces v Snowflake

### Extract (Extrahovanie dát)

Dáta boli extrahované zo Snowflake Marketplace do staging schémy.

Vytvorenie staging tabuliek:

USE WAREHOUSE VIPER_WH;
USE DATABASE VIPER_DB;
CREATE OR REPLACE SCHEMA STAGING;

-- Staging: PBP
```
CREATE OR REPLACE TABLE STG_PBP AS
SELECT * FROM NFL_FANTASY_FOOTBALL.NFL2022.PBP;
```

-- Staging: PBP_PASSING
```
CREATE OR REPLACE TABLE STG_PBP_PASSING AS
SELECT * FROM NFL_FANTASY_FOOTBALL.NFL2022.PBP_PASSING;
```

-- Staging: PBP_RECEIVING
```
CREATE OR REPLACE TABLE STG_PBP_RECEIVING AS
SELECT * FROM NFL_FANTASY_FOOTBALL.NFL2022.PBP_RECEIVING;
```

-- Staging: ROSTER
```
CREATE OR REPLACE TABLE STG_ROSTER AS
SELECT * FROM NFL_FANTASY_FOOTBALL.NFL2022.ROSTER;
```

-- Staging: SCHEDULE
```
CREATE OR REPLACE TABLE STG_SCHEDULE AS
SELECT * FROM NFL_FANTASY_FOOTBALL.NFL2022.SCHEDULE;
```

-- Staging: TEAMS
```
CREATE OR REPLACE TABLE STG_TEAMS AS
SELECT * FROM NFL_FANTASY_FOOTBALL.NFL2022.TEAMS;
```


--Staging: PBP_PUSHING
```
CREATE OR REPLACE TABLE STG_PBP_PUSHING AS
SELECT *
FROM NFL_FANTASY_FOOTBALL.NFL2022.PBP_RUSHING;
```

Účel: Staging tabuľky slúžia ako dočasné úložisko surových dát pred ich transformáciou a načítaním do dimenzionálneho modelu.

### Transform (Transformácia dát)

Hlavné transformácie zahŕňali:

- Čistenie dát: Použitie COALESCE() pre nahradenie NULL hodnôt predvolenými hodnotami
- Deduplikácia: Odstránenie duplicitných záznamov pomocou DISTINCT
- Validácia: Filtrovanie záznamov s platnými kľúčmi (WHERE pbp_id IS NOT NULL)
- Typové konverzie: Konverzia boolean hodnôt (0/1) pomocí CASE výrazov
- Výpočet metrík: Vytvorenie odvodzených metrík pomocou window functions

### Load (Načítanie dát)

#### Vytvorenie dimenzií

DIM_TEAMS - Katalóg tímov:

```
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
FROM STAGING.STG_TEAMS;
```

DIM_ROSTER - Katalóg hráčov:

```
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
```

DIM_SCHEDULE - Rozvrh zápasov:

```
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
    away_score,
    home_score,
    result,
    total,
    overtime,
    location,
    roof,
    surface,
    temp,
    wind,
    stadium_id,
    stadium,
    away_rest,
    home_rest,
    away_moneyline,
    home_moneyline,
    spread_line,
    away_spread_odds,
    home_spread_odds,
    total_line,
    under_odds,
    over_odds,
    div_game,
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
    espn
FROM STAGING.STG_SCHEDULE;
```

DIM_PBP_PASSING, DIM_PBP_RECEIVING, DIM_PBP_PUSHING:
```
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

INSERT INTO DIM_PBP_RUSHING
SELECT DISTINCT
    PBP_ID,
    gsis_id,
    COALESCE(yards_gained, 0),
    COALESCE(touchdown, 0),
    COALESCE(FUMBLE_LOST, 0)
FROM VIPER_DB.STAGING.STG_PBP
WHERE RUSHER_PLAYER_ID IS NOT NULL;
```

#### Vytvorenie faktovej tabuľky s Window Functions

```
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
```

Vysvetlenie Window Functions:

- ROW_NUMBER(): Každá hra v zápase dostáva sekvenčné číslo (1, 2, 3...), čo umožňuje analýzu priebehu zápasu
- LAG(): Získanie score_differential z predchádzajúcej hry umožňuje sledovanie momentum a zmien v skóre počas zápasu

---

## Vizualizácia dát

Dashboard obsahuje 5 vizualizácií poskytujúcich komplexný prehľad o výkonnosti hráčov a tímov.

<img width="1165" height="567" alt="image" src="https://github.com/user-attachments/assets/e28ccb19-5eee-46c5-ba08-e0c8a07b27a5" />

<img width="1175" height="593" alt="image" src="https://github.com/user-attachments/assets/4a994bdc-a41f-47ce-adb0-38f82abb9ee6" />

<img width="1173" height="556" alt="image" src="https://github.com/user-attachments/assets/20dd3aa9-24e5-41c8-85c3-fdb9dd5545d5" />

<img width="1181" height="586" alt="image" src="https://github.com/user-attachments/assets/29b6ad38-5253-4008-a731-79d5d4fa723f" />

<img width="1476" height="595" alt="image" src="https://github.com/user-attachments/assets/00d546ce-c701-4cc1-a949-3f55e6b37182" />


*Obrázok 3: Dashboard s vizualizáciami NFL štatistík*

### Vizualizácia 1: Top passers by total yards and efficiency

SQL dotaz:

```
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
HAVING COUNT(*) >= 50  -- Minimum attempts
ORDER BY total_passing_yards DESC
LIMIT 10;
```

Interpretácia: Tento SQL dotaz identifikuje 10 najlepších quarterbackov podľa celkových pasovacích yardov, pričom zároveň analyzuje ich efektivitu prostredníctvom kľúčových metrík.


### Vizualizácia 2: Top receivers by receptions and yards 

SQL dotaz:

```
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
```

Interpretácia:  Tento SQL dotaz identifikuje 10 najlepších prijímačov podľa celkového počtu chytených prihrávok (receptions), pričom zároveň analyzuje ich produktivitu a efektivitu v prijímacej hre.


### Vizualizácia 3: Games with Most Total Plays

SQL dotaz:

```
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
```

Interpretácia:Tento SQL dotaz identifikuje 10 zápasov s najvyšším celkovým počtom odohraných akcií (plays) v sezóne. Analýza poskytuje pohľad na tempo hry, dĺžku zápasov a intenzitu súbojov medzi tímami.


### Vizualizácia 4: Players by position and experience

SQL dotaz: 
```
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
```

Interpretácia: Tento SQL dotaz identifikuje 10 najlepších prijímačov podľa celkových receiving yardov, čo je jeden z najdôležitejších ukazovateľov ofenzívnej produktivity v NFL. Analýza kombinuje objemové štatistiky s informáciami o tíme každého hráča.


### Vizualizácia 5: Top Receivers by Receiving Yards

SQL dotaz:

```
sql
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
```

Interpretácia:Tento SQL dotaz analyzuje skladbu hráčskych káderov NFL tímov podľa pozícií, skúseností a fyzických charakteristík. Poskytuje komplexný pohľad na roster construction a team building stratégie jednotlivých organizácií.

Tento horizontálny stĺpcový graf zobrazuje 10 najlepších prijímačov sezóny podľa celkových receiving yardov. Graf odhaľuje najproduktívnejších hráčov v prijímacej hre a ich ofenzívny dopad na úspech tímov.

## Záver

### Zhrnutie projektu

Tento projekt úspešne implementoval komplexný ELT proces v Snowflake pre analýzu NFL Fantasy Football datasetu zo Snowflake Marketplace. Výsledný dimenzionálny model typu Star Schema umožňuje efektívne analytické dotazy na sledovanie výkonnosti hráčov, tímov a herných stratégií.

### Kľúčové výsledky

- Úspešne vytvorená Star Schema s 1 faktovou tabuľkou a 6 dimenziami
- Implementované window functions (ROW_NUMBER, LAG) pre pokročilú analýzu hier
- Vytvorených 5 vizualizácií poskytujúcich prehľad o kľúčových metrikách
- Plne funkčný dátový sklad pripravený na ad-hoc analytické dotazy





