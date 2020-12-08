# Create table in Hive

## master dataset
drop table if exists yahancheng_final_master_csv;
create external table if not exists yahancheng_final_master_csv (
    gameId bigint, gameDuraton bigint,
    blueWins bigint, blueFirstBlood bigint,
    blueFirstTower bigint, blueFirstBaron bigint,
    blueFirstDragon bigint, blueFirstInhibitor bigint,
    blueDragonKills bigint, blueBaronKills bigint,
    blueTowerKills bigint, blueInhibitorKills bigint,
    blueWardPlaced bigint, blueWardkills bigint,
    blueKills bigint, blueDeath bigint,
    blueAssist bigint, blueChampionDamageDealt bigint,
    blueTotalGold bigint, blueTotalMinionKills bigint,
    blueTotalLevel bigint,blueAvgLevel bigint,
    blueJungleMinionKills bigint, blueKillingSpree bigint,
    blueTotalHeal bigint, blueObjectDamageDealt bigint,
    redWins bigint, redFirstBlood bigint,
    redFirstTower bigint, redFirstBaron bigint,
    redFirstDragon bigint, redFirstInhibitor bigint,
    redDragonKills bigint, redBaronKills bigint,
    redTowerKills bigint, redInhibitorKills bigint,
    redWardPlaced bigint, redWardkills bigint,
    redKills bigint, redDeath bigint,
    redAssist bigint, redChampionDamageDealt bigint,
    redTotalGold bigint, redTotalMinionKills bigint,
    redTotalLevel bigint, redAvgLevel bigint,
    redJungleMinionKills bigint, redKillingSpree bigint,
    redTotalHeal bigint, redObjectDamageDealt bigint)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    lines terminated by '\n'
    tblproperties("skip.header.line.count"="1");

load data inpath 'hdfs:///tmp/yahancheng/final/master.csv' into table yahancheng_final_master_csv;

create table yahancheng_final_master (
    gameId bigint, gameDuraton bigint,
    blueWins bigint, blueFirstBlood bigint,
    blueFirstTower bigint, blueFirstBaron bigint,
    blueFirstDragon bigint, blueFirstInhibitor bigint,
    blueDragonKills bigint, blueBaronKills bigint,
    blueTowerKills bigint, blueInhibitorKills bigint,
    blueWardPlaced bigint, blueWardkills bigint,
    blueKills bigint, blueDeath bigint,
    blueAssist bigint, blueChampionDamageDealt bigint,
    blueTotalGold bigint, blueTotalMinionKills bigint,
    blueTotalLevel bigint,blueAvgLevel bigint,
    blueJungleMinionKills bigint, blueKillingSpree bigint,
    blueTotalHeal bigint, blueObjectDamageDealt bigint,
    redWins bigint, redFirstBlood bigint,
    redFirstTower bigint, redFirstBaron bigint,
    redFirstDragon bigint, redFirstInhibitor bigint,
    redDragonKills bigint, redBaronKills bigint,
    redTowerKills bigint, redInhibitorKills bigint,
    redWardPlaced bigint, redWardkills bigint,
    redKills bigint, redDeath bigint,
    redAssist bigint, redChampionDamageDealt bigint,
    redTotalGold bigint, redTotalMinionKills bigint,
    redTotalLevel bigint, redAvgLevel bigint,
    redJungleMinionKills bigint, redKillingSpree bigint,
    redTotalHeal bigint, redObjectDamageDealt bigint)
    stored as orc;

insert overwrite table yahancheng_final_master select * from yahancheng_final_master_csv;

## grandMaster dataset
create external table if not exists yahancheng_final_grandMaster_csv (
    gameId bigint, gameDuraton bigint,
    blueWins bigint, blueFirstBlood bigint,
    blueFirstTower bigint, blueFirstBaron bigint,
    blueFirstDragon bigint, blueFirstInhibitor bigint,
    blueDragonKills bigint, blueBaronKills bigint,
    blueTowerKills bigint, blueInhibitorKills bigint,
    blueWardPlaced bigint, blueWardkills bigint,
    blueKills bigint, blueDeath bigint,
    blueAssist bigint, blueChampionDamageDealt bigint,
    blueTotalGold bigint, blueTotalMinionKills bigint,
    blueTotalLevel bigint,blueAvgLevel bigint,
    blueJungleMinionKills bigint, blueKillingSpree bigint,
    blueTotalHeal bigint, blueObjectDamageDealt bigint,
    redWins bigint, redFirstBlood bigint,
    redFirstTower bigint, redFirstBaron bigint,
    redFirstDragon bigint, redFirstInhibitor bigint,
    redDragonKills bigint, redBaronKills bigint,
    redTowerKills bigint, redInhibitorKills bigint,
    redWardPlaced bigint, redWardkills bigint,
    redKills bigint, redDeath bigint,
    redAssist bigint, redChampionDamageDealt bigint,
    redTotalGold bigint, redTotalMinionKills bigint,
    redTotalLevel bigint, redAvgLevel bigint,
    redJungleMinionKills bigint, redKillingSpree bigint,
    redTotalHeal bigint, redObjectDamageDealt bigint)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    lines terminated by '\n'
    tblproperties("skip.header.line.count"="1");

load data inpath 'hdfs:///tmp/yahancheng/final/grandMaster.csv' into table yahancheng_final_grandMaster_csv;

create table yahancheng_final_grandMaster (
    gameId bigint, gameDuraton bigint,
    blueWins bigint, blueFirstBlood bigint,
    blueFirstTower bigint, blueFirstBaron bigint,
    blueFirstDragon bigint, blueFirstInhibitor bigint,
    blueDragonKills bigint, blueBaronKills bigint,
    blueTowerKills bigint, blueInhibitorKills bigint,
    blueWardPlaced bigint, blueWardkills bigint,
    blueKills bigint, blueDeath bigint,
    blueAssist bigint, blueChampionDamageDealt bigint,
    blueTotalGold bigint, blueTotalMinionKills bigint,
    blueTotalLevel bigint,blueAvgLevel bigint,
    blueJungleMinionKills bigint, blueKillingSpree bigint,
    blueTotalHeal bigint, blueObjectDamageDealt bigint,
    redWins bigint, redFirstBlood bigint,
    redFirstTower bigint, redFirstBaron bigint,
    redFirstDragon bigint, redFirstInhibitor bigint,
    redDragonKills bigint, redBaronKills bigint,
    redTowerKills bigint, redInhibitorKills bigint,
    redWardPlaced bigint, redWardkills bigint,
    redKills bigint, redDeath bigint,
    redAssist bigint, redChampionDamageDealt bigint,
    redTotalGold bigint, redTotalMinionKills bigint,
    redTotalLevel bigint, redAvgLevel bigint,
    redJungleMinionKills bigint, redKillingSpree bigint,
    redTotalHeal bigint, redObjectDamageDealt bigint)
    stored as orc;

insert overwrite table yahancheng_final_grandMaster select * from yahancheng_final_grandMaster_csv;


## Challenger dataset
create external table if not exists yahancheng_final_challenger_csv (
    gameId bigint, gameDuraton bigint,
    blueWins bigint, blueFirstBlood bigint,
    blueFirstTower bigint, blueFirstBaron bigint,
    blueFirstDragon bigint, blueFirstInhibitor bigint,
    blueDragonKills bigint, blueBaronKills bigint,
    blueTowerKills bigint, blueInhibitorKills bigint,
    blueWardPlaced bigint, blueWardkills bigint,
    blueKills bigint, blueDeath bigint,
    blueAssist bigint, blueChampionDamageDealt bigint,
    blueTotalGold bigint, blueTotalMinionKills bigint,
    blueTotalLevel bigint,blueAvgLevel bigint,
    blueJungleMinionKills bigint, blueKillingSpree bigint,
    blueTotalHeal bigint, blueObjectDamageDealt bigint,
    redWins bigint, redFirstBlood bigint,
    redFirstTower bigint, redFirstBaron bigint,
    redFirstDragon bigint, redFirstInhibitor bigint,
    redDragonKills bigint, redBaronKills bigint,
    redTowerKills bigint, redInhibitorKills bigint,
    redWardPlaced bigint, redWardkills bigint,
    redKills bigint, redDeath bigint,
    redAssist bigint, redChampionDamageDealt bigint,
    redTotalGold bigint, redTotalMinionKills bigint,
    redTotalLevel bigint, redAvgLevel bigint,
    redJungleMinionKills bigint, redKillingSpree bigint,
    redTotalHeal bigint, redObjectDamageDealt bigint)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    lines terminated by '\n'
    tblproperties("skip.header.line.count"="1");

load data inpath 'hdfs:///tmp/yahancheng/final/challenger.csv' into table yahancheng_final_challenger_csv;

create table yahancheng_final_challenger (
    gameId bigint, gameDuraton bigint,
    blueWins bigint, blueFirstBlood bigint,
    blueFirstTower bigint, blueFirstBaron bigint,
    blueFirstDragon bigint, blueFirstInhibitor bigint,
    blueDragonKills bigint, blueBaronKills bigint,
    blueTowerKills bigint, blueInhibitorKills bigint,
    blueWardPlaced bigint, blueWardkills bigint,
    blueKills bigint, blueDeath bigint,
    blueAssist bigint, blueChampionDamageDealt bigint,
    blueTotalGold bigint, blueTotalMinionKills bigint,
    blueTotalLevel bigint,blueAvgLevel bigint,
    blueJungleMinionKills bigint, blueKillingSpree bigint,
    blueTotalHeal bigint, blueObjectDamageDealt bigint,
    redWins bigint, redFirstBlood bigint,
    redFirstTower bigint, redFirstBaron bigint,
    redFirstDragon bigint, redFirstInhibitor bigint,
    redDragonKills bigint, redBaronKills bigint,
    redTowerKills bigint, redInhibitorKills bigint,
    redWardPlaced bigint, redWardkills bigint,
    redKills bigint, redDeath bigint,
    redAssist bigint, redChampionDamageDealt bigint,
    redTotalGold bigint, redTotalMinionKills bigint,
    redTotalLevel bigint, redAvgLevel bigint,
    redJungleMinionKills bigint, redKillingSpree bigint,
    redTotalHeal bigint, redObjectDamageDealt bigint)
    stored as orc;

insert overwrite table yahancheng_final_challenger select * from yahancheng_final_challenger_csv;


## professional data
### do from 2014 to 2020
create external table yahancheng_final_profession_csv2014 (
    gameid string, datacompleteness string,
    url string, league string,
    year string, split string,
    playoffs bigint, game_date timestamp,
    game bigint, patch string,
    playerid bigint, side string,
    position string, player string,
    team string, champion string,
    ban1 string, ban2 string,
    ban3 string, ban4 string,
    ban5 string, gamelength bigint,
    result bigint, kills bigint,
    deaths bigint, assists bigint,
    teamkills bigint, teamdeaths bigint,
    doublekills bigint, triplekills bigint,
    quadrakills bigint, pentakills bigint,
    firstblood bigint, firstbloodkill bigint,
    firstbloodassist bigint, firstbloodvictim bigint,
    team_kpm float, ckpm float,
    firstdragon bigint,dragons bigint,
    opp_dragons bigint, elementaldrakes bigint,
    opp_elementaldrakes bigint,infernals bigint,
    mountains bigint, clouds bigint, oceans bigint,
    dragons_unknown bigint, elders bigint,
    opp_elders bigint,firstherald bigint,
    heralds bigint, opp_heralds bigint,
    firstbaron bigint, barons bigint,
    opp_barons bigint, firsttower bigint,
    towers bigint, opp_towers bigint,
    firstmidtower bigint,firsttothreetowers bigint,
    inhibitors bigint,opp_inhibitors bigint,
    damagetochampions bigint, dpm float,
    damageshare float, damagetakenperminute float,
    damagemitigatedperminute float, wardsplaced bigint,
    wpm float, wardskilled bigint, wcpm float,
    controlwardsbought bigint,visionscore bigint,
    vspm float, totalgold bigint,
    earnedgold bigint, earned_gpm float,
    earnedgoldshare float, goldspent bigint,
    gspd float, total_cs bigint,
    minionkills bigint, monsterkills bigint,
    monsterkillsownjungle bigint,monsterkillsenemyjungle bigint,
    cspm float, goldat10 bigint,
    xpat10 bigint, csat10 bigint,
    opp_goldat10 bigint, opp_xpat10 bigint,
    opp_csat10 bigint, golddiffat10 bigint,
    xpdiffat10 bigint, csdiffat10 bigint,
    goldat15 bigint, xpat15 bigint,
    csat15 bigint, opp_goldat15 bigint,
    opp_xpat15 bigint, opp_csat15 bigint,
    golddiffat15 bigint, xpdiffat15 bigint,
    csdiffat15 bigint)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    lines terminated by '\n'
    tblproperties("skip.header.line.count"="1");


load data inpath 'hdfs:///tmp/yahancheng/final/profession_2014.csv' into table yahancheng_final_profession_csv2014;

create table yahancheng_final_profession (
    gameid string, datacompleteness string,
    url string, league string,
    year string, split string,
    playoffs bigint, game_date timestamp,
    game bigint, patch string,
    playerid bigint, side string,
    position string, player string,
    team string, champion string,
    ban1 string, ban2 string,
    ban3 string, ban4 string,
    ban5 string, gamelength bigint,
    result bigint, kills bigint,
    deaths bigint, assists bigint,
    teamkills bigint, teamdeaths bigint,
    doublekills bigint, triplekills bigint,
    quadrakills bigint, pentakills bigint,
    firstblood bigint, firstbloodkill bigint,
    firstbloodassist bigint, firstbloodvictim bigint,
    team_kpm float, ckpm float,
    firstdragon bigint,dragons bigint,
    opp_dragons bigint, elementaldrakes bigint,
    opp_elementaldrakes bigint,infernals bigint,
    mountains bigint, clouds bigint, oceans bigint,
    dragons_unknown bigint, elders bigint,
    opp_elders bigint,firstherald bigint,
    heralds bigint, opp_heralds bigint,
    firstbaron bigint, barons bigint,
    opp_barons bigint, firsttower bigint,
    towers bigint, opp_towers bigint,
    firstmidtower bigint,firsttothreetowers bigint,
    inhibitors bigint,opp_inhibitors bigint,
    damagetochampions bigint, dpm float,
    damageshare float, damagetakenperminute float,
    damagemitigatedperminute float, wardsplaced bigint,
    wpm float, wardskilled bigint, wcpm float,
    controlwardsbought bigint,visionscore bigint,
    vspm float, totalgold bigint,
    earnedgold bigint, earned_gpm float,
    earnedgoldshare float, goldspent bigint,
    gspd float, total_cs bigint,
    minionkills bigint, monsterkills bigint,
    monsterkillsownjungle bigint,monsterkillsenemyjungle bigint,
    cspm float, goldat10 bigint,
    xpat10 bigint, csat10 bigint,
    opp_goldat10 bigint, opp_xpat10 bigint,
    opp_csat10 bigint, golddiffat10 bigint,
    xpdiffat10 bigint, csdiffat10 bigint,
    goldat15 bigint, xpat15 bigint,
    csat15 bigint, opp_goldat15 bigint,
    opp_xpat15 bigint, opp_csat15 bigint,
    golddiffat15 bigint, xpdiffat15 bigint,
    csdiffat15 bigint)
    stored as orc;


insert into table yahancheng_final_profession select * from yahancheng_final_profession_csv2014 where position = "team";
### check insertion
select count(gameid) as COUNT from yahancheng_final_profession;