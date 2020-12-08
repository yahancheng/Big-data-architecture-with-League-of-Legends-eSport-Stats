# data cleaning
## profession match
create table yahancheng_final_profession_groupby as
select a.userInputRow as userInputRow, a.patch as patch,
a.winningGames as winningGames, a.totalGames as totalGames,
a.totalKills as totalKills, a.totalDeath as totalDeath,
a.totalAssist as totalAssist, a.totalAvgKPM as totalAvgKPM,
a.totalAvgDPM as totalAvgDPM from
(select CONCAT(side, '_', cast(firstblood as string), '_', 
cast(firstdragon as string), '_', cast(firsttower as string), '_', 
cast(firstbaron as string), '_', cast(int(patch) as string)) as userInputRow, 
int(patch) as patch,
sum(result) as winningGames,
count(result) as totalGames,
sum(kills) as totalKills, 
sum(deaths) as totalDeath,
sum(assists) as totalAssist, 
int(sum(team_kpm)) as totalAvgKPM, 
int(sum(dpm)) as totalAvgDPM
from yahancheng_final_profession_clean
group by year, int(patch), side,
firstblood, firstdragon, 
firsttower, firstbaron) a
where a.userInputRow != 'NULL';

## challenger
create table yahancheng_final_challenger_combine as
select CONCAT(side, '_', cast(firstblood as string), '_', 
cast(firstdragon as string), '_', cast(firsttower as string), '_', 
cast(firstbaron as string), '_', cast(int(patch) as string)) as userInputRow,
challengerWinningGames, challengerTotalGames, 'Challenger' as gameType from 
(select blueFirstBlood as firstblood, blueFirstTower as firsttower,
blueFirstBaron as firstbaron, blueFirstDragon as firstdragon,
sum(blueWins) as challengerWinningGames, count(blueWins) as challengerTotalGames,
'Blue' as side, '10' as patch
from yahancheng_final_challenger
group by blueFirstBlood, blueFirstTower,
blueFirstBaron, blueFirstDragon
UNION ALL
select redFirstBlood as firstblood, redFirstTower as firsttower,
redFirstBaron as firstbaron, redFirstDragon as firstdragon,
sum(redWins) as challengerWinningGames, count(redWins) as challengerTotalGames,
'Red' as side, '10' as patch
from yahancheng_final_challenger
group by redFirstBlood, redFirstTower,
redFirstBaron, redFirstDragon) combine;

## grandmaster
create table yahancheng_final_grandmaster_combine as
select CONCAT(side, '_', cast(firstblood as string), '_', 
cast(firstdragon as string), '_', cast(firsttower as string), '_', 
cast(firstbaron as string), '_', cast(int(patch) as string)) as userInputRow,
grandmasterWinningGames, grandmasterTotalGames, 'GrandMaster' as gameType from 
(select blueFirstBlood as firstblood, blueFirstTower as firsttower,
blueFirstBaron as firstbaron, blueFirstDragon as firstdragon,
sum(blueWins) as grandmasterWinningGames, count(blueWins) as grandmasterTotalGames,
'Blue' as side, '10' as patch
from yahancheng_final_grandmaster
group by blueFirstBlood, blueFirstTower,
blueFirstBaron, blueFirstDragon
UNION ALL
select redFirstBlood as firstblood, redFirstTower as firsttower,
redFirstBaron as firstbaron, redFirstDragon as firstdragon,
sum(redWins) as grandmasterWinningGames, count(redWins) as grandmasterTotalGames,
'Red' as side, '10' as patch
from yahancheng_final_grandmaster
group by redFirstBlood, redFirstTower,
redFirstBaron, redFirstDragon) combine;

## master
create table yahancheng_final_master_combine as
select CONCAT(side, '_', cast(firstblood as string), '_', 
cast(firstdragon as string), '_', cast(firsttower as string), '_', 
cast(firstbaron as string), '_', cast(int(patch) as string)) as userInputRow,
masterWinningGames, masterTotalGames, 'Master' as gameType from 
(select blueFirstBlood as firstblood, blueFirstTower as firsttower,
blueFirstBaron as firstbaron, blueFirstDragon as firstdragon,
sum(blueWins) as masterWinningGames, count(blueWins) as masterTotalGames,
'Blue' as side, '10' as patch
from yahancheng_final_master
group by blueFirstBlood, blueFirstTower,
blueFirstBaron, blueFirstDragon
UNION ALL
select redFirstBlood as firstblood, redFirstTower as firsttower,
redFirstBaron as firstbaron, redFirstDragon as firstdragon,
sum(redWins) as masterWinningGames, count(redWins) as masterTotalGames,
'Red' as side, '10' as patch
from yahancheng_final_master
group by redFirstBlood, redFirstTower,
redFirstBaron, redFirstDragon) combine;


## combine all data
create table yahancheng_final_combine as
select 
yahancheng_final_profession_groupby.userInputRow as userInputRow,
yahancheng_final_profession_groupby.patch as patch,
yahancheng_final_profession_groupby.winningGames as winningGames,
yahancheng_final_profession_groupby.totalGames as totalGames,
yahancheng_final_profession_groupby.totalKills as totalKills,
yahancheng_final_profession_groupby.totalAssist as totalDeath,
yahancheng_final_profession_groupby.totalAssist as totalAssist,
yahancheng_final_profession_groupby.totalAvgKPM as totalAvgKPM,
yahancheng_final_profession_groupby.totalAvgDPM as totalAvgDPM,
yahancheng_final_challenger_combine.challengerwinninggames as challengerWinningGames,
yahancheng_final_challenger_combine.challengertotalgames as challengerTotalGames,
yahancheng_final_grandmaster_combine.grandmasterwinninggames as grandmasterWinningGames,
yahancheng_final_grandmaster_combine.grandmastertotalgames as grandmasterTotalGames,
yahancheng_final_master_combine.masterwinninggames as masterWinningGames,
yahancheng_final_master_combine.mastertotalgames as masterTotalGames
from yahancheng_final_profession_groupby
full outer join yahancheng_final_challenger_combine 
on (yahancheng_final_profession_groupby.userInputRow = yahancheng_final_challenger_combine.userInputRow)
full outer join yahancheng_final_grandmaster_combine
on (yahancheng_final_challenger_combine.userInputRow = yahancheng_final_grandmaster_combine.userInputRow)
full outer join yahancheng_final_master_combine
on (yahancheng_final_master_combine.userInputRow = yahancheng_final_grandmaster_combine.userInputRow);

