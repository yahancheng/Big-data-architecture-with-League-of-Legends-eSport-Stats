# Big Data Architecture and Application - League of Legends eSport Stats

* Name: Ya-Han Cheng (yahancheng)


## Overview

According to estimates, global eSports market revenue will reach almost 1.6 billion U.S. dollars in 2023. The eSports industry is expected to grow rapidly in the coming years. This project applies big data technique to analyze one of the most popular MOBA game (multiplayer online battle arena) - League of Legends, which has approximately 70 million users worldwide.


In League of Legends, for each game, ten players are split into two teams of five, Blue side and Red side. The ultimate goal of this game is to tear down the other team's towers (turret) and destroy the base (nexus).


<img src="https://github.com/yahancheng/Big-data-architecture-with-League-of-Legends-eSport-Stats/blob/main/screenshot/A-Map-of-the-League-of-Legends-game-play-in-the-classic-mode.png" alt="Figure 1, LOL map" width="500"/>



In this project, 4 early game indicators are used to predict the winning rate and critical game stats across different patches, which can be applied to forming early game strategies and analyzing patch differences. Indicators include:

* **First Blood**: the first kill of a game

* **First Tower**: tear down the first tower of a gmae

*and two epic monsters on the map, which can give potentially game-turning advantages to a team that successfully kills them:*

* **First Dragon**: kill the first dragon of a game

* **First Baron**: kill the first baron of a game


<img src="https://github.com/yahancheng/Big-data-architecture-with-League-of-Legends-eSport-Stats/blob/main/screenshot/Jungle_map.jpg" alt="Figure 2, Dragon and Baron on the map" width="500"/>


This application can fetch historical professional game data with these four indicators. There are 3 main features in this app. Users can

* Find professional game stats over patches

* High-elo rank game stats for the latest patch

* Ingest statistics for new games to update the database


<img src="https://github.com/yahancheng/Big-data-architecture-with-League-of-Legends-eSport-Stats/blob/main/screenshot/startingPage.png" alt="Figure 3, Starting page of the application" width="800"/>


At the starting page, users can select their side and 4 early game indicators.


<img src="https://github.com/yahancheng/Big-data-architecture-with-League-of-Legends-eSport-Stats/blob/main/screenshot/firstSearch.png" alt="Figure 4, Find the winning rate and game statistics under conditions (early game indicators)" width="800"/>


Find the winning rate and game statistics under conditions (early game indicators)


## Lambda Architecture

This project uses lambda architecture to handle game data by taking advantage of both batch and stream-processing methods. Below is a graph of the structure of Lambda architecture. There are 3 layers in this big data architecture - **batch Layer**, **serving layer**, and **speed layer**.

<img src="https://github.com/yahancheng/Big-data-architecture-with-League-of-Legends-eSport-Stats/blob/main/screenshot/lambda-architecture.png" alt="Figure 5, Lambda architecture overview" width="500"/>


### Preparation  and Preprocessing

Data sources of this project are from [Oracle's Elixir](https://oracleselixir.com/tools/downloads) and [Kaggle](https://www.kaggle.com/gyejr95/league-of-legends-challenger-ranked-games2020).

Ingestion to HDFS:

* **Data from Oracle's Elixir**

```
year=2014
while [ $year -le 2020 ]
do  
    cat(year)
    curl https://oracleselixir-downloadable-match-data.s3-us-west-2.amazonaws.com/$year_LoL_esports_match_data_from_OraclesElixir_20201123.csv | hdfs dfs -put - /tmp/yahancheng/final/profession_$year.csv
    (( year++ ))
done
```

* **Data from Kaggle**

Kaggle URL does not support curl command, so I copied cURL instead, which can be found with developer tool.

<img src="https://github.com/yahancheng/Big-data-architecture-with-League-of-Legends-eSport-Stats/blob/main/screenshot/kaggle-data.png" alt="Figure 6, Curl data from Kaggle" width="800"/>


Use cURL to ingest to hdfs directly.

```
curl 'https://storage.googleapis.com/kaggle-data-sets/610763/1096497/compressed/Challenger_Ranked_Games.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20201123%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20201123T171506Z&X-Goog-Expires=259199&X-Goog-SignedHeaders=host&X-Goog-Signature=1d8efb8c5b21af2039a9ef23d4a7055d5dad9d5918c5230f86d23c0fd21ddf8ea6bf0f762c5fcb73f05cfca97e219cef7c165f50e8633c8cb49dcddc109f74d02eb3a02464af8b7bc1df427043ee5fac321c4e8aaaf114b6c0b1bc5b5919bc958ebfdc35624a448ff8846d9bc76823dbb822fca107e99824905b057c9f208c4e54775af5584f9a17877517b67d8fb0dd43c5d82750ac5c3caf339eeaad0752b9fb46991f3e76bd33e2887d26468ca04f4b9cb186f212c8f92ef05ab8203cb46311834efe5b820f9e2fe863e2afaee421c59ddc4f7e382a3a748cc0a1aef9a51118885549e901709962bc4e8f46ec5840af2d4b8779aea678cc3d0634cacdeca2' \
  -H 'authority: storage.googleapis.com' \
  -H 'upgrade-insecure-requests: 1' \
  -H 'dnt: 1' \
  -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36' \
  -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' \
  -H 'sec-fetch-site: cross-site' \
  -H 'sec-fetch-mode: navigate' \
  -H 'sec-fetch-user: ?1' \
  -H 'sec-fetch-dest: document' \
  -H 'referer: https://www.kaggle.com/' \
  -H 'accept-language: zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6' \
  --compressed | hdfs dfs -put - /tmp/yahancheng/final/filename.gz
```


Unzip

```
hdfs dfs -cat /tmp/yahancheng/final/filename.gz | gunzip | hdfs dfs -put - /tmp/yahancheng/final/filename.csv
```


### Batch Layer

After data ingestion process, I used **`data-cleaning-and-ingestion/final-create-hive-table.hql`** to ingest data from HDFS to Hive. In Hive, I cleaned datasets (with **`final-data-cleaning.hql`**) and connected to Hbase (with **final-data-cleaning.hql**) to build the batch layer. The goal of data cleaning and concatenating is to form a batch layer which users can search the row by the side and the 4 early game indicators.


Final schema of batch layer:

```
# create external table in Hive:
create external table yahancheng_final_combine_hbase (
    userInputRow string, patch bigint,
    winningGames bigint, totalGames bigint,
    totalKills bigint, totalDeath bigint, 
    totalAssist bigint, totalAvgKPM bigint, 
    totalAvgDPM bigint, 
    challengerWinningGames bigint, challengerTotalGames bigint,
    grandmasterWinningGames bigint, grandmasterTotalGames bigint,
    masterWinningGames bigint, masterTotalGames bigint
    )
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,game:patch#b,game:winningGames#b,game:totalGames#b,game:totalKills#b,game:totalDeath#b,game:totalAssist#b,game:totalAvgKPM#b,game:totalAvgDPM#b,game:challengerWinningGames#b,game:challengerTotalGames#b,game:grandmasterWinningGames#b,game:grandmasterTotalGames#b,game:masterWinningGames#b,game:masterTotalGames#b')
TBLPROPERTIES ('hbase.table.name' = 'yahancheng_final_combine');
```

Column description:

* **userInputRow**: side_firstBlood(dummy)_firstDragon_firstTower_firstBaron_patch. For example, if, in patch 8, blue side got first blood and first draong, but lost first tower and baron, the input would be `Blue_1_1_0_0_8`.

* **patch**: big patch. For example, 10.23 & 10.1 will both be 10. 9.24 will be 9.

* **winningGames**: number of winning games

* **totalGames**: number of games

* **totalKills**: sum of total kills in every games

* **totalDeath**: sum of total deaths in every games

* **totalAssists**: sum of total assists in every games

* **totalAvgKPM**: sum of average team kills per minute

* **totalAvgDPM**: sum of average team damage per minute

* **challenger-/grandmaster-/master-WinningGames**: number of winning games in challenger/grandmaster/master rank

* **challenger-/grandmaster-/master-TotalGames**: number of games in challenger/grandmaster/master rank

Data stored in `yahancheng_final_combine` table in Hbase.



### Serving Layer

With serving layer, users can query data from Hbase table with inputs. The serving process is managed by the first part of **`profession-lol/src/app.js`**, and the web interface is handled by **profession-lol.html**, which can be found in **`profession-lol/src/public`** folder.


<img src="https://github.com/yahancheng/Big-data-architecture-with-League-of-Legends-eSport-Stats/blob/main/screenshot/startingPage.png" alt="Figure 7, Starting page of the serving layer" width="800"/>


Select side and early game indicators, click submit. 


<img src="https://github.com/yahancheng/Big-data-architecture-with-League-of-Legends-eSport-Stats/blob/main/screenshot/firstSearch.png" alt="Figure 8, Query result" width="800"/>


The result table below shows the game stats by patches. Instead of year and month, patch is a more informative factor in game strategy analysis.



### Speed Layer

Since my dataset does not have real-time ingestion, here I use a web page and Kafka message queue to increment user submitted new game data. The speed layer is managed by the rest part of **app.js** and `finalSpeedLayer` folder, and the web interface of speed layer is handled by **submit-game.html**, which can be found in **`profession-lol/src/public`** folder.


Data streaming process is written in scala. Schema of submitted game data:

```
case class GameReport(
    side: String,
    firstblood: Long,
    firstdragon: Long,
    firsttower: Long,
    firstbaron: Long,
    result: Long,
    dpm: Long,
    team_kpm: Long,
    total_kills: Long,
    total_death: Long,
    total_assist: Long,
    patch: Long)
```

In **`finalSpeedLayer/src/main/scala/StreamGames.scala`**, new data is incremented to Hbase table by:

```
  def incrementGameData(kgr : GameReport) : String = {
    val inc = new Increment(Bytes.toBytes(kgr.side+"_"+kgr.firstblood.toString()+"_"+kgr.firstdragon.toString()+"_"+kgr.firsttower.toString()+"_"+kgr.firstbaron.toString()+"_"+kgr.patch.toString()))
    inc.addColumn(Bytes.toBytes("game"), Bytes.toBytes("totalGames"), 1)
    inc.addColumn(Bytes.toBytes("game"), Bytes.toBytes("winningGames"), kgr.result)
    inc.addColumn(Bytes.toBytes("game"), Bytes.toBytes("totalKills"), kgr.total_kills)
    inc.addColumn(Bytes.toBytes("game"), Bytes.toBytes("totalDeath"), kgr.total_death)
    inc.addColumn(Bytes.toBytes("game"), Bytes.toBytes("totalAssist"), kgr.total_assist)
    inc.addColumn(Bytes.toBytes("game"), Bytes.toBytes("totalAvgKPM"), kgr.team_kpm)
    inc.addColumn(Bytes.toBytes("game"), Bytes.toBytes("totalAvgDPM"), kgr.dpm)
    professionGame.increment(inc)
    return "Updated speed layer for new game, " + kgr.side+"_"+kgr.firstblood.toString()+"_"+kgr.firstdragon.toString()+"_"+kgr.firsttower.toString()+"_"+kgr.firstbaron.toString()+"_"+kgr.patch.toString()
  }
```

Interfaces:

<img src="https://github.com/yahancheng/Big-data-architecture-with-League-of-Legends-eSport-Stats/blob/main/screenshot/origin-stats.png" alt="Figure 9, Stats before increment" width="800"/>


<img src="https://github.com/yahancheng/Big-data-architecture-with-League-of-Legends-eSport-Stats/blob/main/screenshot/submit-game.png" alt="Figure 10, Submit a new game" width="800"/>


<img src="https://github.com/yahancheng/Big-data-architecture-with-League-of-Legends-eSport-Stats/blob/main/screenshot/kafka-message-queue.png" alt="Figure 11, New game is added to Kafka message queue" width="550"/>



<img src="https://github.com/yahancheng/Big-data-architecture-with-League-of-Legends-eSport-Stats/blob/main/screenshot/spark-streaming.png" alt="Figure 12, Spark streaming new record to Hbase table" width="550"/>


<img src="https://github.com/yahancheng/Big-data-architecture-with-League-of-Legends-eSport-Stats/blob/main/screenshot/stats-change.png" alt="Figure 13, Query result update immidiately" width="800"/>



## Source

https://www.researchgate.net/figure/A-Map-of-the-League-of-Legends-game-play-in-the-classic-mode_fig1_319839481

https://leagueoflegends.fandom.com/wiki/Jungling

Original deploy address:

* Serving layer LoadBalancer: http://mpcs53014-loadbalancer-217964685.us-east-2.elb.amazonaws.com:3579/profession-lol.html (stop working)

* Speed layer LoadBalancer: http://mpcs53014-loadbalancer-217964685.us-east-2.elb.amazonaws.com:3579/submit-game.html (stop working)
