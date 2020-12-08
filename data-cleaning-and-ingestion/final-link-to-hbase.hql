# create table in HBase
create 'yahancheng_final_combine', 'game'

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

INSERT overwrite table yahancheng_final_combine_hbase
select * from yahancheng_final_combine
order by userInputRow;

