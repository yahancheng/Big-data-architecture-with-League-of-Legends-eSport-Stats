import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes

object StreamGames {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")
  
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val professionGame = hbaseConnection.getTable(TableName.valueOf("yahancheng_final_combine"))

  
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
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }
    
    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamGames")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("yahancheng_profession-lol")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);

    val kfgr = serializedRecords.map(rec => mapper.readValue(rec, classOf[GameReport]))

    // Update speed table    
    val processedGames = kfgr.map(incrementGameData)
    processedGames.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
