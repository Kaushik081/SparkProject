package com.myProjects

import java.io.{PrintWriter, StringWriter}
import java.util.{Collections, Properties}

import kafka.utils.ZkUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka010.OffsetRange

object OffsetManager {

  private val logger = Logger.getRootLogger


  def saveOffsets(TOPIC:String,GROUP_ID:String,offsetRanges:Array[OffsetRange],
                  hbaseTableName:String,
                  batchTime: org.apache.spark.streaming.Time,zkQuorum:String): Unit = {

    try {

      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.setInt("timeout", 120000000)
      hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
      val conn = ConnectionFactory.createConnection(hbaseConf)
      val table = conn.getTable(TableName.valueOf(hbaseTableName))

      val rowKey = TOPIC + ":" + GROUP_ID + ":" + String.valueOf(batchTime.milliseconds)
      val put = new Put(rowKey.getBytes)

      for (offset <- offsetRanges) {
        put.addColumn(Bytes.toBytes("offsets"), Bytes.toBytes(offset.partition.toString), Bytes.toBytes(offset.untilOffset.toString))
      }
      table.put(put)
      conn.close()
    }catch {
      case t:Throwable => val sw = new StringWriter() ; t.printStackTrace(new PrintWriter(sw))
        logger.error(sw.toString)
    }

  }

  /*
    Returns last committed offsets for all the partitions of a given topic from HBase in following cases.
      - CASE 1: SparkStreaming job is started for the first time. This function gets the number of topic partitions from
        Zookeeper and for each partition returns the last committed offset as 0
      - CASE 2: SparkStreaming is restarted and there are no changes to the number of partitions in a topic. Last
        committed offsets for each topic-partition is returned as is from HBase.
      - CASE 3: SparkStreaming is restarted and the number of partitions in a topic increased. For old partitions, last
        committed offsets for each topic-partition is returned as is from HBase as is. For newly added partitions,
        function returns last committed offsets as 0
     */

  def getLastCommittedOffsets(TOPIC:String,GROUP_ID :String,hbaseTableName:String,zkQuorum:String,brokers:String,sessionTimeout:Int,connectionTimeOut:Int): Map[TopicPartition,Long] = {

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.setInt("timeout", 1200000)
    hbaseConf.set("hbase.zookeeper.quorum",zkQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    val fromOffsets = collection.mutable.Map[TopicPartition,Long]()
    val zkUrl = zkQuorum
    val zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl,sessionTimeout,connectionTimeOut)
    val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2,false)
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf(hbaseTableName))
    val scan = new Scan()

    val zKNumberOfPartitionsForTopic = zkUtils.getPartitionsForTopics(Seq(TOPIC)).get(TOPIC).toList.head.size
    //Connect to HBase to retrieve last committed offsets
    val startRow = TOPIC + ":" + GROUP_ID + ":" + String.valueOf(System.currentTimeMillis())
    val stopRow = TOPIC + ":" + GROUP_ID + ":" + 0
    val scanner = table.getScanner(scan.withStartRow(startRow.getBytes).withStopRow(stopRow.getBytes).setReversed(true))
    val result = scanner.next()
    var hbaseNumberOfPartitionsForTopic = 0

    if(result != null ){
      //If the result from hbase scanner is not null, set number of partitions from hbase to the number of cells
      hbaseNumberOfPartitionsForTopic = result.listCells().size()
    }
    if(hbaseNumberOfPartitionsForTopic == 0){
      // initialize fromOffsets to beginning
      //change this logic to get the first available offset in kafka
      val prop =  new Properties()
      prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers)
      prop.put(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID)
      prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
      prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
      prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")


      val consumer = new KafkaConsumer[String, String](prop)
      for (partition <- 0 until zKNumberOfPartitionsForTopic){
        val topicPartition = new TopicPartition(TOPIC, partition)
        consumer.assign(Collections.singletonList(topicPartition))
        consumer.seekToBeginning(Collections.singletonList(topicPartition))

        fromOffsets += (new TopicPartition(TOPIC,partition) -> consumer.position(topicPartition))
      }
    } else if(zKNumberOfPartitionsForTopic > hbaseNumberOfPartitionsForTopic){
      // handle scenario where new partitions have been added to existing kafka topic
      for (partition <- 0 until hbaseNumberOfPartitionsForTopic){
        val fromOffset = Bytes.toString(result.getValue(Bytes.toBytes("offsets"),Bytes.toBytes(partition.toString)))
        fromOffsets += (new TopicPartition(TOPIC,partition) -> fromOffset.toLong)}
      for (partition <- hbaseNumberOfPartitionsForTopic until zKNumberOfPartitionsForTopic){
        fromOffsets += (new TopicPartition(TOPIC,partition) -> 0)}
    } else {
      //initialize fromOffsets from last run
      for (partition <- 0 until hbaseNumberOfPartitionsForTopic){
        val fromOffset = Bytes.toString(result.getValue(Bytes.toBytes("offsets"),Bytes.toBytes(partition.toString)))
        fromOffsets += (new TopicPartition(TOPIC,partition) -> fromOffset.toLong)}
    }
    scanner.close()

    conn.close()
    fromOffsets.toMap
  }
}

