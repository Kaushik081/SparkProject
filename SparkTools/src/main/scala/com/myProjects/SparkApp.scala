package com.myProjects

import java.nio.ByteBuffer
import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object SparkApp {

  lazy private val LOGGER = Logger.getRootLogger

  def main(args: Array[String]): Unit = {




    val conf = new SparkConf()

    if (System.getProperty("os.name").toLowerCase() != "linux") {
      conf.setAppName("App")
      conf.setMaster("local[*]")
    }


    val SUBSCRIBED_TOPICS = conf.get("spark.subscribe_topics","topic1")
    val KAFKA_BROKERS = conf.get("spark.kafka.brokers","kytcksnp001.state.ky.us:9092,kytcksnp002.state.ky.us:9092,kytcksnp003.state.ky.us:9092")
    val ZOOKEEPER_QUORUM = conf.get("spark.zookeeper.quorum","easnn01.state.ky.us:2181,easnn02.state.ky.us:2181,easkb.state.ky.us:2181")
    val SPARK_JOB_BATCH_INTERVAL = conf.getInt("spark.batch_interval",10)
    val GROUP_ID = conf.get("spark.group_id","group000")
    val HBASE_OFFSET_MANAGER = conf.get("spark.offsetManager.hbaseTable","stream_kafka_offsets")
    val SCHEMA_REGISTRY = conf.get("spark.schema_registry","C:/Users/venkata.chaganti/Documents/new_avro_schemas/Register.txt")



    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> KAFKA_BROKERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> GROUP_ID,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val offsets : Map[TopicPartition,Long] = OffsetManager
      .getLastCommittedOffsets(SUBSCRIBED_TOPICS,
        GROUP_ID,
        HBASE_OFFSET_MANAGER,
        ZOOKEEPER_QUORUM,
        KAFKA_BROKERS,
        Int.MaxValue,
        10000)

    lazy val props = new Properties()
    props.setProperty("bootstrap.servers", KAFKA_BROKERS)
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")


    val sc = new SparkContext(conf)

    val registory = sc.broadcast(AvroSchemaRegistor.loadSchema(SCHEMA_REGISTRY))
    val sink = sc.broadcast(JsonSink.apply(props))

    val ssc = new StreamingContext(sc, Seconds(SPARK_JOB_BATCH_INTERVAL))
    val stream = KafkaUtils.createDirectStream[String, Array[Byte]](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String, Array[Byte]](offsets.keys,kafkaParams,offsets))

    stream.foreachRDD((rdd,batchTime) => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offset => LOGGER.info(s"Topic:${offset.topic} ,partition:${offset.partition}, offset range :[${offset.fromOffset} - ${offset.untilOffset}"))

     rdd.foreachPartition(partition =>{

       partition
         .map(_.value())
         .flatMap(buffer => {
         try {
           val bb = ByteBuffer.wrap(buffer)
           val schemaId = bb.getInt
           val sourceAvroSchema = new Schema.Parser() parse registory.value(schemaId).fromSchema
           val reader = new GenericDatumReader[GenericRecord](sourceAvroSchema)
           LOGGER.debug(s"Deserializing data ;schemaId : $schemaId" )
           val decoder = DecoderFactory.get().binaryDecoder(buffer, bb.position(), bb.remaining(), null)
           val record  = reader.read(null, decoder)
           val packet :Tuple2[Int,org.apache.avro.generic.GenericRecord] = Tuple2(schemaId, record)
           Some(packet)
         }catch {
           case t: Throwable => LOGGER.error(s"Failed to Deserialize ; error : ${t.getMessage}")
             None
         }
       })
         .map{case(k,v) =>

           /*

           you can apply all you business logic to different schemas here send events to a different topic

            */

           val enrichedStream = v
           //converting avro record to json
           (k,enrichedStream.toString)
         }
         .foreach{case(shemaId,jsonEvent) =>
           sink.value.send(registory.value(shemaId).destinationTopic,null,jsonEvent)
         }


     })

    })

    ssc.start()
    ssc.awaitTermination()



  }

}
