package com.myProjects

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}



class JsonSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  /**
    * Records assigned to partitions using the configured partitioner.
    *
    * @param topic name of the topic
    * @param key key of choice
    * @param value value to send
    */
  def send(topic: String, key: String, value: String): Unit = {
    producer.send(new ProducerRecord(topic, key, value))
  }

  /**
    * Records assigned to partitions explicitly, ignoring the configured partitioner.
    *
    * @param topic name of the topic
    * @param partition partition for choice
    * @param key key of choice
    * @param value value to send
    */
  def send(topic: String, partition: Int, key: String, value: String): Unit = {
    producer.send(new ProducerRecord(topic, partition, key, value))
  }
}

object JsonSink {
  def apply(config: Properties): JsonSink = {
    val f = () => {
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new JsonSink(f)
  }
}

