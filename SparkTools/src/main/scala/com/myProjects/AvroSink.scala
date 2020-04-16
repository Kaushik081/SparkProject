package com.myProjects

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class AvroSink(createProducer: () => KafkaProducer[String, Array[Byte]]) extends Serializable {

  lazy val producer = createProducer()

  /**
    * Records assigned to partitions using the configured partitioner.
    *
    * @param topic name of the topic
    * @param key key of choice
    * @param value  value to send
    */
  def send(topic: String, key: String, value: Array[Byte]): Unit = {
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
  def send(topic: String, partition: Int, key: String, value: Array[Byte]): Unit = {
    producer.send(new ProducerRecord(topic, partition, key, value))
  }
}

object AvroSink {

  def apply (config: Properties) : AvroSink = {

    val f = () => {
      new KafkaProducer[String,Array[Byte]](config)
    }
    new AvroSink(f)
  }


}

