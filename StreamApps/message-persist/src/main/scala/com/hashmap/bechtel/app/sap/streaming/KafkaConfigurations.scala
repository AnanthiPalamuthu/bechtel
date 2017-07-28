package com.hashmap.bechtel.app.sap.streaming

import java.lang
import java.util.Properties
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}


trait KafkaConfigurations {


  def kafkaProducerProperties(kafkaBroker: String,zookeeperUrl: String,id: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaBroker)
    props.put("zk.connect", zookeeperUrl)
    props.put("client.id", id)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  def kafkaConsumerProperties(kafkaBroker: String,zookeeperUrl: String) = {
    Map("bootstrap.servers" -> kafkaBroker,
      "zookeeper.connect" -> zookeeperUrl,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "rolling_average"
      //"auto.offset.reset" -> "earliest",
      //"enable.auto.commit" -> (false: lang.Boolean)
    )
  }
}
