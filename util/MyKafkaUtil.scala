package com.atguigu.fink.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object MyKafkaUtil {
  def getConsumer(topic: String) = {
    val prop = new Properties()

    prop.setProperty("bootstrap.servers", "hadoop103:9092")
    prop.setProperty("group.id", "atguigu")

    val myKafkaConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop)
    myKafkaConsumer

  }
}
