package com.atguigu.fink

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.atguigu.fink.bean.StartupLog
import com.atguigu.fink.util.MyKafkaUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


object StartupApp {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //调用方法获取
    //val kafkaConsumer  =MyKafkaUtil.getConsumer("gmall_event")
    //val myKafkaConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String]("gmall_event", new SimpleStringSchema(), prop)

    //直接设置
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "hadoop103:9092")
    prop.setProperty("group.id", "atguigu")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset", "latest")


    val dstream = environment.addSource(new FlinkKafkaConsumer011[String]("gmall_event", new SimpleStringSchema(), prop))
    val value = dstream.map { json =>
      val info = JSON.parseObject(json, classOf[StartupLog])
      (info.area, 1)
    }
    val valueds = value.keyBy(0).sum(1)

    valueds.print()

    environment.execute("aa")
  }
}
