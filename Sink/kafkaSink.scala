package com.atguigu.fink.Sink

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.atguigu.fink.bean.StartupLog
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.api.scala._

object kafkaSink {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

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

    val ss = value.map(x=>x._1)
    ss.addSink(new FlinkKafkaProducer011[String]("hadoop103:9092", "test", new SimpleStringSchema()))
    environment.execute("aa")
  }
}
