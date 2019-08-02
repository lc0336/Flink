package com.atguigu.fink.Sink

import java.util

import com.atguigu.fink.Source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import org.apache.flink.api.scala._

object esSinkTest {
    def main(args: Array[String]): Unit = {

      //创建执行环境
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)

      //读取文件
      val strem1 = env.fromCollection(List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718201, 15.402984393403084),
        SensorReading("sensor_7", 1547718202, 6.720945201171228),
        SensorReading("sensor_10", 1547718205, 38.101067604893444)
      ))

      val httpHosts = new util.ArrayList[HttpHost]()
      httpHosts.add(new HttpHost("hadoop103", 9200))

      val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](httpHosts,
        new ElasticsearchSinkFunction[SensorReading] {
          override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
            println("saving data" + t)
            val json = new util.HashMap[String, String]()
            json.put("data", t.toString)

            val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(json)
            requestIndexer.add(indexRequest)
            println("saved successfully")
          }
        })

      strem1.addSink(esSinkBuilder.build())

      env.execute("es sink test")
    }
}
