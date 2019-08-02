package com.atguigu.fink.Transform

import com.atguigu.fink.Source.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object SplitAndSelect {

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

    //Split 和 select
    val splitStream: SplitStream[SensorReading] = strem1.split(data => {
      if (data.score > 30) Seq("high") else Seq("low")
    })

    val higt: DataStream[SensorReading] = splitStream.select("high")
    val low: DataStream[SensorReading] = splitStream.select("low")
    val all: DataStream[SensorReading] = splitStream.select("all")

    //Connect 和 CoMap   //高分流和低分流   Connect 将两个流进行合并
    val warning = higt.map(data => (data.id, data.score))
    val connected = warning.connect(low)

    val CoMap = connected.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "low")
    )

    CoMap.print("all")

    //合并以后打印  union
    val unionStream = higt.union(low)
    unionStream.print("union:::")

    env.execute()

  }

}
