package com.atguigu.fink.Transform

import com.atguigu.fink.Source.SensorReading
import org.apache.flink.streaming.api.scala._

object reduce {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //读取文件
    val input: String = "file:///d:/Data/mrinput/combine/a.txt"
    val ds = env.readTextFile(input)

    //读取文件
    //    val strem1 = env.fromCollection(List(
    //      SensorReading("sensor_1", 1547718199, 35.80018327300259),
    //      SensorReading("sensor_6", 1547718201, 15.402984393403084),
    //      SensorReading("sensor_7", 1547718202, 6.720945201171228),
    //      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    //    ))

    val value = ds.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }).keyBy("id")
      .reduce((x, y) => SensorReading(x.id, x.time + 1, y.score))

    value.print("cc")

    env.execute("aa")

  }

}
