package com.atguigu.fink.Source

import org.apache.flink.api.scala._


object reduce {
  def main(args: Array[String]): Unit = {
    //构造执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //读取文件
    val strem1 = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))

    //    //读取文件
    //    val input: String = "file:///d:/Data/mrinput/combine/a.txt"
    //    val ds: DataSet[String] =env.readTextFile(input)
    //
    //    //经过groupby 进行分组 ，sum进行聚合
    //    val aggDs: AggregateDataSet[(String, Int)] = ds.flatMap(_.split(","))
    //      .map((_, 1))
    //      .groupBy(0)
    //      .sum(1)
    

    strem1.print("aa").setParallelism(1)


    env.execute()
  }

}


case class SensorReading(id: String, time: Long, score: Double)