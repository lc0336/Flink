package com.atguigu.fink.Window

import com.atguigu.fink.Source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TimeWindow {

  def main(args: Array[String]): Unit = {

    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //读取文件
    //    val dataStream = env.fromCollection(List(
    //      SensorReading("sensor_1", 1547718199, 35.80018327300259),
    //      SensorReading("sensor_6", 1547718201, 15.402984393403084),
    //      SensorReading("sensor_7", 1547718202, 6.720945201171228),
    //      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    //    ))

    val inputStream = env.socketTextStream("hadoop103", 7777)

    val dataStream = inputStream.map(
      data => {
        val dataArray = data.split(",")
        SensorReading( dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble )
      }
    )

    val data = dataStream.map(r => (r.id, r.time))
      .keyBy(_._1)
      //.timeWindow(Time.seconds(15),Time.seconds(5))

      //默认的CountWindow是一个滚动窗口，只需要指定窗口大小即可，当元素数量达到窗口大小时，就会触发窗口的执行。
      //.countWindow(5)

      //每当某一个key的个数达到2的时候,触发计算，计算最近该key最近10个元素的内容
      .countWindow(10,2)
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))


    data.print("timeWindow")
    println("===================")

    env.execute("timeWindow env")


  }

}
