package com.atguigu.fink.Window

import com.atguigu.fink.Source.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimeAndWartermark {
  def main(args: Array[String]): Unit = {

    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //EventTime的引入
      // 从调用时刻开始给env创建的每一个stream追加时间特征
     env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //读取文件
    val dataStream = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))


    //引入水位为1秒  ，分配时间戳接口  Flink暴露了TimestampAssigner接口供我们实现
    val dataStreama = dataStream.assignTimestampsAndWatermarks(new
        BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
      override def extractTimestamp(t: SensorReading): Long = {
        t.time * 1000
      }
    })



    dataStreama.print()

    env.execute("da")

  }
}
