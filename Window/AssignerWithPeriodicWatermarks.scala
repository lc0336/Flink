package com.atguigu.fink.Window

import com.atguigu.fink.Source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark


object AssignerWithPeriodicWatermarks {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 从调用时刻开始给env创建的每一个stream追加时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 每隔5秒产生一个watermark
    env.getConfig.setAutoWatermarkInterval(5000)

    //读取文件
    val dataStream = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))

    val output = dataStream.assignTimestampsAndWatermarks(new MyAssigner)

   // val output = dataStream.assignTimestampsAndWatermarks(new PunctuatedAssigner)


    output.print()

    env.execute()

  }

}

//顺序式生成watermark
class MyAssigner extends AssignerWithPeriodicWatermarks[SensorReading]{
  // 两个参数：最大延时，最大时间戳
  val bound: Long = 60*1000L
  var maxTs = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    println("new watermark")
    new Watermark( maxTs - bound )
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    //检查时间是都大于当前窗口的水位时间
    maxTs = maxTs.max( element.time * 1000 )
    element.time * 1000L
  }
}

//间断式地生成watermark
class PunctuatedAssigner  extends AssignerWithPunctuatedWatermarks[SensorReading]{
  val bound:Long =60 * 1000

  override def extractTimestamp(t: SensorReading, l: Long): Long = t.time

  override def checkAndGetNextWatermark(r: SensorReading, extractedTS: Long): Watermark = {
    if (r.id == "sensor_1") {
      new Watermark(extractedTS - bound)
    } else {
      null
    }
  }

}