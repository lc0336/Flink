package com.atguigu.fink.Source

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

object FlinkApp {
  def main(args: Array[String]): Unit = {
    //构造执行环境
    // val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val stream4 = env.addSource(new MySensorSource)

    stream4.print("stream4").setParallelism(1)

    env.execute("aa")
  }

}

class MySensorSource extends SourceFunction[SensorReading]{
  //设置标志flag：表示数据源是否正常运行
  var running:Boolean=true

  override def cancel(): Unit = {
    running=false
  }

  //随机生成传感器温度的基准数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化一个随机数
    val rand = new Random()

    //随机生成初始化10个传感器温度的基准数据
    var curTemp = 1.to (10).map(
      i =>("sensor_"+i,60+rand.nextGaussian()*20)
    )
    // 用一个循环来不断地产生数据流，当cancel时停止
    while (running){
      // 更新温度值
      curTemp=curTemp.map(
        t=>(t._1,t._2+rand.nextGaussian())
      )
      // 获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        t => sourceContext.collect( SensorReading( t._1, curTime, t._2 ) )
      )
      // 间隔100ms
      Thread.sleep(100)

    }
  }


}