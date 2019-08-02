package com.atguigu.fink.FunctionClasses

import com.atguigu.fink.Source.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction}
import org.apache.flink.streaming.api.scala._


//Flink暴露了所有udf函数的接口(实现方式为接口或者抽象类)。
// 例如MapFunction, FilterFunction, ProcessFunction等等
class MyFilter  extends  FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
    t.id.contains("sensor_1")
  }
}


class KeywordFilter(keyWord: String) extends FilterFunction[String] {
  override def filter(value: String): Boolean = {
    value.contains(keyWord)
  }
}


object fun {
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

    //自定义funfilter
    strem1.filter(new MyFilter)

    //将函数实现成匿名类
    val value = strem1.filter(
      new RichFilterFunction[SensorReading] {
        override def filter(t: SensorReading): Boolean = {
          t.id.contains("sensor_1")
        }
      }
    )
    value.print()


    //我们filter的字符串"sensor_1"还可以当作参数传进去。
    //数据类型必须一致
    //strem1.filter(new KeywordFilter("sensor_1"))

    env.execute()
  }
}