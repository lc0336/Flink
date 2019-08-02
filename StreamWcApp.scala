package com.atguigu.fink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamWcApp {
  def main(args: Array[String]): Unit = {
    //从外部命令获取参数
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = tool.get("host")
    val port: Int = tool.get("port").toInt

    //创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //接受socket 文本流
    val dStream = env.socketTextStream(host, port)

    //flatMap 和 map 需要引用得隐式转换
    import org.apache.flink.api.scala._
    //处理 分组并且 sum 聚合
    val DS = dStream.flatMap(_.split(" ")).filter(_.nonEmpty)
      .map((_, 1)).keyBy(0).sum(1)

    //打印
    DS.print()

    env.execute()
  }
}
