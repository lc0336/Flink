package com.atguigu.fink.Sink

import com.atguigu.fink.Source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper._
import org.apache.flink.api.scala._


object redisSinkTest {
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

      var conf=new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()

      strem1.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper))

      env.execute("redis sink test")

      //我们filter的字符串"sensor_1"还可以当作参数传进去。
      //数据类型必须一致
      //strem1.filter(new KeywordFilter("sensor_1"))

  }
}


class MyRedisMapper extends RedisMapper[SensorReading] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temperature")
  }

  override def getKeyFromData(t: SensorReading): String = {
    t.score.toString
  }

  override def getValueFromData(t: SensorReading): String = t.id
}
