package com.atguigu.fink.Sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.fink.Source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}


object MYJdbcSinkTest {
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

      strem1.addSink(new MyJbdcSink)

      env.execute("jdbc sink test")
  }
}


//JDBC 自定义sink
class MyJbdcSink extends RichSinkFunction[SensorReading]{

  var conn:Connection=_
  var insertStmt:PreparedStatement=_
  var updateStmt:PreparedStatement=_

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    conn=DriverManager.getConnection("jdbc:mysql://hadoop104:3306/test","root","123456")

    insertStmt=conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?, ?)")

    updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")

  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(2,value.score)
    updateStmt.setString(1,value.id)
    updateStmt.execute()


    if (updateStmt.getUpdateCount==0){
      insertStmt.setDouble(1,value.score)
      insertStmt.setString(2,value.id)
      insertStmt.execute()
    }
  }
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
