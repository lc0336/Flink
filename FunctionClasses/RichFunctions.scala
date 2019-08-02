package com.atguigu.fink.FunctionClasses

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
  **
“富函数”是DataStream API提供的一个函数类的接口，所有Flink函数类都有其Rich版本。它与常规函数的不同在于，可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更复杂的功能。
*	RichMapFunction
*	RichFlatMapFunction
*	RichFilterFunction
*	…
*Rich Function有一个生命周期的概念。典型的生命周期方法有：
*	open()方法是rich function的初始化方法，当一个算子例如map或者filter被调用之前open()会被调用。
*	close()方法是生命周期中的最后一个调用的方法，做一些清理工作。
*	getRuntimeContext()方法提供了函数的RuntimeContext的一些信息，例如函数执行的并行度，任务的名字，以及state状态
 *
 */
class MyFlatMap extends RichFlatMapFunction[Int, (Int, Int)] {
  var subTaskIndex = 0

  override def open(configuration: Configuration): Unit = {
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
    // 以下可以做一些初始化工作，例如建立一个和HDFS的连接
  }

  override def flatMap(in: Int, out: Collector[(Int, Int)]): Unit = {
    if (in % 2 == subTaskIndex) {
      out.collect((subTaskIndex, in))
    }
  }

  override def close(): Unit = {
    // 以下做一些清理工作，例如断开和HDFS的连接。
  }
}

