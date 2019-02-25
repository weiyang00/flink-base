package com.example

import org.apache.flink.api.scala._

/**
  * Created by WeiYang on 2/25/2019.
  *
  * @Author: WeiYang
  * @Package com.example
  * @Project: flink_test
  * @Title:
  * @Description: Please fill description of the file here
  * @Date: 2/25/2019 6:37 PM
  */
object DataSetWordCount extends App {

  val env = ExecutionEnvironment.getExecutionEnvironment
  val text = env.fromElements("Who's there?" +
    "I think I hear them. Standm ho! Who's there?")

  val counts = text.flatMap {
    _.toLowerCase.split("\\W+").filter {
      _.nonEmpty
    }
  }
    .map {
      (_, 1)
    }
    .groupBy(0)
    .sum(1)
  counts.print()

}