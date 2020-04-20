package com.class11.flink

import org.apache.flink.api.scala.ExecutionEnvironment

object Test {
  def main(args: Array[String]): Unit = {
    val benv = ExecutionEnvironment.getExecutionEnvironment
    val path = "C:\\gmm\\大数据\\11期正式课\\00-data"
    val data = benv.readTextFile(s"$path\\orders.csv")
    data.print()

  }
}
