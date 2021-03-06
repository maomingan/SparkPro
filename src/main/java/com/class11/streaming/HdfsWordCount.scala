package com.class11.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 拷贝自 /usr/gmm/spark-2.0.2-bin-hadoop2.6/examples/src/main/scala/org/apache/spark/examples/streaming/HdfsWordCount.scala
  */
object HdfsWordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("HdfsWordCount")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    //val lines = ssc.textFileStream(args(0))
    //我们改成直接通过socket输入
    val lines = ssc.socketTextStream("192.168.204.10",9999)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    //写到HDFS  路径到提交命令处指定
    wordCounts.saveAsTextFiles(args(0))

    ssc.start()
    ssc.awaitTermination()

  }
}
