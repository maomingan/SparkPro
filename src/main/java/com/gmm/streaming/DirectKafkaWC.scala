package com.gmm.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 代码拷贝自 /usr/gmm/spark-2.0.2-bin-hadoop2.6/examples/src/main/scala/org/apache/spark/examples/streaming/DirectKafkaWordCount.scala
  */
object DirectKafkaWC {

  def main(args: Array[String]) {
    //需要两个参数，如果传的不是两个参数，那么久直接退出程序
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics) = args
    //val Array(brokers, topics) = Array("192.168.204.10:9092","test")

    // Create context with 2 second batch interval
    //设置进行自动反压，即spark streaming处理不过来的时候，可以反压在kafka，按照自己节奏来进行消费
    val sparkConf = new SparkConf()
      .setAppName("DirectKafkaWordCount")
      .set("spark.streaming.backpressure.enabled","true")
    //创建2秒一批数据的定义
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    // 因为是Direct方式，所以这里的topics不需要再自己指定对应的线程数了，因为线程数不受自己控制，不像Receiver方式
    // 可以看一下两种方式消费kafka数据的区别图，就明白参数为什么不一样了
    val topicsSet = topics.split(",").toSet
    //不指定groupID
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    //如果需要指定groupID,需要增加参数:group.id
    //val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> "badou_test")
    val dStream = KafkaUtils.createDirectStream[
      String,
      String,
      StringDecoder,
      StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print

    val lines = dStream.map(_._2)
    // 如果需要在作业里重分区（即重新分配处理作业的task线程数量）的话，可以这样设置
    // 这样重分区的使用场景：可以直接在spark streaming程序中，将多份数据使用一个重分区来达到合并小文件输出的作用，但是作业时间必然增加
    // spark streaming 写入hdfs中可能会出现小文件比较多情况，对于中小型数据量，用repartion（1），如果有10个分区repartition到一个分区上，相当于就是合并10个文件数据到一个文件中。
    // val lines = dStream.map(_._2).repartition(1)


    //如果message传过来是一句话
    /*val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)*/

    //如果message传过来就是简单的词，即一句话就是一个词
    val wordCounts = lines.map(x => (x,1L)).reduceByKey(_+_)

    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}
