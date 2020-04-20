package com.gmm.streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Direct方式来自己控制消费数据的offset，属于低阶的难度较大，但是更加灵活的一种方式
  */
object DirectOffsetDemo {
  def main(args: Array[String]): Unit = {

    //参数
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val Array(brokers, topics, consumer) = Array("192.168.204.10:9092","test","group_bd")

    //创建context，间隔2秒处理一批数据
    val sparkConf = new SparkConf().setAppName("DirectOffsetDemo")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    //创建Direct Kafka Stream使用上诉信息
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> consumer)

    //核心逻辑，自己控制和维护offset。如果不需要自己控制offset，则使用KafkaUtils.createDirectStream来创建DStream
    val km = new KafkaManager(kafkaParams)
    val dStream = km.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicsSet)
    //增加了一个offset获取，打印展示的过程
    var offsetRanges = Array[OffsetRange]()
    dStream.foreachRDD{rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for(offsize <- offsetRanges){
        km.commitOffsetsToZK(offsetRanges)
        print(s"${offsize.topic} ${offsize.partition} ${offsize.fromOffset} ${offsize.untilOffset}")
      }
    }
    dStream.map(_._2).map((_,1L)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
