package com.class11.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object ReceiverTest {
  def main(args: Array[String]): Unit = {

    // 以后可以作为参数传入
    val Array(group_id,topic,exectime) = Array("group_badou20200213","badou20200213","2")

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    // 如果提交到集群上去跑，则不能加上setMaster这段
    val conf = new SparkConf().setMaster("local[2]").setAppName("ReceiverTest")
    conf.set("spark.testing.memory", "571859200")
    val ssc = new StreamingContext(conf, Seconds(exectime.toInt))

    // 设置每个broker去消费topic时，各起几个线程
    val topicSet = topic.split(",").toSet
    val numPartitions = 1
    val topicMap = topicSet.map((_,numPartitions)).toMap

    val zkQuorum = "192.168.204.10:2181"

    // 需要取kafka的message的第二个值，才是真正在传输的值。否则得到的是(null,aaa)等数据，key相当于header信息
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group_id, topicMap).map(_._2)

    // 本地跑或者打包到集群上去跑
    lines.map((_,1L)).reduceByKey(_+_).print()



    ssc.start()
    ssc.awaitTermination()

  }
}
