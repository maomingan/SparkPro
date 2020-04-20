package com.gmm.streaming

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._

/**
  * Receiver方式将日志数据-flume-kafka-spark streaming-hive整合练习
  */
object ReceiverFromKafka {

  //Json格式数据转换 {"order_id":"1","user_id":"2"}
  case class Order(order_id:String,user_id:String)

  def main(args: Array[String]): Unit = {

    /*if(args.length < 4){
      System.err.println("must number of params must be 4!")
      System.exit(1)
    }*/

    //0、对输出日志做控制
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    //1、获取参数/指定参数6个  得到字符串结果为：192.168.204.10,192.168.204.11,192.168.204.12
    //val Array(group_id,topic,exectime,dt) = args
    //如果是线上系统，则可以动态传入dt，这样就可以完成数据分区进入hive的需求
    val Array(group_id,topic,exectime,dt) = Array("group_test","test","30","20190310")
    val zkHostIP = Array("10","11","12").map("192.168.204."+_)
    val ZK_QUORUM = zkHostIP.map(_+":2181").mkString(",")
    val numPartitions = 1
    //2、创建streamContext
    val conf = new SparkConf()//.setAppName("ReceiverFromKafkaTest").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(exectime.toInt))

    //topic对应线程 Map{topic:numPartitions}  这里numPartitions其实就是决定在每个Broker上起几个线程来消费拉到内存里的kafka数据，receiver方式是可以自己设置控制的
    val topicSet = topic.split(",").toSet
    val topicMap = topicSet.map((_,numPartitions.toInt)).toMap

    //3、通过receiver接收kafka数据
    //为什么是map(_._2) 因为return DStream of (Kafka message key, Kafka message value)  而我们现在只需要value值即可
    val dStream = KafkaUtils.createStream(ssc,ZK_QUORUM,group_id,topicMap).map(_._2)
    //dStream.map((_,1L)).reduceByKey(_+_).print()

    //4、生成一个rdd转DF的方法，供后面使用
    def rdd2DF(rdd: RDD[String]): DataFrame = {
      val spark = SparkSession
        .builder()
        .appName("Streaming From Kafka")
        .config("hive.exec.dynamic.partition","true")
        .config("hive.exec.dynamic.partition.mode","nonstrict")
        .enableHiveSupport().getOrCreate()
      // 需要toDF隐式转换的，就需要导入这个
      import spark.implicits._
      //用java的类Orders解析json后，再重新构造一个按需求自己需要的只有两个属性的类Order，因为类Order可能是后端直接提供的
      rdd.map{x=>
        val mess = JSON.parseObject(x, classOf[Orders])
        Order(mess.order_id,mess.user_id)
      }.toDF()

    }

    //5、DStream核心处理逻辑: 1)对DStream中的每一个rdd做“rdd转DF” 2)然后通过DF结构将数据追加到分区表中
    val log = dStream.foreachRDD{rdd=>
      val df = rdd2DF(rdd)
      //把日期追加写到hive分区表badou.order_partition中，lit是指明整列都是这个固定值
      //最终得到三列"order_id","user_id","dt"
      df.withColumn("dt",lit(dt.toString))
        .write.mode(SaveMode.Append)
        .insertInto("badou.order_partition")
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
