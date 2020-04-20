package com.class11.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object LocalStreamTest {
  def main(args: Array[String]): Unit = {
    // 如果是普通的spark，则是使用SparkSession来得到=》sparkconf+sparkcontext,但是spark streaming设置如下
    val conf = new SparkConf().setAppName("WC").setMaster("local[2]")
    // 出现异常
    // Exception in thread "main" java.lang.IllegalArgumentException:
    // System memory 259522560 must be at least 471859200. Please increase heap size using the
    // --driver-memory option or spark.driver.memory in Spark configuration.
    // 需要加上此配置才会运行不报错
    conf.set("spark.testing.memory", "571859200")
    // 按照2s切DStream
    val ssc = new StreamingContext(conf,Seconds(2))
    // 出现异常
    // Exception in thread "main" java.lang.IllegalArgumentException: requirement failed:
    // The checkpoint directory has not been set. Please set it by StreamingContext.checkpoint()
    // 在进行全局变量统计的时候，必须加上checkpoint，否则启动会报错
    ssc.checkpoint("C:\\gmm\\d\\checkpoint")

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    //执行逻辑 连接master节点的ip:port  在master节点输入nc -lp 9999即可打开输入窗口给这边连接
    val lines = ssc.socketTextStream("192.168.204.10",9999)
    val words = lines.flatMap(_.split(" "))
    //val wordCounts = words.map((_,1)).reduceByKey(_+_)
    //wordCounts.print()

    //窗口操作 10s的窗口总长度，2秒的滑动时间间隔
    //因为前面是按照2s来切分的DStream，所以这两个参数都必须是2s的倍数
    //val wordCounts2 = words.map((_,1)).reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(10),Seconds(2))
    //wordCounts2.print()

    // 全局统计
    val addFunc = (curValues:Seq[Long], preValueState:Option[Long])=>{
      val curCount = curValues.sum
      val preCount = preValueState.getOrElse(0L)
      Some(curCount+preCount)
    }
    val wordCounts3 = words.map((_,1L)).updateStateByKey[Long](addFunc)
    wordCounts3.print()

    // Spark Streaming需要加上
    ssc.start()
    // 除非出错或者主动停掉，否则会一直在跑，Streaming就是这样的
    ssc.awaitTermination()

  }
}
