package com.gmm.jieba

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * windows本地测试spark：local模式下的结巴分词统计
  */
object JiebaLocal {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LocalTest").setMaster("local[2]").set("spark.testing.memory","512000000")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //val spark = SparkSession.builder().appName("localTest").master("local[2]").getOrCreate()
    val testRDD = spark.sparkContext.textFile("C:\\gmm\\大数据\\7期正式课（郑老师）\\【八期-day01】\\code\\The_Man_of_Property.txt")
    testRDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,ascending = false).take(20).foreach(println)

  }
}
