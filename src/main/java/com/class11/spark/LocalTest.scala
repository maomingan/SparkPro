package com.class11.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LocalTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    //读取hdfs文件数据
    val file = sc.textFile("/data/ext/The_Man_of_Property.txt")
    //取10个分隔后的单词，然后循环打印出来
    file.flatMap(line => line.split(" ")).take(10).foreach(println)
    //spark 做 word count
    val counts = file.flatMap(line=>line.split(" ")).map((_,1)).reduceByKey(_+_)
    //将结果存储到hdfs目录上
    counts.saveAsTextFile("/output/wc2")

  }
}
