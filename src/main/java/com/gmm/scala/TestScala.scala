package com.gmm.scala

import scala.io.Source

object TestScala {
  def main(args: Array[String]): Unit = {

    //简单熟悉
    val str = "The figure of Irene, never, as the reader may possibly have observed, present, except through the senses of other characters, is a concretion of disturbing Beauty impinging on a possessive world.";
    val arr = str.split(" ")
    val length = arr.length;
    println("str's length is : "+length)
    println("str's first word is : "+arr(1))

    //读取文件,做wordcount
    val lines = Source.fromFile("C:\\gmm\\大数据\\8期正式课（郑老师）\\【八期-day01】开学典礼+业务系统+MR\\code\\The_Man_of_Property.txt").getLines().toList
    //var wordsList = lines.map(x=>x.split(" ")).flatten;
    val wordsList = lines.flatMap(x=>x.split(" "))
    val wordMap = wordsList.map((_,1))
    //计算方法1：偷懒的wordcount计算方法，直接计算这个词对应的list大小
    val wordCountMap1 = wordMap.groupBy(_._1).map(x=>(x._1,x._2.length))
    //计算方法2：正常逻辑写法
    val wordCountMap2 = wordMap.groupBy(_._1).map(x=>(x._1,x._2.map(_._2).sum))
    //计算方法3：reduce
    val wordCountMap3 = wordMap.groupBy(_._1).map(x=>(x._1,x._2.map(_._2).reduce(_+_)))
    //排序方法1  slice表示取排在前10的数据
    val sortResult1 = wordCountMap1.toList.sortBy(_._2).reverse.slice(0,10)
    //排序方法2
    val sortResult2 = wordCountMap1.toList.sortWith(_._2>_._2).slice(0,10)

    println(sortResult2)

  }
}
