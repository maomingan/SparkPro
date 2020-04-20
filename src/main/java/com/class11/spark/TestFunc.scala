package com.class11.spark

import org.apache.spark.sql.SparkSession

/**
  * 下面的代码可以在spark-shell客户端里直接执行验证
  */
object TestFunc {
  def main(args: Array[String]): Unit = {
    //实例化sparksession 在client端自动实例化sparksession
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql("select * from badou11.orders")
    val priors = spark.sql("select * from badou11.order_products_prior")
    df.join(priors,"user_id")

    // 每个用户，根据order_hour_of_day排序，后期可用于解决：每个客户最喜欢的top3商品，先排序，再取前3
    // 先转化为RDD进行排序处理，再转回成DataFrame
    import spark.implicits._
    val orderNumerSort = df.select("user_id","order_number","order_hour_of_day")
      .rdd.map(x => (x(0).toString,(x(1).toString,x(2).toString)))
      .groupByKey()
      .mapValues(_.toArray.sortWith(_._2<_._2).slice(0,3))
      .toDF("user_id","order_sort_by_day")

    // udf  如果需要处理复杂的逻辑，则替换=>后面的逻辑即可
    import org.apache.spark.sql.functions._
    val plusUDF = udf((col1:String,col2:String)=>col1.toInt+col2.toInt)
    df.withColumn("plus",plusUDF(col("order_number"),col("order_dow"))).show()


  }
}
