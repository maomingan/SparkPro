package com.class11.offline

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Spark作业练习
  */
object SimpleFeature {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("test").master("local[2]").enableHiveSupport().getOrCreate()
    val priors = spark.sql("select * from badou11.order_products_prior")
    val orders = spark.sql("select * from badou11.orders")

    /**product feature 商品统计特征
      * 1.销售量 prod_cnt
      * 2.商品被再次购买（reordered）量prod_sum_rod
      * 3.统计reordered比率 prod_rod_rate  但是reordered要么1要么0，所以可以使用avg来直接得到比例
      * */
    // 计算方法1
    // 将reorder转化为int类型
    val priors2 = priors.selectExpr("product_id","cast(reordered as int)")
    val prod_cnt = priors2.groupBy("product_id").count().withColumnRenamed("count","prod_cnt")
    val prod_sum_rod = priors2.groupBy("product_id").sum("reordered").as("prod_sum_rod")
    val prod_rod_rate = priors2.groupBy("product_id").avg("reordered").as("prod_rod_rate")

    //其它指标计算方式2  再groupBy之后可以使用agg来接多个聚合函数(avg,sum,count等)操作，用逗号分隔，如果只有一个则无需加agg
    //这个性能会更好，开发时一定要注意，等于groupBy一次，就计算出所有的指标，等于只做一次shuffle就能得到所有结果，分开的话得进行3次shuffle
    val allProFeature = priors.selectExpr("product_id","cast(reordered as int)")
      .groupBy("product_id")
      .agg(
        count("product_id").as("prod_cnt"),
        sum("reordered").as("prod_sum_rod"),
        avg("reordered").as("prod_rod_rate")
      )

    /**
      * user Features: 用户统计特征
      * 1. 每个用户购买订单的平均间隔
      * 2. 每个用户的总订单数
      * 3. 每个用户购买的product商品去重后的集合数据
      * 4. 每个用户总商品数量以及去重后的商品数量
      * 5. 每个用户购买的平均每个订单商品数量
      * */
      // 异常值处理：将days_since_prior_order中的控制进行处理
    val ordersNew = orders.selectExpr("*","if(days_since_prior_order='',0.0,days_since_prior_order) as dspo")
        .drop("days_since_prior_order")
    // 1. 每个用户购买订单的平均间隔
    val userGap = ordersNew.selectExpr("user_id","cast(dspo as int)").groupBy("user_id").avg("dspo").withColumnRenamed("avg(dspo)","u_avg_day_gap")
    // 2. 每个用户的总订单数
    val userTotalOrder = ordersNew.groupBy("user_id").count()
    // 3. 每个用户购买的product商品去重后的集合数据
    val opDF = orders.join(priors,"order_id")
    val up = opDF.select("user_id","product_id")
    // 方式一：因为spark的DF形式的数据，聚合操作只有sum、avg、count等简单操作，所以需要转换成rdd来进行进一步处理，将数组转化弄成set来进行去重,完成后再转换回DF
    import spark.implicits._
    val userUnionProSet = up.rdd.map(x=>(x(0).toString,x(1).toString))
      .groupByKey()
      .mapValues(_.toSet.mkString(","))
      .toDF("user_id","union_pro_set")
    // 方式二：使用构造类的形式，来进行处理
//    case class upClass(var user_id:String, var product_id:String)
//    val userUnionProSet2 = up.rdd.map{x=>
//      val ups = x.asInstanceOf[upClass];
//      (ups.user_id,ups.product_id)
//    }.groupByKey().mapValues(_.toSet.mkString(",")).toDF("user_id","union_pro_set")
    // 方式三：不转化成rdd处理，直接使用DataFrame的方法回收集合
    val userUnionProSet3 = up.groupBy("user_id").agg(collect_set("product_id"))

    // 4. 每个用户总商品数量以及去重后的商品数量
    val userAllProCnt = up.groupBy("user_id").count()
    val userUnionProCnt = up.rdd.map(x=>(x(0).toString,x(1).toString))
      .groupByKey()
      .mapValues(_.toSet.size)
      .toDF("user_id","union_pro_Cnt")
    // 当有两个共有的groupByKey时，有类似的操作，为了避免多次shuffle过程，进行合并处理进行优化
    // 优化方式一：提取公因子
//    val userRDDGroup = up.rdd.map(x=>(x(0).toString,x(1).toString)).groupByKey().cache()
//    val userUnionProSet2 = userRDDGroup.mapValues(_.toSet.mkString(",")).toDF("user_id","union_pro_set")
//    val userUnionProCnt2 = userRDDGroup.mapValues(_.toSet.size).toDF("user_id","union_pro_Cnt")
    // 优化方式二：同时计算两个   mapValues体中加上分号主要是为了合并成一行可以到shell中执行，因为需要识别那一行是最后一行来return
    val conbineMethod = up.rdd.map(x=>(x(0).toString,x(1).toString))
      .groupByKey()
      .mapValues{records=>
        val recSet = records.toSet;
        (recSet.size,recSet.mkString(","))
      }.toDF("user_id","tuple")
      .selectExpr("user_id","tuple._1 as pro_Cnt","tuple._2 as pro_set")
    // DataFrame的直接方法，避免转成rdd
    val conbineMethod2 = up.groupBy("user_id")
      .agg(
        size(collect_set("product_id")).as("pro_Cnt"),
        collect_set("product_id").as("pro_set")
      )

    // 5. 每个用户购买的平均每个订单商品数量
    //step1：求每个订单有几个商品
    val orderProCnt = priors.groupBy("order_id").count().withColumnRenamed("count","orderProCnt")
    //step2：求每个用户平均每个订单有多少商品
    val userOrderAvgCnt = orders.join(orderProCnt,"order_id")
      .groupBy("user_id")
      .avg("orderProCnt").withColumnRenamed("avg(orderProCnt)","userOrderAvgCnt")


  }

  def feat(priors:DataFrame,orders:DataFrame):(DataFrame,DataFrame)={
    /**product feature 商品统计特征
      * 1.销售量 prod_cnt
      * 2.商品被再次购买（reordered）量prod_sum_rod
      * 3.统计reordered比率 prod_rod_rate  但是reordered要么1要么0，所以可以使用avg来直接得到比例
      * */
    // 计算方法1
    // 将reorder转化为int类型
    val priors2 = priors.selectExpr("product_id","cast(reordered as int)")
    val prod_cnt = priors2.groupBy("product_id").count().withColumnRenamed("count","prod_cnt")
    val prod_sum_rod = priors2.groupBy("product_id").sum("reordered").as("prod_sum_rod")
    val prod_rod_rate = priors2.groupBy("product_id").avg("reordered").as("prod_rod_rate")

    //其它指标计算方式2  再groupBy之后可以使用agg来接多个聚合函数(avg,sum,count等)操作，用逗号分隔，如果只有一个则无需加agg
    //这个性能会更好，开发时一定要注意，等于groupBy一次，就计算出所有的指标，等于只做一次shuffle就能得到所有结果，分开的话得进行3次shuffle
    val allProFeature = priors.selectExpr("product_id","cast(reordered as int)")
      .groupBy("product_id")
      .agg(
        count("product_id").as("prod_cnt"),
        sum("reordered").as("prod_sum_rod"),
        avg("reordered").as("prod_rod_rate")
      )

    /**
      * user Features: 用户统计特征
      * 1. 每个用户购买订单的平均间隔
      * 2. 每个用户的总订单数
      * 3. 每个用户购买的product商品去重后的集合数据
      * 4. 每个用户总商品数量以及去重后的商品数量
      * 5. 每个用户购买的平均每个订单商品数量
      * */
    // 异常值处理：将days_since_prior_order中的控制进行处理
    val ordersNew = orders.selectExpr("*","if(days_since_prior_order='',0.0,days_since_prior_order) as dspo")
      .drop("days_since_prior_order")
    // 1. 每个用户购买订单的平均间隔
    val userGap = ordersNew.selectExpr("user_id","cast(dspo as int)").groupBy("user_id").avg("dspo").withColumnRenamed("avg(dspo)","u_avg_day_gap")
    // 2. 每个用户的总订单数
    val userTotalOrder = ordersNew.groupBy("user_id").count()
    // 3. 每个用户购买的product商品去重后的集合数据
    val opDF = orders.join(priors,"order_id")
    val up = opDF.select("user_id","product_id")
    // 方式一：因为spark的DF形式的数据，聚合操作只有sum、avg、count等简单操作，所以需要转换成rdd来进行进一步处理，将数组转化弄成set来进行去重,完成后再转换回DF
    import orders.sparkSession.implicits._
    val userUnionProSet = up.rdd.map(x=>(x(0).toString,x(1).toString))
      .groupByKey()
      .mapValues(_.toSet.mkString(","))
      .toDF("user_id","union_pro_set")
    // 方式二：使用构造类的形式，来进行处理
    //    case class upClass(var user_id:String, var product_id:String)
    //    val userUnionProSet2 = up.rdd.map{x=>
    //      val ups = x.asInstanceOf[upClass];
    //      (ups.user_id,ups.product_id)
    //    }.groupByKey().mapValues(_.toSet.mkString(",")).toDF("user_id","union_pro_set")
    // 方式三：不转化成rdd处理，直接使用DataFrame的方法回收集合
    val userUnionProSet3 = up.groupBy("user_id").agg(collect_set("product_id"))

    // 4. 每个用户总商品数量以及去重后的商品数量
    val userAllProCnt = up.groupBy("user_id").count()
    val userUnionProCnt = up.rdd.map(x=>(x(0).toString,x(1).toString))
      .groupByKey()
      .mapValues(_.toSet.size)
      .toDF("user_id","union_pro_Cnt")
    // 当有两个共有的groupByKey时，有类似的操作，为了避免多次shuffle过程，进行合并处理进行优化
    // 优化方式一：提取公因子
    //    val userRDDGroup = up.rdd.map(x=>(x(0).toString,x(1).toString)).groupByKey().cache()
    //    val userUnionProSet2 = userRDDGroup.mapValues(_.toSet.mkString(",")).toDF("user_id","union_pro_set")
    //    val userUnionProCnt2 = userRDDGroup.mapValues(_.toSet.size).toDF("user_id","union_pro_Cnt")
    // 优化方式二：同时计算两个   mapValues体中加上分号主要是为了合并成一行可以到shell中执行，因为需要识别那一行是最后一行来return
    val conbineMethod = up.rdd.map(x=>(x(0).toString,x(1).toString))
      .groupByKey()
      .mapValues{records=>
        val recSet = records.toSet;
        (recSet.size,recSet.mkString(","))
      }.toDF("user_id","tuple")
      .selectExpr("user_id","tuple._1 as pro_Cnt","tuple._2 as pro_set")
    // DataFrame的直接方法，避免转成rdd
    val conbineMethod2 = up.groupBy("user_id")
      .agg(
        size(collect_set("product_id")).as("pro_Cnt"),
        collect_set("product_id").as("pro_set")
      )

    // 5. 每个用户购买的平均每个订单商品数量
    //step1：求每个订单有几个商品
    val orderProCnt = priors.groupBy("order_id").count().withColumnRenamed("count","orderProCnt")
    //step2：求每个用户平均每个订单有多少商品
    val userOrderAvgCnt = orders.join(orderProCnt,"order_id")
      .groupBy("user_id")
      .avg("orderProCnt").withColumnRenamed("avg(orderProCnt)","userOrderAvgCnt")

    val userFeat = userGap.join(userTotalOrder,"user_id")
      .join(conbineMethod,"user_id").join(userOrderAvgCnt,"user_id")
      .selectExpr("user_id","u_avg_day_gap",
        "count as user_ord_cnt",
        "pro_Cnt as u_prod_dist_size",
        "pro_set as u_prod_records",
        "userOrderAvgCnt")
    (userFeat, allProFeature)

  }
}
