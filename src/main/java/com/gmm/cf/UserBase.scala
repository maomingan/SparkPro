package com.gmm.cf

import breeze.numerics.{pow, sqrt}
import org.apache.spark.sql.SparkSession

object UserBase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("User Base CF").enableHiveSupport().getOrCreate()
    val udata = spark.sql("select user_id,item_id,rating from badou.udata")

    //  一、使用余弦相似度来计算用户相似度  cosine = a点乘b/(|a|*|b|) 其中|a|=sqrt(a1^2+a2^2+....an^2)
    import spark.implicits._

    /**
      * 1.1 计算分母  取user_id和rating两列来计算每个用户的平方和|a|
      * +-------+-------------------+
      * |user_id|rating_pow_sum_sqrt|
      * +-------+-------------------+
      * |    273|  2162.650688391447|
      * |    528|  3410.777184161991|
      * |    584| 1307.5262903666603|
      * |    736| 2927.0010249400325|
      */
    val userScoreSum = udata.rdd.map(x => (x(0).toString,x(1).toString))
      .groupByKey()
      .mapValues(x => sqrt(x.toArray.map(x => pow(x.toDouble,2)).sum))
      .toDF("user_id","rating_pow_sum_sqrt")

    /**
      * 1.2 获取item->user倒排表:复制一份v，来做笛卡尔积之后，得到同一商品两个用户的打分，用于做分子的点乘dot
      * +-------+-------+------+------+--------+
      * |item_id|user_id|rating|user_v|rating_v|
      * +-------+-------+------+------+--------+
      * |    242|    196|     3|   721|       3|
      * |    242|    196|     3|   720|       4|
      * |    242|    196|     3|   500|       3|
      * |    242|    196|     3|   845|       4|
      */
    val udata_v = udata.selectExpr("user_id as user_v","item_id as item_id","rating as rating_v")
    val udata_decare = udata.join(udata_v,"item_id").filter("cast(user_id as long) <> cast(user_v as long)")

    /**
      *   1.3 得到基于同一商品的笛卡尔积之后，便可以在此基础上计算每两个用户在1个商品上的相乘结果
      * +-------+-------+------+------+--------+--------------+
      * |item_id|user_id|rating|user_v|rating_v|rating_product|
      * +-------+-------+------+------+--------+--------------+
      * |    242|    196|     3|   721|       3|           9.0|
      * |    242|    196|     3|   720|       4|          12.0|
      * |    242|    196|     3|   500|       3|           9.0|
      * |    242|    196|     3|   845|       4|          12.0|
      */
    import org.apache.spark.sql.functions._
    val product_udf = udf((rating1:Double,rating2:Double) => rating1*rating2)
    val udata_product = udata_decare.withColumn("rating_product", product_udf(col("rating"),col("rating_v")))

    /**
      *   1.4 求和，计算完整的分子部分结果
      * +-------+------+----------+
      * |user_id|user_v|rating_dot|
      * +-------+------+----------+
      * |    196|   617|      24.0|
      * |    196|   123|      71.0|
      * |    166|   206|     104.0|
      * |    298|   465|     523.0|
      * |    305|   807|     843.0|
      * |     62|   254|     786.0|
      */
    val udata_sim_dot = udata_product.groupBy("user_id","user_v").agg("rating_product"->"sum").withColumnRenamed("sum(rating_product)","rating_dot")

    /**
      *   1.5 计算得到每两个用户之间的余弦相似度
      * +-------+------+--------------------+
      * |user_id|user_v|             simRate|
      * +-------+------+--------------------+
      * |    467|   296| 0.04373558495838492|
      * |    675|   296|0.018053425057155237|
      * |    691|   296| 0.01419985412315454|
      * |    829|   296|0.051625633252967644|
      * |    125|   296|  0.0537664889482949|
      * |    451|   296|0.009103697582627001|
      */
    val userScoreSum_v = userScoreSum.selectExpr("user_id as user_v","rating_pow_sum_sqrt as rating_pow_sum_sqrt_v")
    val udata_sim_rate = udata_sim_dot.join(userScoreSum,"user_id").join(userScoreSum_v,"user_v").selectExpr("user_id","user_v","rating_dot/(rating_pow_sum_sqrt+rating_pow_sum_sqrt_v) as simRate")


    //  二、获取相似用户的物品集合并得到推荐的物品
    /**
      *   2.1 取得前topN个相似用户    udata_sim_rate 通过倒序排序取topN得到-> udata_sim_rate_topN
      */
    val udata_sim_rate_topN = udata_sim_rate.rdd.map(x => (x(0).toString,(x(1).toString,x(2).toString)))
      .groupByKey()
      .mapValues(x => x.toArray.sortWith((x1,x2) => x1._2 > x2._2).slice(0,10))
      .flatMapValues(x=>x)
      .toDF("user_id","user_v_sim")
      .selectExpr("user_id as user_id","user_v_sim._1 as user_v","user_v_sim._2 as simRate")

    /**
      *   2.2 获取用户的物品集合,拼接到大表来
      * +------+-------+--------------------+--------------------+--------------------+
      * |user_v|user_id|             simRate|     item_rating_arr|   item_rating_arr_v|
      * +------+-------+--------------------+--------------------+--------------------+
      * |   296|    829|0.051625633252967644|[278_1, 189_4, 10...|[705_5, 508_5, 20...|
      * |   296|    470| 0.05195344152495993|[475_4, 1097_3, 2...|[705_5, 508_5, 20...|
      * |   296|    323| 0.06876635216435814|[257_2, 223_4, 29...|[705_5, 508_5, 20...|
      * |   296|    424| 0.03249469481518414|[1346_4, 25_4, 26...|[705_5, 508_5, 20...|
      * |   296|    910|0.054984852755147126|[182_4, 1012_4, 1...|[705_5, 508_5, 20...|
      * |   296|    874| 0.04665258465897436|[306_4, 321_3, 34...|[705_5, 508_5, 20...|
      */
    val udata_user_itemAndRating = udata.rdd.map(x => (x(0).toString, x(1).toString+"_"+x(2).toString)).groupByKey().mapValues(x => x.toArray).toDF("user_id","item_rating_arr")
    val udata_user_itemAndRating_v = udata_user_itemAndRating.selectExpr("user_id as user_v","item_rating_arr as item_rating_arr_v")
    val udata_simRate_itemRatingArr = udata_sim_rate_topN.join(udata_user_itemAndRating,"user_id").join(udata_user_itemAndRating_v,"user_v")

    /**
      *   2.3 使用udf来过滤相似用户user_v中存在user_id已经买过的物品
      * +-------+--------------------+--------------------+
      * |user_id|             simRate|       filterItemArr|
      * +-------+--------------------+--------------------+
      * |    829|0.051625633252967644|[508_5, 228_4, 42...|
      * |    470| 0.05195344152495993|[705_5, 20_5, 228...|
      * |    323| 0.06876635216435814|[705_5, 20_5, 228...|
      * |    424| 0.03249469481518414|[705_5, 20_5, 228...|
      * |    910|0.054984852755147126|[705_5, 20_5, 228...|
      */
      val itemFilterUDF = udf{(items:Seq[String], items_v:Seq[String]) =>
        val userIdItemMap = items.map{x =>
          val arr = x.split("_")
          (arr(0),arr(1))
        }.toMap
        items_v.filter{x =>
          val arr = x.split("_")
          //  如果再map中存在的，则得到false，则过滤掉
          userIdItemMap.getOrElse(arr(0), -1) == -1
        }
      }
      val udata_simRate_filterItem = udata_simRate_itemRatingArr
        .withColumn("filterItemArr", itemFilterUDF(col("item_rating_arr"),col("item_rating_arr_v")))
        .select("user_id","simRate","filterItemArr")

    /**
      *   2.4 计算最终推荐分值 = 用户相似度 * rating
      * +-------+--------------------+
      * |user_id|    recommendItemArr|
      * +-------+--------------------+
      * |    829|[508_0.2581281662...|
      * |    470|[705_0.2597672076...|
      * |    323|[705_0.3438317608...|
      * |    424|[705_0.1624734740...|
      */
    val simRatingUDF = udf{(sim:Double, items:Seq[String]) =>
      items.map{x =>
        val arr = x.split("_")
        arr(0)+"_"+(sim*arr(1).toDouble)
      }
    }
    val udata_recommend_itemArr = udata_simRate_filterItem
      .withColumn("recommendItemArr", simRatingUDF(col("simRate"),col("filterItemArr")))
      .select("user_id","recommendItemArr")


    /**
      *   2.5 将数据打平存放，得到最终的针对一个用户的推荐商品列表,最终推荐时只需要按score降序排序后再去重就可以得到需要推荐的商品了
      * +-------+-------+--------------------+
      * |user_id|item_id|       recomendScore|
      * +-------+-------+--------------------+
      * |    829|    508|  0.2581281662648382|
      * |    829|    228| 0.20650253301187058|
      * |    829|    429|  0.2581281662648382|
      * |    829|    248|  0.2581281662648382|
      * |    829|    242| 0.20650253301187058|
      */
    val recommedation = udata_recommend_itemArr
      .select(udata_recommend_itemArr("user_id"), explode(udata_recommend_itemArr("recommendItemArr")))
      .toDF("user_id","item_score")
      .selectExpr("user_id", "split(item_score,'_')[0] as item_id", "cast(split(item_score,'_')[1] as double) as recomendScore")

  }

}
