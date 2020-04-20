package com.class11.cf

import breeze.numerics.{pow, sqrt}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UserCF {
  def main(args: Array[String]): Unit = {

    // spark shell执行
//    val spark = SparkSession.builder().appName("User CF").master("local[2]").enableHiveSupport().getOrCreate()
    // spark on yarn 执行，需要把master去掉
    val spark = SparkSession.builder().appName("User CF").enableHiveSupport().getOrCreate()
    val df = spark.sql("select user_id,item_id,rating from badou11.udata")

    // 1.计算用户相似度 余弦cosine = a*b/(|a|*|b|)

    // 1.1 得到分母的集合(|a|*|b|)
    import spark.implicits._
    val userScoreSum = df.rdd
      .map(x=>(x(0).toString,x(2).toString))
      .groupByKey()
      .mapValues(x=>sqrt(x.toArray.map(rating=>pow(rating.toDouble,2)).sum))
      .toDF("user_id","rating_pow_sum_sqrt")
    println("分母部分：")
    userScoreSum.show

    // 理解笛卡尔积的测试
//    val df3 = df.selectExpr("user_id").distinct().filter("cast(user_id as bigint)<=3")
//    val df33 = df3.selectExpr("user_id as user_v")
//    df3.join(df33).filter("cast(user_id as int) < cast(user_v as int)").show
    // 1.2 得到item->user倒排表, 使用join进行笛卡尔积之后再加上条件排除掉相同用户之间的相似
    val df_v = df.selectExpr("user_id as user_v","item_id","rating as rating_v")
    val df_decare = df.join(df_v,"item_id").filter("cast(user_id as long) <> cast(user_v as long)")
    // 1.3 计算两个用户在一个item下的评分的乘积，是cosine公式的分子中的一部分
    val df_product = df_decare.selectExpr("item_id","user_id","user_v","cast(rating as double)*cast(rating_v as double) as prod")
    // 求和，计算完整的分子点乘部分
    val df_sim_group = df_product.groupBy("user_id","user_v").agg(sum("prod").as("rating_dot"))
    println("分子部分：")
    df_sim_group.show()
    // 1.5计算余弦相似度（用户相似度）
    val uerScoreSum_v = userScoreSum.selectExpr("user_id as user_v","rating_pow_sum_sqrt as rating_pow_sum_sqrt_v")
    val df_sim = df_sim_group.join(userScoreSum,"user_id").join(uerScoreSum_v,"user_v")
      .selectExpr("user_id","user_v","rating_dot/(rating_pow_sum_sqrt*rating_pow_sum_sqrt_v) as cosine_sim")


    // 2.获取相似用户的物品集合及打分
    // 2.1 获取前n个相似用户
    // 代码说明：toArray是为了进行排序取top，然后再flatmap把array排序后的数据打平，才能转为DF
    val df_nsim = df_sim.rdd
      .map(x=>(x(0).toString,(x(1).toString,x(2).toString)))
      .groupByKey()
      .mapValues(x=>x.toArray.sortWith(_._2>_._2).slice(0,10))
      .flatMapValues(x=>x).toDF("user_id","user_v_sim")
      .selectExpr("user_id","user_v_sim._1 as user_v","user_v_sim._2 as sim")
    println("取相似的前10个用户：")
    df_nsim.show()
    df_nsim.show(1,false)

    // 2.2 获取用户的物品集合进行过滤
    val df_user_item = df.rdd.map(x=>(x(0).toString,x(1).toString+"_"+x(2).toString))
      .groupByKey().mapValues(_.toArray).toDF("user_id","item_rating_arr")
    val df_user_item_v = df_user_item.selectExpr("user_id as user_v", "item_rating_arr as item_rating_arr_v")
    val df_gen_item = df_nsim.join(df_user_item,"user_id").join(df_user_item_v,"user_v")

    // 2.3 实现UDF：处理和过滤掉user_v下的商品中user_id已经购买过了的
    import org.apache.spark.sql.functions.udf
    val filter_udf = udf{(items:Seq[String],items_v:Seq[String])=>
      val fMap = items.map{x=>
        val arr = x.split("_")
        (arr(0), arr(1))
      }.toMap
      items_v.filter{x=>
        val arrV = x.split("_")
        // 在fMap里没有的，都保留下来
        fMap.getOrElse(arrV(0), -1) == -1
      }
    }
    val df_filter_item = df_gen_item
      .withColumn("filtered_item",filter_udf(col("item_rating_arr"),col("item_rating_arr_v")))
      .select("user_id","user_v","sim","filtered_item")


    // 2.4 公式计算  物品推荐的最终得分 = 用户相似度 * 相似用户的物品打分
    val simRating_udf = udf{(sim:Double,items:Seq[String])=>
      items.map{x=>
        val arr = x.split("_")
        arr(0)+"_"+arr(1).toDouble*sim
      }
    }
    val itemSimRating = df_filter_item
      .withColumn("item_prod", simRating_udf(col("sim"),col("filtered_item")))
      .select("user_id","item_prod")
    // 将item_score的数组拆解出来explode  行转列（一行数组转成多行字符串）
    val userItemScore = itemSimRating.select(itemSimRating("user_id"),explode(itemSimRating("item_prod")))
      .toDF("user_id","item_prod")
      .selectExpr("user_id","split(item_prod,'_')[0] as item_id","cast(split(item_prod,'_')[1] as double) as score")
    // 如果有从多个用户推荐了相同的物品，则应该对该物品的打分进行累加，让该物品得到更高的分数
    val userItemScoreSum = userItemScore.groupBy("user_id","item_id").agg(sum("score") as "finalScore")
    // 上面结束后，就可以将推荐列表这个结果，存到线上的redis等，供线上使用


    // 3.倒序排序score，取topN进行基于相似用户的最终推荐
    val user_base_recommend = userItemScoreSum.rdd
      .map(x=>(x(0).toString,(x(1).toString,x(2).toString)))
      .groupByKey()
      .mapValues(_.toArray.sortWith(_._2>_._2).slice(0,10))
      .flatMapValues(x=>x)
      .toDF("user_id","itemAndTopN")
      .selectExpr("user_id","itemAndTopN._1 as item_id","itemAndTopN._2 as finalScore")
    println("最终用户的推荐结果：")
    user_base_recommend.show()
    user_base_recommend.show(1,false)


  }
}
