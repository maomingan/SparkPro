package com.class11.lr

import com.class11.offline.SimpleFeature.feat
import org.apache.spark.ml.classification.{BinaryLogisticRegressionTrainingSummary, LogisticRegression}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object LR_test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LR Test").enableHiveSupport().getOrCreate()
    val priors = spark.sql("select * from badou11.order_products_prior")
    val orders = spark.sql("select * from badou11.orders")
    val trains = spark.sql("select * from badou11.trains")

    val (userFeat,prodFeat) = feat(priors,orders)

    // user1的商品集合,用于验证结论：一个用户的prior商品集合，和它的train商品集合的交集，是train中的一部分商品
//    val df_u1_prior = orders.filter("user_id='1'").join(priors, "order_id").select("product_id").distinct()
//    val df_u1_train = orders.filter("user_id='1'").join(trains, "order_id").select("product_id").distinct()
//    val intersect_products = df_u1_prior.intersect(df_u1_train)

    val op = orders.join(priors, "order_id")
    val ot = orders.join(trains, "order_id")

    //给trains的数据，相当于训练集中最终买了的商品，全部打上label=1
    val user_real = ot.select("product_id","user_id").distinct().withColumn("label",lit(1))
    //将用户特征、商品特征和label拼成训练集大表，并将合并后label为null的调整为0
    val trainData = op.join(user_real, Seq("product_id","user_id"),"outer")
      .select("user_id","product_id","label").distinct().na.fill(0)
    val train = trainData.join(userFeat,"user_id").join(prodFeat,"product_id")

    // 模型训练
    // setFormula是使用字符串的方式来得到特征的向量对应label，特征的变量可以自己增减，然后设置两个列名字
    // 将对应值（不管是离散还是连续）转变成向量形式放入到feature这一列中
    val rformula = new RFormula().setFormula("label~ u_avg_day_gap + user_ord_cnt + " +
      "u_prod_dist_size + userOrderAvgCnt + prod_sum_rod + prod_rod_rate + prod_cnt")
      .setFeaturesCol("features").setLabelCol("label")
    // 进一步对数据进行transform，这一步才转化为向量
    // Rformula处理之后的数据，features这列对应的是向量，label对应的是标签。
    val df = rformula.fit(train).transform(train).select("features","label")
    // 实例化或者说定义逻辑回归模型  setMaxIter设置迭代次数，setRegParam设置正则表达式，现在设置为0
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0)
    // 随机切分数据分为两部分，按7:3的比例设置训练集和测试集
    val Array(trainningData, testData) = df.randomSplit(Array(0.7,0.3))
    // 开始训练模型
    val lrModel = lr.fit(trainningData)
    // 打印参数 weights为权重w   intercept为截距b, 即这样就得到了公式 y=w1x1+w2x2+....+wnxn + b
    print(s"weights :  ${lrModel.coefficients} intercept : ${lrModel.intercept}")
    // 到这里，我们就得到了一个概率预测模型，我们就可以使用这个公式来计算，test下的用户数据，以及该用户历史购买过的商品prod，传入上诉的7各维度变量数据，可预测该用户对于该产品会被再次购买的概率[0,1]

    // 收集过程日志
    val trainingSummary = lrModel.summary
    val objectHistory = trainingSummary.objectiveHistory
    // 打印loss
    objectHistory.foreach(loss=>println(loss))

    // 打印模型评价指标 roc和auc
    // 用auc来评价模型训练结果 auc是roc曲线下的面积
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionTrainingSummary]
    val roc = binarySummary.roc
    roc.show()
    print(binarySummary.areaUnderROC)

  }
}
