package com.gmm.offline.lr

import com.gmm.offline.feat.SimpleFeature
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * 训练得到可以预测某个用户下一次会购买什么东西的模型，使用这个模型可以把这些东西推荐给特定用户
  */
object LRTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("LRTest").enableHiveSupport().getOrCreate()

    val user_order = spark.sql("select * from badou.orders")
    val order_prior = spark.sql("select * from badou.order_products_prior")
    val order_train = spark.sql("select * from badou.order_products_train")

    // eval_set字段有prior，train，test三个值，以此作为用户集合分析。单看去重后的user，总共有206209个user，其中prior = train + test
    // train类别的用户，作为训练集的正样本 label=1
    // prior类别的用户，属于历史数据，去掉train后的集合作为训练集负样本 label=0
    // test类别的用户，用于做预测集合
    val trainUser = user_order.filter("eval_set='train'").select("user_id").distinct() // count:131209
    val priorUser = user_order.filter("eval_set='prior'").select("user_id").distinct() // count:206209
    val testUser = user_order.filter("eval_set='test'").select("user_id").distinct() // count:75000

    // step 1：计算和组装好坏标签数据，得到训练集：user,product,label
    // 关联user-order-product的记录
    val uop = user_order.join(order_prior, "order_id")
    val uot = user_order.join(order_train, "order_id")
    import spark.implicits._
    // 正样本数量为train用户的购买商品记录
    val specimenGood = uot.select("user_id","product_id").distinct().withColumn("label",lit(1))
    // 负样本数量为prior中除开train用户的其它用户的商品购买记录
    val specimenBad = uop.select("user_id","product_id").distinct()
    // 使用user_id和product_id联合键进行outer连接，这样的话，train用户相关记录的label都已被打上1，而其它则为null，把null设置为0，则得到了我们的正负样本合在一起的训练集
    val trainLabelData = specimenGood.join(specimenBad,Seq("user_id","product_id"),"outer").na.fill(0)
    // 拼接上用户特征和商品特征，得到训练集大表
    val (productFeature, userFeature) = SimpleFeature.Feat(order_prior, user_order)
    val trainData = trainLabelData.join(userFeature,"user_id").join(productFeature,"product_id")

    // step 2：使用训练集训练模型
    // 特征处理通过rformula得到离散化的特征one-hot,连续特征不处理
    val rformula = new RFormula().setFormula("label ~ u_avg_day_gap + user_ord_cnt + u_prod_dist_cnt + u_avg_ord_prods + prod_sum_rod + prod_rod_rate + prod_cnt").setFeaturesCol("features").setLabelCol("label")
    // 使用rformula将训练集数据处理成新的数据形式：features是特征的向量vector，label是标签
    val rformulaTrainData = rformula.fit(trainData).transform(trainData).select("features","label").cache()
    // 算法为可收敛的逻辑回归，但是迭代停止是达到了最大的迭代次数，这里只设置迭代10次，因为太耗内存，否则迭代次数多得到的计算结果会更好
    // LR模型的定义
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0)
    // 划分训练集和测试集
    val Array(data_for_train,data_for_test) = rformulaTrainData.randomSplit(Array(0.7,0.3))
    // 模型训练
    // 跑完后会提示：19/11/28 16:43:08 WARN classification.LogisticRegression: LogisticRegression training finished but the result is not converged because: max iterations reached 表示达到了最大迭代次数
    val LR_Model = lr.fit(data_for_train)
    // 打印权重W和截距B
    print(s"Coefficients:${LR_Model.coefficients} intercept:${LR_Model.intercept}")
    // 使用训练的模型来验证测试集效果，好的话便可以用在线上给用户做商品推荐
    val test = LR_Model.transform(data_for_test)
    test.show()

    //step 3：评估模型训练和效果
    val trainSummary = LR_Model.summary
    // 查看损失loss
    val objectiveHistory = trainSummary.objectiveHistory
    objectiveHistory.foreach(loss => print(loss))
    // 查看ROC和AUC
    val binarySummary = trainSummary.asInstanceOf[BinaryLogisticRegressionSummary]
    val roc = binarySummary.roc
    // show出两列 FPR和TPR
    roc.show()
    println(binarySummary.areaUnderROC)

  }
}
