package com.class11.nlp

import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, udf}

/**
  * 将分好词的文章，处理成向量格式（词典大小，[每个词的位置index向量数组],[每个词的向量值(tf或tf-idf)数组]）
  * 最终处理完的数据为如下，然后就可以使用余弦相似度进行文章相似性计算了
  * | 无锡 双龙 雷斯特 优惠 元 另 赠送 礼包 搜狐 汽车 购车 咨询 热线 ： 转 搜狐 汽车 无锡 站 编辑 ： 赏 车 、 购车 、 聊 车 、 玩 车 搜狐 汽车 无锡 车友 群 ： （ 此 群 已 满 ） 搜狐 汽车 无锡 车友 群 ： （ 请 加 此 群 ） 搜狐 汽车 无锡 车友 群 ： （ 此 群 已 满 ） 搜狐 汽车 无锡 车友 群 ： （ 此 群 已 满 ） 搜狐 汽车 无锡 车友 群 ： （ 此 群 已 满 ） 搜狐 汽车 无锡 车友 群 ： （ 此 群 已 满 ） （ 加 群 须知 ： 新人 报道 并 改 网名 ， 格式 为 区 名 车型 网名 ） 无锡 车友 会 ： 责任 编辑 ： 杨 冲 冲 分享##@@##auto
  * |(42363,
  * [132,1063,1201,1729,1788,2859,2946,3536,4612,4751,4937,5953,8088,8936,10393,11738,14649,17946,18835,20121,20516,22112,22803,24461,24462,25199,25257,25556,26514,26669,28448,28719,29657,30102,30537,31013,31239,31657,32188,32455,35371,35710,36918,37061,37937,38369,39497,41375,42361],
  * [1.4760607060072688,0.9041531319094054,0.8408712963651976,4.562789755729599,0.9142033700665557,2.8157601911099666,1.7770907016712498,5.108763635261146,2.1594256370127116,1.8847245800710795,2.4103662551467733,0.9649929217599185,1.1857545757350607,1.55102546765373,1.563799755690159,0.6146282986507491,1.21051453151622,2.3291305533302453,5.381809108109933,0.012386175014852709,2.047451877568779,1.3291767180373737,5.521586246916058,2.3254591813680934,1.9274765604920294,0.16379044241516155,16.416006880340312,0.33872203594360417,3.10205093530746,2.912753303671323,9.467136423152532,0.8648297513541402,1.303514727716237,0.43605014046270496,0.45285845092617116,0.9396254500716243,0.8495450836001117,2.2813948778647997,2.544776518376729,1.2627302592753957,1.325416569164067,15.064840932805676,0.3604907807057758,0.0,3.088844562727004,1.5573656456847491,1.6738712147561863,2.3148295853173004,-1.7693806800184422E-4]
  * )|
  */
object TfIdfTransform {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("tf-idf").enableHiveSupport().getOrCreate()

    val df = spark.sql("select sentence from badou11.news_seg")
    val df_seg = df.selectExpr("split(split(sentence,'##@@##')[0],' ') as seg")
    val doc_size = df_seg.count()
    // 在spark shell里这样执行，能展示完整的一行数据
    //df_seg.show(1,false)

    /**
      * 实现方式一：spark中自带的tf-idf方法
      */
    // hasingtf 把文章[word1,word2,...] 转化成 sparse vector向量(2^18,[word location index],[word count])
    // setBinary(false)多项式分布：一个词在一篇文章中出现多少次，就算多少次。setBinary(true):伯努利分布：一个词在文章中出现多少次都为1，用来标识是否出现过
    // 然后设置传入的列seg，得到计算好后传出的列feature_tf,并设置词典的大小为2的18次方个词
    val hashingtf = new HashingTF().setBinary(false).setInputCol("seg").setOutputCol("feature_tf").setNumFeatures(1<<18)
    val df_tf = hashingtf.transform(df_seg).select("feature_tf")

    // idf 对word进行idf加权
    // setMinDocFreq(2)进行过滤：对一个词，至少出现在2篇文章中
    val idf = new IDF().setInputCol("feature_tf").setOutputCol("feature_tfidf").setMinDocFreq(2)
    val idfModel = idf.fit(df_tf)
    val df_tfIdf = idfModel.transform(df_tf).select("feature_tfidf")

    /**
      * 实现方式二：自己实现
      */
    // 1.文档频率的计算 doc freq -> 所有文章的单词集合（词典）
    val setUDF = udf((str:String)=>str.split(" ").distinct)
    val df_set = df.withColumn("words_set",setUDF(col("sentence")))
    // 行转列 得到每个词出现在多少count的文章中
    // collectAsMap 将数据回收到driver端，且只有转成rdd才能做这个操作,好处是会变成全局都要使用的变量，最后方便分发到所有节点上，实际工作中尽量少用，消耗性能
    val docFreq_map = df_set.select(explode(col("words_set")) as ("word")).groupBy("word").count().rdd
      .map(x=>(x(0).toString,x(1).toString)).collectAsMap()
    // 词表大小 42363
    val dictSize = docFreq_map.size
    // 对每个词进行编码，分配给每个词一个对应的位置
    val wordEncode = docFreq_map.keys.zipWithIndex.toMap

    // 2.词频计算 term freq 对每篇文章（一行数据）统计词频
    // 定义UDF，将分好词的文章直接转换成向量形式
    val mapUDF = udf { (str: String) =>
      val tfMap = str.split("##@@##")(0).split(" ").map((_,1L)).groupBy(_._1).mapValues(_.length)
      val tfIdfMap = tfMap.map{x=>
        val idf_v = math.log10(doc_size.toDouble/(docFreq_map.getOrElse(x._1,"0.0").toDouble+1.0))
        (wordEncode.getOrElse(x._1,0), x._2.toDouble * idf_v)
      }
      Vectors.sparse(dictSize, tfIdfMap.toSeq)
    }
    val dfTF = df.withColumn("tf_idf",mapUDF(col("sentence")))



  }
}
