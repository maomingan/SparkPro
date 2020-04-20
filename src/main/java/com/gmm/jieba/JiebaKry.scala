package com.gmm.jieba

import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object JiebaKry {
  def main(args: Array[String]): Unit = {

    //定义结巴分词这个类的序列化
    val conf = new SparkConf().registerKryoClasses(Array(classOf[JiebaSegmenter])).set("spark.rpc.message.maxSize","800")
    //建立sparkSession,并传入定义好的conf
    val spark = SparkSession.builder().appName("Jieba UDF").enableHiveSupport().config(conf).getOrCreate()
    //定义结巴分词的方法，传入的是DataFrame，返回的也是DataFrame，只是多一列seg（分好词的一列）
    def jieba_seg(df:DataFrame,colName:String):DataFrame={
      val segmenter = new JiebaSegmenter()
      //广播对象 从driver直接广播把segmenter变量对象直接发送给需要执行的Executor，Executor拿到后的对象就是seg
      val seg = spark.sparkContext.broadcast(segmenter)
      val jieba_udf = udf{(sentence:String)=>
        // Executor从广播中拿到值，然后处理
        val segValue = seg.value
        segValue.process(sentence.toString,SegMode.INDEX).toArray().map(_.asInstanceOf[SegToken].word).filter(_.length>1).mkString("/")
      }
      return df.withColumn("jiebaSeg",jieba_udf(col(colName)))
    }

    val df = spark.sql("select sentence,label from badou.allfiles_noseg limit 300")
    val df_seg = jieba_seg(df,"sentence")
    df_seg.show()
    //将结果存储到hive表中
    df_seg.write.mode("overwrite").saveAsTable("badou.allfiles_jieba")

  }
}
