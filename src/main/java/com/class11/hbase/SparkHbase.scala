package com.class11.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._

/**
  * client去通过rdd的数据形式去请求hbase，写数据
  */
object SparkHbase {
  def main(args: Array[String]): Unit = {

    // zookeeper
    val ZOOKEEPER_QUORUM = "192.168.204.10,192.168.204.11,192.168.204.12"
    // 读取hive中的数据写入hbase，创建sparksession
    val spark = SparkSession.builder().appName("spark let hive to hbase").enableHiveSupport().getOrCreate()

    // 使用rdd来操作
    val rdd = spark.sql("select order_id,user_id,order_dow from badou11.orders limit 300").rdd

    // 一个put对象就是一行记录，在构造方法中主键rowkey为user_id
    // 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes这个包
    rdd.map{row=>
      val order_id = row(0).asInstanceOf[String]
      val user_id = row(1).asInstanceOf[String]
      val order_dow = row(2).asInstanceOf[String]
      // 加处理逻辑,设置user_id为rowkey
      val p = new Put(Bytes.toBytes(user_id))
      // id 列族存放所有id类型列，order为列，value对应的order_id
      p.add(Bytes.toBytes("id"),Bytes.toBytes("order_id"),Bytes.toBytes(order_id))
      // num为列族存放所有num数值类型的列，dow为列，order_dow为具体值
      p.add(Bytes.toBytes("num"),Bytes.toBytes("order_dow"),Bytes.toBytes(order_dow))
      p
    }.foreachPartition{partition=>
      // spark作业是针对每一份partition来进行的作业，这里是对每一份partition数据进行的具体操作
      // 任务配置
      val jobconf = new JobConf(HBaseConfiguration.create())
      // 指定zk集群
      jobconf.set("hbase.zookeeper.quorum",ZOOKEEPER_QUORUM)
      jobconf.set("hbase.zookeeper.property.clientPort","2181")
      // 指定这个数据是在zk的哪个节点下，节点的全路径，这样才能正常访问zk下的hbase信息
      jobconf.set("zookeeper.znode.parent","/hbase")
      jobconf.setOutputFormat(classOf[TableOutputFormat])
      // 写入的表名  需要现在Hbase里建好表
      val table = new HTable(jobconf,TableName.valueOf("orders"))
      // 真正写入数据
      table.put(seqAsJavaList(partition.toSeq))
    }

  }
}
