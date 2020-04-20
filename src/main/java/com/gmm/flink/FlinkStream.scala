package com.gmm.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkStream {
  def main(args: Array[String]): Unit = {
    //获取flink stream的环境对象
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    //通过socket的方式从网络中获取数据,监控master节点的9999端口
    val data = senv.socketTextStream("192.168.204.10",9999,'\n')
    //做wordCount，并测试和熟悉各api方法
    val wordCounts = data.map((_, 1L))
      .keyBy(0)
      //timeWindow(窗口总大小,滑动时间间隔),测试会看到wordCount统计的数量慢慢变小，因为时间窗口在滑动
//      .timeWindow(Time.seconds(5),Time.seconds(1))
      //前一个参数代表最大只能装下多少，即最大只能统计和打印10，第二个参数代表步长，及满足2的倍数才会打印
//      .countWindow(10,2)
      //该方法适合的业务是：统计一批数据到达
//      .countWindowAll(5)
      .sum(1)
    //设置打印，且并行度为1
    wordCounts.print().setParallelism(1)

    senv.execute("flink stream")

  }
}
