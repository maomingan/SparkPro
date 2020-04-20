package com.gmm.streaming

import com.alibaba.fastjson.JSON

/**
  * scala可以直接调用java的类
  */
object JsonTest {

  def main(args: Array[String]): Unit = {
    //定义一个类，一个类就是一个数据结构
    /*case class Order(order_id:String,
                     user_id:String,
                     eval_set:String,
                     order_number:String,
                     order_dow:String,
                     hour:String,
                     day:String)*/

    //val s = """{"order_id":1514738,"user_id":1493,"eval_set":"prior","order_number":1,"order_dow":3,"hour":17,"day":20}""".stripMargin
    //val s = "{\"order_id\":1514738,\"user_id\":1493,\"eval_set\":\"prior\",\"order_number\":1,\"order_dow\":3,\"hour\":17,\"day\":20}"
    //val s = "{'order_id':1514738,'user_id':1493,'eval_set':'prior','order_number':1,'order_dow':3,'hour':17,'day':20}"
    val s = "{\"order_id\": 2398795, \"user_id\": 1, \"eval_set\": \"prior\", \"order_number\": 2, \"order_dow\": 3, \"hour\": 7, \"day\": 15.0}"

    val mess = JSON.parseObject(s, classOf[Orders])
    println(mess.order_id,mess.user_id,mess.eval_set,mess.order_number,mess.order_dow,mess.hour,mess.day)

  }

}
