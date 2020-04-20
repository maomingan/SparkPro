package com.gmm.algorithm

object Lcs {
  def main(args: Array[String]): Unit = {


    System.out.println("hello world scala")

    //scala生成重复句子，协助hive更好处理数据

    /*val str = (a:String) =>s"sum(case when order_dow='$a' then 1 else 0 end) as dow_$a"
    val arr = (0 to 6)
    var result1 = arr.map(x=>str(x.toString)).mkString(",")
    var result2 = arr.map(x=>str(x.toString)).mkString("{",",","}")
    System.out.println(result1)
    System.out.println(result2)*/

    var a = "BDCABA"
    var b = "ABCBDAB"

    System.out.println(LCS_asc(a,b))
    System.out.println(LCS_desc(a,b))

  }

  def LCS_asc(a:String,b:String):Double={
    val n = a.length
    val m = b.length;
    val arr = Array.ofDim[Int](n+1,m+1)
    for(i <- 1 to n){
      for(j <- 1 to m){
        if(a(i-1)==b(j-1)){
          arr(i)(j) = arr(i-1)(j-1)+1
        }else{
          arr(i)(j) = arr(i-1)(j).max(arr(i)(j-1))
        }
      }
    }
    return arr(n)(m).toDouble/((m+n).toDouble/2)
  }

  def LCS_desc(a:String,b:String):Double={
    val n = a.length
    val m = b.length
    val arr = Array.ofDim[Int](n+1,m+1)
    for(i<-0 until n reverse){
      for(j<-0 until m reverse){
        if(a(i)==b(j)){
          arr(i)(j) = arr(i+1)(j+1)+1
        }else{
          arr(i)(j) = arr(i+1)(j).max(arr(i)(j+1))
        }
      }
    }
    return arr(0)(0).toDouble/((n+m).toDouble/2)
  }
}
