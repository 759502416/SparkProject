package com.whx.offline

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object OperatorTest {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("spr").master("local[2]").getOrCreate()
    val ssc: SparkContext = session.sparkContext
    val rdd1: RDD[(Int, Int)] = ssc.parallelize(Array((1,1),(1,2),(2,1),(2,2),(3,1),(3,2)))
    val rdd2: RDD[(Int, Int)] = ssc.parallelize(Array((1,3),(1,4),(2,3),(2,4),(3,3),(3,4)))

    val rdd3: RDD[(Int, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    //(k,v).corgroup((k,v1))===>(k,((k,v),(k,v1))
    rdd3.foreach(x=>{
      x._2._1.foreach(y=>println(y))
      x._2._2.foreach(y=>println(y))
    })
  }
}
