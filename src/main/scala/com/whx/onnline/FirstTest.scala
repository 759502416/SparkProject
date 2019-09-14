package com.whx.onnline

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object FirstTest {
  def main(args: Array[String]): Unit = {
    // 配置SparkStreaming
    val conf: SparkConf = new SparkConf().setAppName("The Streaming wordCount").setMaster("local[3]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
    // 获得离散流
    val receiverInputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.199.111",9999)
    val words: DStream[String] = receiverInputDStream.flatMap(x=>x.split(" "))
    val pairs: DStream[(String, Int)] = words.map(x=>(x,1))
    val value: DStream[(String, Int)] = pairs.reduceByKey((x, y) => x + y)

    value.print()
    // 常规操作-->启动它
    ssc.start()
    ssc.awaitTermination()

  }

}
