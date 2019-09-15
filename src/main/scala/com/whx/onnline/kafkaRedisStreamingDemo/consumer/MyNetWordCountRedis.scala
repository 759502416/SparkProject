package com.whx.onnline.kafkaRedisStreamingDemo.consumer

import com.whx.onnline.kafkaRedisStreamingDemo.EnumForDemo
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * 将 kafka streaming 和 redis 进行整合 实现词频统计
  */
object MyNetWordCountRedis {
  def main(args: Array[String]): Unit = {
    //创建SparkStreaming 对象
    val conf: SparkConf = new SparkConf().setAppName("The Streaming wordCount").setMaster("local[3]")
    // sparkStreaming的详细配置请见官网：https://spark.apache.org/docs/latest/configuration.html#spark-streaming
    // 设置从kafka拉取数据的最大分区数为 5
    conf.set("spark.streaming.kafka.maxRatePerPartition","5")
    // 如果为真，Spark会在JVM关闭时优雅地关闭StreamingContext，而不是立即关闭。
    conf.set("spark.streaming.kafka.stopGracefullyOnShutdown","true")
    val ssc: StreamingContext = new StreamingContext(conf,Durations.seconds(5))

    // 定义一个消费者
    val groupId = "sp01"
    // 订阅主题
    val topic = "wordCount"

    /**
      * 配置kafka
      */
    val kafkaParams: Map[String, Object] = Map[String,Object](
      "bootstrap.servers" -> EnumForDemo.brootStrapUrl.toString,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 连接kafka，创造direct
    // LocationStrategies.PreferConsistent 表示kafka的各分区在spark上的各分区上均匀分布
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams))

    // 创建在Master节点上的Driver
    stream.foreachRDD(rdd=>{

    })
  }
}
