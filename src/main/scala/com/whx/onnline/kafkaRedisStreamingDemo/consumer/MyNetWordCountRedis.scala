package com.whx.onnline.kafkaRedisStreamingDemo.consumer

import com.whx.onnline.kafkaRedisStreamingDemo.EnumForDemo
import com.whx.onnline.kafkaRedisStreamingDemo.redis.{JPools, JedisOffset}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}
import redis.clients.jedis.Jedis

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
    val topic = "wordCount1"

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
    // 获得 主题偏移量保存对象
    val partitionToLong: Map[TopicPartition, Long] = JedisOffset(groupId)
    // 判断是否是初次消费
    val stream: InputDStream[ConsumerRecord[String, String]] ={
      if(partitionToLong.size == 0){
        KafkaUtils.createDirectStream(ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams))
      }else{
        // kafka的Subscribe 和 assign 模式的区别 见博客 https://www.cnblogs.com/dongxiao-yang/p/7200971.html
        // 大致总结：assign指定consumer消费partition，且不会拥有consumerManager，故不会发生rebalance
        System.err.println("我要开始续消费了")
        KafkaUtils.createDirectStream(ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign(partitionToLong.keys,kafkaParams,partitionToLong))
      }
    }

    // 创建在Master节点上的Driver
    stream.foreachRDD(rdd=>{
      // val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges 是固定写法
      // 可以获取RDD的消费分区和各分区的消费偏移量
      val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val reduced: RDD[(String, Int)] = rdd.map(x=>(x.value(),1)).reduceByKey(_+_)

      // 为什么要使用foreachParition？为什么将redis放在算子里？
      // 见博客 https://blog.csdn.net/legotime/article/details/51836039
      reduced.foreachPartition(rdd=>{
        val redis: Jedis = JPools.getJedis
        // 连接redis
        rdd.foreach({
              // redis API https://blog.csdn.net/qq_40110871/article/details/84962554
              // 将名称为wordcount的hash中x._1的value增加x._2.toLong
          x=>redis.hincrBy("wordcount",x._1,x._2.toLong)
            redis.close()
        })
      })

      // 将偏移量保存至redis
      val redis: Jedis = JPools.getJedis
      offsetsList.foreach(x=>{
        val topic: String = x.topic
        val partition: Int = x.partition
        val offset: Long = x.untilOffset
        redis.hset(groupId,topic+"--"+partition,offset.toString)
        System.err.println("主题:"+topic+"分区："+partition+"偏移量："+offset)
      })

    })
    ssc.start()
    ssc.awaitTermination()
  }
}
