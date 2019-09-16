package com.whx.onnline.kafkaRedisStreamingDemo.redis

import java.util

import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

/**
  * 查找redis中保存的groupId 的 offset
  */
object JedisOffset {

  def apply(groupid:String)={
    // 定义Map，存储各分区的偏移量
    var partitionToLong: Map[TopicPartition, Long] = Map[TopicPartition,Long]()
    // 获得Jedis连接对象
    val jedis: Jedis = JPools.getJedis
    // 获得散列键的所有属性以及其属性对应的值
    val topicPartitionOffset: util.Map[String, String] = jedis.hgetAll(groupid)
    // 隐式转换，将java的Map转换成scala的map
    import scala.collection.JavaConversions._
    for(partitionOffset <- topicPartitionOffset){
      // 将key进行切割，获得topic Name  和 topic partition
      val strings: Array[String] = partitionOffset._1.split("--")
      // 将topic的每个分区的偏移量保存到partitionToLong对象中去    topicName,topic对应的分区编号-->对应分区的偏移量
      partitionToLong +=(new TopicPartition(strings(0),strings(1).toInt) -> partitionOffset._2.toLong)
    }
    partitionToLong
  }
}
