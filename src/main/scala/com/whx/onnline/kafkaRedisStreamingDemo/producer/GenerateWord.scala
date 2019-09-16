package com.whx.onnline.kafkaRedisStreamingDemo.producer

import java.util.{Date, Properties, UUID}

import com.whx.onnline.kafkaRedisStreamingDemo.EnumForDemo
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
  * 创建一个生产者，生成随机的key和value
  * 用于实现词频流统计
  */
object GenerateWord {
  def main(args: Array[String]): Unit = {
    val properties: Properties = new Properties()
    // 设置kafka集群的ip，由于我是搭建的单节点，故只写一个，正常第二个参数应该是192.168.199.111:6667，192.168.199.112:6667，
    // 192.168.199.113:6667，192.168.199.1124:6667 类似这种 把所有的kafka节点都包进去
    properties.setProperty("bootstrap.servers",EnumForDemo.brootStrapUrl.toString)
    // 实现kafka 的生产消息的kv序列化需要指定序列化方式
    // 更多的序列化和反序列化方式请见大佬的博客 https://blog.csdn.net/shirukai/article/details/82152172
    properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.put("acks","all")
    //kafkaProducerRetries
    properties.put("retries","3")
    properties.put("batch.size","16384")
    // 配置如有不理解，请见官方网站http://kafka.apache.org/0110/documentation.html#config
    properties.put("linger.ms","1")
    properties.put("buffer.memory", "33554432")

    // 创建一个生产者的客户端实例
    // 由于时间问题，后续会更新更复杂的demo，现此处模拟循环获取数据,后续会替换
    val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](properties)
    // 定义一个计数器
    var count = 0;
    try {
      while (true) {
        Thread.sleep(500);
        val key: String = UUID.randomUUID().toString
        val value: Int = new Random().nextInt(26) + 97
        // 可指定分区数，也可以不指定，这里我不指定
        val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("wordCount1", key, value.toString)
        producer.send(record)
        count+=1;
      }
    } finally {
      System.err.println("已发送"+count+"条消息")
      producer.close()
    }
  }
}
