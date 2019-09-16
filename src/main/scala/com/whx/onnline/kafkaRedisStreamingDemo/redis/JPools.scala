package com.whx.onnline.kafkaRedisStreamingDemo.redis

import com.whx.onnline.kafkaRedisStreamingDemo.EnumForDemo
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * redis 连接池
  */
object JPools {

  private val config: GenericObjectPoolConfig = new GenericObjectPoolConfig
  // 最大的空闲连接数 连接池中最大的空闲连接数默认8
  config.setMaxIdle(8)
  // 支持最大的连接数  默认为8
  config.setMaxTotal(2000)

  // 连接池是私有的 不能对外公开访问 端口的缺省值就是6379，故只需填地址
  private val pool: JedisPool = new JedisPool(config,EnumForDemo.hostAddress.toString)

  def getJedis = {
    val resource: Jedis = pool.getResource
    resource.select(0)
    resource
  }

}
