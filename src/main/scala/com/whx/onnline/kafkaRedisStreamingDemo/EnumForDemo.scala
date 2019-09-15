package com.whx.onnline.kafkaRedisStreamingDemo

/**
  * 创建枚举类，用于替换参数
  */
object EnumForDemo extends Enumeration {
  type EnumForDemo = Value
  val brootStrapUrl = Value("192.168.199.111:6667")
  val hostAddress = Value("192.168.199.111")
}
