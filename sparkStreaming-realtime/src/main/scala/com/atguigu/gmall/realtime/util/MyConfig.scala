package com.atguigu.gmall.realtime.util

/**
 *  @author Adam-Ma 
 *  @date 2022/4/19 0:53
 *  @Project spark-realtime-1118
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   将 配置 文件中的所有 k,v 存储在对象中，直接在用到的地方调用
 */
object MyConfig {
  val KAFKA_BROKER_LIST = "kafka.broker.list"

  val REDIS_HOST="redis.host"
  val REDIS_PORT="redis.port"
}
