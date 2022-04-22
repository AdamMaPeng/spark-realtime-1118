package com.atguigu.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 *  @author Adam-Ma 
 *  @date 2022/4/20 15:16
 *  @Project spark-realtime-1118
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
 *  Redis 工具类
*     获取 Jedis 、关闭 Jedis实例
 */
object MyRedisUtils{
  var jedisPool : JedisPool = null

  /**
  *  获取 Jedis 实例
   */
  def getJedisFromPool(): Jedis ={
    if (jedisPool == null) {
      // JedisPoolConfig 的配置
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      // host, port 的获取
      val host = MyPropsUtils(MyConfig.REDIS_HOST)
      val port = MyPropsUtils(MyConfig.REDIS_PORT)

      // 创建 jedis 连接池
      jedisPool = new JedisPool(jedisPoolConfig,host,port.toInt)
    }
    jedisPool.getResource
  }

  /**
  *  关闭 Jedis 连接
   */
  def closeJedis(jedis: Jedis): Unit ={
    if (jedis != null) {
      jedis.close()
    }
  }

  /**
  *  测试一下 Jedis 连接池
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = getJedisFromPool()

    val str: String = jedis.ping()
    println(str)

    closeJedis(jedis)
  }
}
