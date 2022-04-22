package com.atguigu.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 *  @author Adam-Ma 
 *  @date 2022/4/22 13:56
 *  @Project spark-realtime-1118
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
 *  @author Adam-Ma
 *  @date 2022/4/21 19:38
 *  @Project spark-realtime-1118
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
 *   通过 SparkStreaming 消费 Kafka 中的数据，使用默认 提交offset 的方式，所以有如下两个问题：
 *      重复消费： 先消费，后提交offset，但是在 提交 offset 过程中失败了，则下次消费从原offset 进行消费，导致重复消费
 *      漏消费 ：  先提交 offset ，后消费。当offset 提交后，数据如果还在 内存中，没有消费成功，此时挂了，则会导致漏消费
 *
 *  如何解决 重复消费 和 漏消费问题？
 *      事务 ： 需要有支持事务的数据下游，例如关系型数据库（海量场景，不适用） ，
 *            分布式锁，比较麻烦
 *
 *      后置提交偏移量 offset + 幂等性：
 *          后提交 offset ： 保证数据不丢失，防止漏消费
 *          幂等 ： 保证数据只一份，防止重复消费
 *
 *      后置提交 offset ：
 *         需要手动提交 offset ，才能保证数据消费后，再提交 offset
 *        1、能否使用当前消费 kafka 中的数据的 SparkStreaming 来提交 offset ？
 *            SparkStreaming 提供了 手动提交 offset 的方式：
 *               val offsetRanges: Array[OffsetRange] = xxDstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
 *            但如上的代码，只能是由 InputDStream 形式的流进行获取 offsetRanges: Array[OffsetRange] 对象，其中有 offset 的信息
 *            对于当前的项目而言，我们通过 SparkStreaming 获取到 Kafka 中的数据，首先转换了流对象的格式，转换成了 DStream[JSONObject]
 *           格式的流，然后还要进行数据的分流。为了保证这些业务流程的正常执行，所以通过 SparkStreaming 提交 offset 的方式不行。
 *
 *        2、采用自己维护 offset 的方式：
 *            将 offset 存储在 Redis 中，消费数据的时候，kafka 先从 Redis 中获取offset 信息，当数据消费完成后，将消费完的数据的最后 的offset
 *            记录在 Redis 中
 *
 *        3、offset 的
 */
object MyOffsetUtil {
  // 存 offset 到 Redis
  /**
   *  问题：
   *      存储的 offset 从哪里来？
   *          从 消费的数据中提取到，然后传递到当前的方法中
   *         offsetRange: Array[OffsetRange]
   *       offset 的结构？
   *         kafka中保存 offset 的格式为 ：gtp
   *              groupId + topic + partition --> offset
   *      将 offset 存储到Redis 中：
   *         类型： hash
   *         key :  offset:topic:groupId
   *         value:  partition-offset 、partition-offset ……
   *         写入 API： hset
   *         读取 API: hgetAll
   *         是否过期： 不过期
   *
   */
  def saveOffset(topic: String , groupId: String ,offsetRanges: Array[OffsetRange]): Unit ={
    // 对 offsetRanges 进行判断
    if( offsetRanges != null && offsetRanges.length > 0) {
      // 将 offset 作为 value 保存到 Redis 中
      val offsets = new util.HashMap[String,String]()
      for( offsetRange <- offsetRanges) {
        val partition: Int = offsetRange.partition
        val untilOffset: Long = offsetRange.untilOffset
        offsets.put(partition.toString,untilOffset.toString)
      }

      // 获取 Redis 实例
      val jedis: Jedis = MyRedisUtils.getJedisFromPool()
      // 拼接 key
      val offsetKey = s"offset:$topic:$groupId"
      jedis.hset(offsetKey, offsets)

      // 关闭 jedis 连接
      MyRedisUtils.closeJedis(jedis)
    }
  }

  // 从 Redis 中获取到 offset
  /**
   *   由于获取到 offset ，需要传递到 SparkStreaming ，指示其从 kafka 中对应 offset处进行消费，所以需要去看一下 SparkStreaming 中是否提供了从某处消费数据的能力
   * @param topic
   * @param groupId
   */
  def readOffset(topic:String, groupId:String): Map[TopicPartition, Long] ={
    // 获取 Jedis 实例
    val jedis: Jedis = MyRedisUtils.getJedisFromPool()

    // 拼接 key
    val offsetKey = s"offset:$topic:$groupId"

    // 获取 offset 对象
    val offsetValues: util.Map[String, String] = jedis.hgetAll(offsetKey)

    // 将获取到的 util.Map 转换成 SparkStreaming 消费数据时能够正常识别的 Map[TopicPartition, Long]
    val offsetMap: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
    // 将 java 中的 map 转换成 scala 中的map
    import scala.collection.JavaConverters._
    for ((partition, offset) <- offsetValues.asScala) {
      val topicPartition = new TopicPartition(topic,partition.toInt)
      offsetMap.put(topicPartition,offset.toLong)
    }
    MyRedisUtils.closeJedis(jedis)
    offsetMap.toMap
  }
}

