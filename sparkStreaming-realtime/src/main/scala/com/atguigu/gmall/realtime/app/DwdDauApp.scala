package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.PageLog
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.{SparkConf, streaming}

/**
 *  @author Adam-Ma 
 *  @date 2022/4/23 9:43
 *  @Project spark-realtime-1118
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   日活宽表
 *
 *    取数据来源： DWD_PAGE_LOG_TOPIC_1118
 *    操作步骤：
 *      1、准备SparkStreaming 环境
 *      2、读取 offset 偏移量
 *      3、从Kafka中（在偏移量处）消费数据
 *      4、根据消费到的数据，提取其中的结束 offset
 *      5、处理数据
 *        5.1 转换数据格式
 *        5.2 去重
 *        5.3 维度关联
 *      6、写入 ES
 *      7、提交 offsets
 *
 */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    // 1、准备 SparkStreaming 环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dwd_dau_app")
    val ssc: StreamingContext = new StreamingContext(sparkConf,streaming.Seconds(5))

    // topicName
    val topicName : String = "DWD_PAGE_LOG_TOPIC_1118"
    // groupId
    val groupId : String = "DWD_DAU_GROUP"
    // 2、从Redis 中获取偏移量
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName,groupId)

    // 3、消费数据
    // 从 指定offset 处消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName,groupId,offsets)
    }else{
      // 刚消费，按照默认的offset 进行消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    // 4、获取消费到的数据的 offset
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      consumerRecord => {
        offsetRanges = consumerRecord.asInstanceOf[HasOffsetRanges].offsetRanges
        consumerRecord
      }
    )

    // 5、处理数据
    // 5.1 转换结构
    val pageLogDStream: DStream[PageLog] = offsetRangesDStream.map(
      ConsumerRecord => {
        val JsonStr: String = ConsumerRecord.value()
        val pageLog: PageLog = JSON.parseObject(JsonStr, classOf[PageLog])
        pageLog
      }
    )

//    测试是否消费到 DWD_PAGE_LOG_TOPIC_1118 中的数据
//    pageLogDStream.print(100)
    // 自我审查前
    pageLogDStream.cache()    //  进行 cache 后，流就可以多次使用，如果不进行cache ，则只能使用一次
    pageLogDStream.foreachRDD(
      pageLog => println("自我审查前的数据量： " + pageLog.count())
    )
    
    // 5.2 去重 ：
    // 自我审查： 将 last_page_id != null (当前页面有上一级页面) 过滤掉，只保留每次访问的首页
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )
    // 自我审查后
    filterDStream.cache()
    filterDStream.foreachRDD(
      rdd => {
        println("自我审查后的数据量： " + rdd.count())
        println("*" * 150)
      }
    )

    /*  第三方去重 ： 自己的思路，无法实现
    // 5.3 第三方审查： 通过Redis 将当日活跃的mid 维护起来，自我审查后的每条数据需要到redis 中进行对比去重
      存储的类型 ：   string
      key       ：   DAU:MID
      value     :    JsonObjStr
      写入API    :    setnx
      读取API    :    get
      是否过期	  ：   不过期

    val secondFilterDStream: DStream[PageLog] = filterDStream.transform(
      rdd => {
        rdd.foreachPartition(
          pageLogIter => {
            val jedis: Jedis = MyRedisUtils.getJedisFromPool()

            for (pageLog <- pageLogIter) {
              val dauRedisKey: String = "DAU:" + pageLog.mid
              jedis.setnx(dauRedisKey, pageLog.toString)
            }
            // 关闭 jedis 连接
            MyRedisUtils.closeJedis(jedis)
          }
        )
        rdd
      }
    )
    secondFilterDStream.cache()
    secondFilterDStream.filter(
      PageLog => {
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        jedis.get("DAU:" + PageLog.mid) != null
      }
    )

    secondFilterDStream.foreachRDD(
      rdd => {
        println("第三方审查后的数据量： " + rdd.count())
        println("*" * 150)
      }
    )
  */

    // 开启 StreamingContext 环境
    ssc.start()
    ssc.awaitTermination()
  }
}
