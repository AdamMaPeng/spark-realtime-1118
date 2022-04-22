package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

/**
 *  @author Adam-Ma 
 *  @date 2022/4/22 13:43
 *  @Project spark-realtime-1118
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   业务数据的 消费 + 分流
 *   步骤：
 *      1、SparkStreaming 环境准备
 *
 *      2、从 Redis 中读取偏移量
 *
 *      3、（从 offset处）获取消费的数据
 *
 *      4、提取到 消费的数据的最终偏移量
 *
 *      5、处理数据
 *
 *      6、将数据的偏移量保存到 Redis 中
 */
object OdsBaseDBApp {
  def main(args: Array[String]): Unit = {

    // 1、准备 SparkStreaming 环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ods_base_db_app")
    val ssc: StreamingContext = new StreamingContext(sparkConf,spark.streaming.Seconds(5))

    val topicName : String = "ODS_BASE_DB_1118"
    val groupId : String = "ODS_BASE_DB_1118_GROUP"
    // 2、从 Redis中读取offset
    val lastOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName,groupId)
    
    // 3、（从offsetr处）消费 数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (lastOffsets != null && lastOffsets.nonEmpty) {
      kafkaDStream  = MyKafkaUtils.getKafkaDStream(ssc,topicName,groupId,lastOffsets)
    }else{
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,topicName,groupId)
    }
    
    // 4、提取消费到的数据的最终偏移量
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangeDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5、处理数据
    //    5.1、转换数据结构
    val jsonObjStream: DStream[JSONObject] = offsetRangeDStream.map(
      ConsumerRecord => {
        val jsonString: String = ConsumerRecord.value()
        val jsonObject: JSONObject = JSON.parseObject(jsonString)
        jsonObject
      }
    )

    // 测试是否可以消费到 ODS_BASE_DB_1118 主题的数据
//   jsonObjStream.print(1000)

    // 如何 动态配置 表清单
    // 事实表清单
    val factTables: Array[String] = Array[String]("order_info", "order_detail")
    // 维度表清单
    val dimTables: Array[String] = Array[String]("user_info","base_province")

    // 5.2 分流
    jsonObjStream.foreachRDD(   // 此处由于需要直接触发执行，所以需要使用行动算子，通过 foreachRDD 来完成离散化流的循环遍历
      rdd => {
        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {
              // 提取操作类型
              val operType: String = jsonObj.getString("type")
              // 将操作类型进行转换，只进行一个标记
              val opValue: String = operType match {
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null
              }
              // 当 opValue ！=null 时，才是我们需要的数据，针对我们需要的数据进行处理
              if ( opValue != null){
                 // 获取 表名
                val tableName: String = jsonObj.getString("table")
                // 获取表中的数据
                val data: JSONObject = jsonObj.getJSONObject("data")

                  // 事实数据
                if (factTables.contains(tableName)){
                  // 例如： DWD_ORDER_INFO_I_1118  DWD_ORDER_INFO_U_1118 DWD_ORDER_INFO_D_1118
                  val dwdTopicName:String = s"DWD_${tableName.toUpperCase}_${opValue}_1118"
                  //分流数据
                  MyKafkaUtils.send(dwdTopicName, data.toString())
                }

                  // 维度数据 : 分流到 Redis
                if(dimTables.contains(tableName)){
                  /*
                      存入 Redis 中的类型： string    hash
                          // hash  ： 整个表存成一个hash ， 要考虑目前数据量大小和将来数据量增长 及 高频访问问题
                          // hash  :  一条数据存成一个hash
                          // String : 一条数据存成一个 jsonString
                      key     :   DIM:表名:ID
                      value   :   整条数据的 JsonString
                      写入 API :   set
                      读取 API :   get
                      过期不   :   不过期
                   */
                  // 获取 data 中的id： 用于拼接 redis 的 key
                  val id: String = data.getString("id")
                  // 拼接 redis 中的 key
                  val redisKey : String = s"DIM:${tableName}:${id}"
                  // 获取 Jedis 实例
                  val jedis: Jedis = MyRedisUtils.getJedisFromPool()
                  // 存储数据
                  jedis.set(redisKey, data.toString())
                  // 释放 jedis 连接
                  MyRedisUtils.closeJedis(jedis)
                }
              }

            }
          // 刷新 Kafka缓冲区
            MyKafkaUtils.flush()
          }
        )
        // 提交offset
        MyOffsetUtils.saveoffset(topicName,groupId,offsetRanges)
      }
    )

    jsonObjStream
    // 开启环境
    ssc.start()
    ssc.awaitTermination()
  }
}
