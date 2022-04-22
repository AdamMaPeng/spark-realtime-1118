package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.{SparkConf, streaming}

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
    val ssc: StreamingContext = new StreamingContext(sparkConf,streaming.Seconds(5))

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

    // 转换数据结构
    val jsonObjStream: DStream[JSONObject] = offsetRangeDStream.map(
      ConsumerRecord => {
        val jsonString: String = ConsumerRecord.value()
        val jsonObject: JSONObject = JSON.parseObject(jsonString)
        jsonObject
      }
    )

    // 测试是否可以消费到 ODS_BASE_DB_1118 主题的数据
    println(jsonObjStream)

    jsonObjStream
    // 开启环境
    ssc.start()
    ssc.awaitTermination()
  }

}
