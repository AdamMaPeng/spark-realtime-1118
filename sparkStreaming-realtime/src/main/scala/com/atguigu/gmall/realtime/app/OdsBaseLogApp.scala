package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *  @author Adam-Ma 
 *  @date 2022/4/18 15:16
 *  @Project spark-realtime-1118
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
 *   日志数据的消费和分流
 *      1、准备实时环境
 *      2、从 kafka 中消费数据
 *      3、处理数据
 *        3.1、转换数据结构
 *            通用结构 ： Map  JsonObject
 *            专用结构： 自己封装的 Bean 对象
 *
 *      4、将处理好的数据分流到 Kafka
 *  */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    // 创建SparkStreaming 环境
    val sc: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ods_base_log_app")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    // topic
    val topicName : String = "ODS_BASE_LOG_1118"
    // GroupId
    val groupId : String = "ODS_BASE_LOG_GROUP"
    // 准备 消费
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)

    // 由于 得到的是 ConsumerRecord[String, String] 对象，
    // 1、不支持序列化，所以无法在当前场景下打印
    // 2、处理起来很不方便，我们需要的只是 其中的 Value ：String，也就是 json格式的数据
    // 所以需要转换格式， 将 ConsumerRecord --》 JsonObject
    // fastJson 提供了将 String 转换成 JsonObject 的能力

    // 转换格式
    val jsonObjStream: DStream[JSONObject] = kafkaDStream.map(
      ConsumerRecord => {
        // 获取到 其中的 value：String
        val jsonStr: String = ConsumerRecord.value()
        // 将 jsonStr 转换成 JsonObject
        val jsonObject: JSONObject = JSON.parseObject(jsonStr)
        jsonObject
      }
    )
     // 此次打印只是为了查看是否 从 Kafka 中消费到数据
//    jsonObjStream.print(100)
    
    // 开启
    ssc.start()
    ssc.awaitTermination()
  }
}
