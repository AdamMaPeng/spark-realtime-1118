package com.atguigu.gmall.realtime.util

import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 *  @author Adam-Ma
 *  @date 2022/4/18 14:06
 *  @Project spark-realtime-1118
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   Kafka 的工具类，提供基于 Kafka 的生产和消费功能
 *     1、创建生产者，生产数据发送到 Kafka 种
 *     2、创建消费者，消费Kafka 中的数据
 */
object MyKafkaUtils {
  /**创建生产者的方法
   * @return
   */
  def createProducer(): KafkaProducer[String, String] = {
    // 创建 map 类型的 配置对象
    val producerParams = new util.HashMap[String, AnyRef]();
    // 将配置参数 put 到 配置对象中
    // kafka的集群地址
    producerParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
    // kv 的 序列化
    producerParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    // bufferMemory、 batch.size、linger.ms、compression、retries

    // 创建 生产者对象
    val kafkaProducer = new KafkaProducer[String, String](producerParams)
    kafkaProducer
  }

  /**
  *  创建生产者
   */
   val producer: KafkaProducer[String, String] = createProducer()

  /**
  *   生产者生产数据
   */
  def send(topic: String, message: String) = {
    producer.send(new ProducerRecord[String, String](topic, message))
  }

  /**
  *  消费者的 配置
   */
  val consumerParams: mutable.Map[String, Object] = mutable.Map[String, Object](
     // kafka 集群信息
//     ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
     ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BROKER_LIST),
     // kv 的反序列化
     ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
     ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
     // Offset 提交方式： 自动提交
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    // offset 自动提交间隔
     ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5000",
     //offset 是否重置
     ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
   )

  /**
  *   从 kafka中消费数据，
   *   kafka 作为数据源：KafkaUtils.createDirectStream()
   *   获取到的DStream 对象即为 需要消费的数据
   */
  def getKafkaDStream1(ssc : StreamingContext, topic : String, groupID : String) = {
    // 消费者组 配置，通过外部指定
    consumerParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
    // 通过 KafkaUtils获取 DStream 对象
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerParams)
    )
    kafkaDStream
  }

}
