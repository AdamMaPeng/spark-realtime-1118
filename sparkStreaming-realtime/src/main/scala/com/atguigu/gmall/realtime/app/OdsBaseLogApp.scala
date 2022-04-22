package com.atguigu.gmall.realtime.app

import java.lang

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.bean
import com.atguigu.gmall.realtime.bean.{PageDisplayLog, PageLog, StartLog}
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
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
    val topicName: String = "ODS_BASE_LOG_1118"
    // GroupId
    val groupId: String = "ODS_BASE_LOG_GROUP"

    // TODO 从 Redis 中读取 offset ，指定 offset 进行消费
    /**
     * 在进行消费前，先获取到 当前的 offset
     */
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)
    // 对 offsets 进行判断，如果不为null ，且 length > 0，则表明需要从当前 的offset进行消费；反之，则为第一次消费，不需要指定offset
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      // 指定 offset 进行消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else{
      // 首次开启 SparkStreaming ，获取不到 offset ，则使用默认的消费方式
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName,groupId)
    }
    /*
         val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)
         var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
         if (offsets != null && offsets.nonEmpty) {
           // 指定 offset 进行 消费
           kafkaDStream =
               MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
         }else{
            // 默认 offset 进行消费
            // 准备 消费
            kafkaDStream =
               MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
         }
     */

    // TODO   补充： 从当前消费到的数据中提取 offsets ， 不对流中的数据做任何处理
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    /*
        var offsetRanges: Array[OffsetRange] = null
        val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
          rdd => {
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // 在哪里执行？ driver 端
            rdd
          }
        )

     */

    // 由于 得到的是 ConsumerRecord[String, String] 对象，
    // 1、不支持序列化，所以无法在当前场景下打印
    // 2、处理起来很不方便，我们需要的只是 其中的 Value ：String，也就是 json格式的数据
    // 所以需要转换格式， 将 ConsumerRecord --》 JsonObject
    // fastJson 提供了将 String 转换成 JsonObject 的能力

    // 转换格式
    val jsonObjStream: DStream[JSONObject] = offsetRangesDStream.map(
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

    // 对获取到的数据进行分流操作
    /**
     * 日志数据 构成：
     * 页面日志数据：
     *        公共数据
     *        页面数据
     *        曝光数据
     *        事件数据
     *        错误数据
     * 启动日志数据：
     *        公共数据
     *        启动数据
     *        错误数据
     */
    /**
     * 数据的分流原则：
     *    页面日志数据：
     *        页面数据：公共数据 + 页面数据 --》 DWD_PAGE_LOG_TOPIC_1118
     *        曝光数据：公共数据 + 曝光数据 --》 DWD_DISPLAY_TOPIC_1118
     *        事件数据：公共数据 + 事件数据 --》 DWD_ACTION_TOPIC_1118
     *    启动日志数据：
     *        启动数据：公共数据 + 启动数据 --》 DWD_START_TOPIC_1118
     *    错误日志数据： 页面日志中的错误数据 + 启动日志中的错误数据
     *        错误数据： 页面 err + 启动err --》DWD_ERROR_TOPIC_1118
     */
    // 页面数据的topic
    val page_topic: String = "DWD_PAGE_LOG_TOPIC_1118"
    // 曝光数据 的 topic
    val display_topic: String = "DWD_DISPLAY_TOPIC_1118"
    // 事件数据 的 topic
    val action_topic: String = "DWD_ACTION_TOPIC_1118"
    // 启动数据 的 topic
    val start_topic: String = "DWD_START_TOPIC_1118"
    // 错误数据 的 topic
    val error_topic: String = "DWD_ERROR_TOPIC_1118"

    jsonObjStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
            jsonObjIter =>{
              for (jsonObj <- jsonObjIter) {
                // 判断是否 是 Error 的数据: 可通过 JSONObject对象调用 getJSONObject
                val errJsonObj: JSONObject = jsonObj.getJSONObject("err")
                // 当 一个消息中出现 err 时，则将这个消息不做任何处理发送到 ERROR的 topic 中
                // 错误日志数据
                if (errJsonObj != null) {
                  MyKafkaUtils.send(error_topic, jsonObj.toJSONString)
                } else {
                  // common 内容
                  // 因为 common，err、page 等的都还是 json 对象，所以调用 getJSONObject方法，返回对应的JSON对象。在获取 其中的各个 key 所对应的value 更加方便获取到
                  val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                  val ar: String = commonJsonObj.getString("ar") // area_code        地区编码
                  val uid: String = commonJsonObj.getString("uid") // uid              用户编码
                  val os: String = commonJsonObj.getString("os") // Operation System 操作系统
                  val ch: String = commonJsonObj.getString("ch") // channel          渠道
                  val isNew: String = commonJsonObj.getString("is_new") // is_new           是否为新用户
                  val md: String = commonJsonObj.getString("md") // md               手机型号
                  val mid: String = commonJsonObj.getString("mid") // mid              设备id
                  val vc: String = commonJsonObj.getString("vc") // vc               app版本号
                  val ba: String = commonJsonObj.getString("ba") // ba               手机品牌
                  // 获取 ts
                  val ts: lang.Long = jsonObj.getLong("ts") // ts                跳入时间戳
                  // 页面日志数据
                  val pageJsonObj: JSONObject = jsonObj.getJSONObject("page")
                  if (pageJsonObj != null) {
                    // 获取 page 数据
                    val pageId: String = pageJsonObj.getString("page_id") //  page_id       页面 id
                    val pageItem: String = pageJsonObj.getString("item") //  item          目标id
                    val duringTime: lang.Long = pageJsonObj.getLong("during_time") //  during_time   持续时间 毫秒
                    val pageItemType: String = pageJsonObj.getString("item_type") //  item_type     目标类型
                    val lastPageId: String = pageJsonObj.getString("last_page_id") //  last_page_id  上页id
                    val sourceType: String = pageJsonObj.getString("source_type") //  source_type   来源类型

                    /*
                       将这些字段进行发送，需要考虑按照何种类型进行发送
                          1、JSON :    将各个字段封装到 JSON 对象中发送  （封装起来略嫌麻烦）
                          2、Bean对象 ：将各个字段封装到 Bean 对象中，然后将 Bean 对象转换成 JSONObj 发送
                     */
                    // 将 page 数据，封装为 PageLog 对象发送到  DWD_PAGE_LOG_TOPIC_1118 中
                    val pageLog =
                    PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, ts)

                    // 此处，pageLog.toString 确实不报错，但数据到dwd的topic，对string 处理起来很麻烦，所以以JsonString传入到DWD的topic
                    //                MyKafkaUtils.send(page_topic, pageLog.toString)

                    /*
                      通过JSON.toJSONString()确实可以将我们的对象转换成JSON字符串，
                      但是fast-json 是基于JAVA写的，而在Java版本中，将Bean对象直接转换成JSONString ，是去找各个属性的get，set 方法，然后才能封装成json 中的key-value
                      但当前的 scala 代码没有 get，set 方法，所以采用  new SerializeConfig(true) , 直接使用字段进行转换吗，基于字段进行转换
                    */
                    MyKafkaUtils.send(page_topic, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                    // 曝光数据
                    // 由于 display 的 value 是 json 中的 array，所以调用 JSONObject.getJSONArray（）方法
                    val displayJsonArr: JSONArray = jsonObj.getJSONArray("displays")
                    if (displayJsonArr != null && displayJsonArr.size() > 0) {
                      // 迭代 arr，取出每个 display json 对象
                      for (i <- 0 until displayJsonArr.size()) {
                        // 提取字段
                        val displayObj: JSONObject = displayJsonArr.getJSONObject(i)
                        val displayType: String = displayObj.getString("display_type") // display_type ： 曝光类型
                        val displayItem: String = displayObj.getString("item") // item         :  曝光对象 id
                        val displayItemType: String = displayObj.getString("item_type") // item_type    :  曝光对象类型
                        val posId: String = displayObj.getString("pos_id") // pos_id       :  曝光位置
                        val order: String = displayObj.getString("order") // order        :  出现顺序
                        //封装成PageDisplayLog
                        val pageDisplayLog =
                          PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, displayType, displayItem, displayItemType, order, posId, ts)
                        //发送到对应的主题
                        MyKafkaUtils.send(display_topic, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
                      }
                    }

                    // 事件数据
                    // 事件数据格式同 曝光数据：也是 [] array 结构
                    val actionJsonObj: JSONArray = jsonObj.getJSONArray("actions")
                    if (actionJsonObj != null && actionJsonObj.size() > 0) {
                      for (i <- 0 until actionJsonObj.size()) {
                        val actionObj: JSONObject = actionJsonObj.getJSONObject(i)
                        val actionId: String = actionObj.getString("action_id") //  action_id   :  动作 id
                        val actionItem: String = actionObj.getString("item") //  item        :  目标 id
                        val actionItemType: String = actionObj.getString("item_type") //  item_type   :   目标类型
                        val actionTs: Long = actionObj.getLong("ts") //  ts          :   动作时间戳

                        val pageActionLog = bean.PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, actionId, actionItem, actionItemType, actionTs, ts)
                        MyKafkaUtils.send(action_topic, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
                      }
                    }
                  }
                  // 启动日志数据
                  val startJsonObj: JSONObject = jsonObj.getJSONObject("start")
                  if (startJsonObj != null) {
                    val entry: String = startJsonObj.getString("entry") //
                    val loadingTime: Long = startJsonObj.getLong("loading_time") //
                    val openAdId: String = startJsonObj.getString("open_ad_id") //
                    val openAdMs: Long = startJsonObj.getLong("open_ad_ms") //
                    val openAdSkipMs: Long = startJsonObj.getLong("open_ad_skip_ms") //

                    val startLog: StartLog = bean.StartLog(mid, uid, ar, ch, isNew, md, os, vc, ba, entry, openAdId, loadingTime, openAdMs, openAdSkipMs, ts)
                    MyKafkaUtils.send(start_topic, JSON.toJSONString(startLog, new SerializeConfig(true)))
                  }
                }
              }
              // 由于当前使用的是 foreachPartition ，是在 executor 端执行的，每批次每分区执行一次
              MyKafkaUtils.flush()
            }
          // foreach 里面：提交offset？？？ A executor 执行，每条数据执行一次
          // foreach 里面： 刷写 Kafka 缓冲区？？？ executor 执行，每条数据执行一次，相当于是同步发送
        )
        // foreachRDD 里面， foreach 外面： 提交offset？？B（正确） Driver 端执行： foreachRDD算子，会在Driver端执行，且是周期性执行，每一批次一提交
        // foreachRDD 里面，foreach 外面： 刷写 Kafka 缓冲区？？？B  Driver 端执行，一批次执行一次（周期型），分流是在 Executor 端完成，driver 端做刷写，刷的不是同一个对象缓冲区
        MyOffsetUtils.saveoffset(topicName,groupId,offsetRanges)
      }
    )
    // foreachRDD 外面： 提交 offset ？ C  Driver 端执行，且只执行一次，此方式提交offset 肯定不可以
    // foreachRDD 外面 ： 刷写Kafka 缓冲区？？？ C Driver执行，每次启动程序执行一次，分流是在 executor 端完成，driver 端做刷写，刷的不是同一个对象的缓冲区

    // 开启
    ssc.start()
    ssc.awaitTermination()
  }
}
