package com.atguigu.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{DauInfo, PageLog}
import com.atguigu.gmall.realtime.util.{MyBeanUtils, MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.{SparkConf, streaming}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
    // 5.2.1 自我审查
    // 自我审查： 将 last_page_id != null (当前页面有上一级页面) 过滤掉，只保留每次访问的首页
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )
    // 自我审查后的数据量
    filterDStream.cache()
    filterDStream.foreachRDD(
      rdd => {
        println("自我审查后的数据量： " + rdd.count())
//        println("*" * 150)
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

    // 5.2.2 第三方审查
    /*
      第三方审查： 使用 Redis 来对同一个mid的多个 page页面进行去重，保留一个 mid，存储到Redis中，达到每日用户数的存储，记录到Redis 后，在将数据写入到 ES中
        类型     ：  list          |     set
        key      :  DAU:DATE
        value    :  mid
        写入API   ：lpush/rpush    |    sadd
        读取API   : lrange         |    sismember
        是否过期   ：每日过期
     */
//    filterDStream.filter()   每条数据进行过滤，Redis 开启，关闭的频率太高，使用 mapPartitions 优化
    // 常规的mapPartitions ： [A, B, C] --> [AA, BB, CC] ,此处 ：  [A, B, C] --> [AA, BB]
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(
      Iterator => {
        // 提取要的数据
        // pageLogs: ListBuffer[PageLog] : 用于 存储 需要的mid
        val pageLogs: ListBuffer[PageLog] = mutable.ListBuffer[PageLog]()
        // 获取 Redis 连接
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        // 对日期进行格式化
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

        for (pageLog <- Iterator) {
          // 提取每条数据的 mid （我们日活的统计基于 mid ，也可以基于 uid）
          // redisValue
          val mid: String = pageLog.mid
          // 获取日期 , 因为我们要测试不同天的数据，所以不能直接获取系统时间
          val ts: Long = pageLog.ts
          val dateStr: String = sdf.format(new Date(ts))

          // redisKey
          val redisDauKey = s"DAU:${dateStr}"

          /*   下面代码在分布式环境中，存在并发问题，可能多个并行度同时进入到if 中，导致最终保留同一个 mid 的多余数据
            // list
            val mids: util.List[String] = jedis.lrange(redisDauKey, 0, -1)
           if( !mids.contains(mid)) {
              jedis.lpush(redisDauKey, mid)
              pageLogs.append(pageLog)
           }

            // set
  //          if (jedis.sismember(redisDauKey, mid) == 0) {
  //            jedis.sadd(redisDauKey, mid)
  //          }
            // set
              val setMids: util.Set[String] = jedis.smembers(redisDauKey)
            if (!setMids.contains(mid)){
             jedis.sadd(redisDauKey,mid)
              pageLogs.append(pageLog)
            }
          */
          // 为了解决分布式并发写入的问题，可以使用 set中的 sadd 方法，sadd 添加数据成功返回1，不成功返回 0
          val isNew: lang.Long = jedis.sadd(redisDauKey, mid)
          if (isNew == 1L) {
            pageLogs.append(pageLog)
            // 设置过期时间
            jedis.expire(redisDauKey, 24 * 60 * 60)
          }
        }
        // 关闭 redis
        jedis.close()
        pageLogs.toIterator
      }
    )

    // 第三方审查后的数据量
    redisFilterDStream.cache()
    redisFilterDStream.foreachRDD({
      rdd => {
        println("第三方审查后的数据量 ： " + rdd.count())
        println("*" * 150)
      }
    })

    // 5.3 维度关联
    /*
       将 pageLog 中的数据都取出来，存储到 DauInfo 中
     */
    val dauInfoStream: DStream[DauInfo] = redisFilterDStream.mapPartitions(
      pageLogIter => {
        // 创建 迭代器对象，用于 最终返回
        val dauInfos: ListBuffer[DauInfo] = ListBuffer[DauInfo]()

        // 开启Redis
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()

        // 对 ts 进行格式化
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        for (pageLog <- pageLogIter) {
          // 创建 dauInfo  对象，用于接收 pageLog 中的数据
          val dauInfo: DauInfo = new DauInfo()
          // 1、将 pageLog 中已有的字段拷贝到DauInfo 中
          //笨办法： 将 pageLog 中的每个字段的值挨个提取，赋值给dauInfo 中对应的字段
          // 好办法 ： 通过对象拷贝来完成
          // dauInfo 中 对应 pageLog 中的数据都已经取到
          MyBeanUtils.copyField(pageLog, dauInfo)

          // 2、部分维度
          // 2.1 用户信息维度 ： 性别 + 年龄
          val userId: String = pageLog.mid
          // 从 Redis 中 通过 Key：DIM:${tableName}:${id} ，获取到 该用户的 data
          val redisUserKey: String = s"DIM:USER_INFO:${userId}"
          val userJson: String = jedis.get(redisUserKey)
          val userJsonObj: JSONObject = JSON.parseObject(userJson)
          // 获取性别
          val gender: String = userJsonObj.getString("gender")
          val birthday: String = userJsonObj.getString("birthday")
          // 换算年龄
          val birthdayLD: LocalDate = LocalDate.parse(birthday)
          val nowLD: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayLD, nowLD)
          val age: Int = period.getYears

          // 将 用户的性别 + 年龄存储到 dauInfo 对象中
          dauInfo.user_gender = gender
          dauInfo.user_age = age.toString

          // 2.2 地区信息维度: province_name 、province_iso_code、 province_3166_2、 province_area_code
          val province_id: String = pageLog.province_id
          val redisProvinceKey = s"DIM:BASE_PROVINCE_${province_id}"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
          // 获取 province_name
          val provinceName: String = provinceJsonObj.getString("name")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
          val provinceIso3166: String = provinceJsonObj.getString("iso_3166_2")

          //补充维度
          dauInfo.province_name = provinceName
          dauInfo.province_area_code = provinceAreaCode
          dauInfo.province_iso_code = provinceIsoCode
          dauInfo.province_3166_2 = provinceIso3166

          // 2.3 日期字段处理
          val dthr: String = sdf.format(new Date(pageLog.ts))
          val dthrArr: Array[String] = dthr.split(" ")
          val dt: String = dthrArr(0)
          val hr: String = dthrArr(1).split(":")(0)

          //补充维度
          dauInfo.dt = dt
          dauInfo.hr = hr

          // 将处理完后的 dauInfo 追加到 dauInfos
          dauInfos.append(dauInfo)
        }

        // 关闭 Jedis 连接
        jedis.close()
        dauInfos.toIterator
      }
    )
    dauInfoStream


    // 开启 StreamingContext 环境
    ssc.start()
    ssc.awaitTermination()
  }
}
