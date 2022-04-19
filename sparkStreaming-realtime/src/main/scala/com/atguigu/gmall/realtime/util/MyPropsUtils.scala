package com.atguigu.gmall.realtime.util

import java.util.ResourceBundle

/**
 *  @author Adam-Ma 
 *  @date 2022/4/18 14:41
 *  @Project spark-realtime-1118
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*  配置文件 解析器
 */
object MyPropsUtils {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(key:String): String = {
    val value: String = bundle.getString(key)
    value
  }

  def main(args: Array[String]): Unit = {
    println(MyPropsUtils("kafka.broker.list"));
  }
}
