package com.atguigu.gmall.realtime.util

import java.lang.reflect.{Field, Method, Modifier}

import com.atguigu.gmall.realtime.bean.{DauInfo, PageLog}

import scala.util.control.Breaks

/**
*     对象的拷贝： 主要就是对象属性的拷贝
 */
object MyBeanUtils {
  // 测试代码
  def main(args: Array[String]): Unit = {
    val pageLog: PageLog = PageLog("m1001", "u1001",null,null,null,null,null ,"v1001",null ,"p1001",null,null,null,1111,null,22222)

    val dauInfo: DauInfo = new DauInfo()

    copyField(pageLog, dauInfo)

    println(dauInfo)
  }

  def copyField(srcObj : AnyRef, destObj : AnyRef): Unit ={
    // 1、首先进行判断，如果 srcObj 或则和 destObj 为null，则直接return
    if ( srcObj == null || destObj == null) {
      return
    }

    // 2、从 srcObj 中获取到所有的属性，逐一拷贝到 destObj 对应的属性中
    val srcFields: Array[Field] = srcObj.getClass.getDeclaredFields

    // 3、迭代 srcObj 中的属性
    for (srcField <- srcFields) {
      // 如果对于私有的属性，使其依然有操作的权限
      srcField.setAccessible(true)
//      判断destObj是否有该属性及是否为final
      Breaks.breakable{
        try{
          // 根据名称获取到 destObj 中对应的属性
          val destField: Field =
            destObj.getClass.getDeclaredField(srcField.getName)
          // 进行判断，如果 destField 属性的修饰符 为 val 的，则没法进行拷贝，此时，直接结束当前循环，到下一次循环
          if(destField.getModifiers.equals(Modifier.FINAL)){
            Breaks.break()
          }
          /**
           * Scala 中 会自动为类中的属性提供 get、set 方法
           * get ： fieldName（）
           * set ： fieldName_$eq()
           */
          // 通过srcObj 中的 getter() 获取 srcField 中的值，通过descObj 的 setter（），将从srcObj 中获取到的对应的值进行赋值
          // 获取 srcObj 中的 getter（） 方法
          val getMethodName: String = srcField.getName
          val srcFieldGetter: Method = srcObj.getClass.getDeclaredMethod(getMethodName)
          // 获取 srcfield 的值
          val srcFieldValue: AnyRef = srcFieldGetter.invoke(srcObj)

          // 获取 destObj 中相同的属性的 setter（） 方法 :可能获取不到这个方法，所以需要处理异常
          val setMethodName: String = srcField.getName +"_$eq"
          val destFieldSetter: Method = destObj.getClass.getDeclaredMethod(setMethodName, destField.getType)
          // 将 srcFieldValue 拷贝给 destField
          destFieldSetter.invoke(destObj, srcFieldValue)
        }catch {
          // NoSuchMethodException
          case ex: Exception => Breaks.break() // 这个方法也在抛异常，所以需要将整个代码进行捕获，通过 Breaks.breakable()
        }
      }
    }
  }


}
