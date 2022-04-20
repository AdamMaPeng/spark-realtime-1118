package com.atguigu.gmall.realtime.bean

/**
 *  @author Adam-Ma 
 *  @date 2022/4/20 9:44
 *  @Project spark-realtime-1118
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */  
case class StartLog (
                      mid :String,
                      user_id:String,
                      province_id:String,
                      channel:String,
                      is_new:String,
                      model:String,
                      operate_system:String,
                      version_code:String,
                      brand : String ,
                      entry:String,
                      open_ad_id:String,
                      loading_time_ms:Long,
                      open_ad_ms:Long,
                      open_ad_skip_ms:Long,
                      ts:Long
                    ){

}
