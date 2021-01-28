package app

import com.alibaba.fastjson.{JSON}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{DateUtil, KafkaUtil, RedisUtil}

object DauApp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        val topic = "realtimeSpark"
        val groupId = "MeltyKiss"
        val inputDstream = KafkaUtil.getKafkaStream(topic, ssc, groupId)
//        val jsonStringDstream = inputDstream.map(_.value())
//        jsonStringDstream.print()

        // 给原始数据新增了两个字段：dt和hr
        val jsonObjDstream = inputDstream.map {
            record => {
                val jsonString = record.value()
                val jsonObject = JSON.parseObject(jsonString)
                val ts = jsonObject.getLong("ts")
                // 根据ts字段解析出日期和时间，加到原数据中
                jsonObject.put("dt", DateUtil.tsToDate(ts).date)
                jsonObject.put("hr", DateUtil.tsToDate(ts).hour)
                jsonObject
            }
        }

        // 根据Last_page_id字段过滤出用户打开App的记录
        val firstPageJsonObjDstream = jsonObjDstream.filter {
            jsonObj => {
                val pageJsonObj = jsonObj.getJSONObject("page")
                if (pageJsonObj != null) {
                    val lastPageId = pageJsonObj.getString("last_page_id")
                    if (lastPageId == null || lastPageId.length == 0) {
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        }

        /**
         * 离线计算和实时计算缓存的差别：
         * 离线计算，没缓存，大不了慢条斯理再算一次
         * 实时计算，没缓存，第一次消费完Kafka的数据，提交了偏移量。第二次呢？
         */

        firstPageJsonObjDstream.cache()
        firstPageJsonObjDstream.count().print()
        // 去重优化
        val dauDstream = firstPageJsonObjDstream.mapPartitions(
            partition => {
                val jedis = RedisUtil.getJedisClient
                val filteredList = partition.filter {
                    jsonObj => {
                        val mid = jsonObj.getJSONObject("common").getString("mid")
                        val dt = jsonObj.getString("dt")

                        val key = "dau:" + dt
                        val isNew = jedis.sadd(key, mid)
                        jedis.expire(key, 3600 * 24)

                        if (isNew == 1L) {
                            true
                        } else {
                            false
                        }
                    }
                }
                // 为什么这么早就能关闭呢？因为我们只需要isNew的值呀。拿到isNew，我们就可以把线程还回去咯
                jedis.close()
                filteredList
            }
        )

//        //优化过 ： 优化目的 减少创建（获取）连接的次数 ，做成每批次每分区 执行一次
//        val dauDstream: DStream[JSONObject] = firstPageJsonObjDstream.mapPartitions { jsonObjItr =>
//            val jedis = RedisUtil.getJedisClient //该批次 该分区 执行一次
//            val filteredList: ListBuffer[JSONObject] = ListBuffer[JSONObject]()
//            for (jsonObj <- jsonObjItr) { //条为单位处理
//                //提取对象中的mid
//                val mid: String = jsonObj.getJSONObject("common").getString("mid")
//                val dt: String = jsonObj.getString("dt")
//                // 查询 列表中是否有该mid
//                // 设计定义 已访问设备列表
//
//                //redis   type? string （每个mid 每天 成为一个key,极端情况下的好处：利于分布式  ）   set √ ( 把当天已访问存入 一个set key  )   list(不能去重,排除) zset(不需要排序，排除) hash（不需要两个字段，排序）
//                // key? dau:[2021-01-22] (field score?)  value?  mid   expire?  24小时  读api? sadd 自带判存 写api? sadd
//                val key = "dau:" + dt
//                val isNew = jedis.sadd(key, mid)
//                jedis.expire(key, 3600 * 24)
//
//                // 如果有(非新)放弃    如果没有 (新的)保留 //插入到该列表中
//                if (isNew == 1L) {
//                    filteredList.append(jsonObj)
//                }
//            }
//            jedis.close()
//            filteredList.toIterator
//        }

        // 去重
//        val dauStream = firstPageJsonObjDstream.filter {
//            jsonObj => {
//                val mid = jsonObj.getJSONObject("common").getString("mid")
//                val dt = jsonObj.getString("dt")
//
//                val jedis = RedisUtil.getJedisClient
//                val key = "dau:" + dt
//                val isNew = jedis.sadd(key, mid)
//                jedis.expire(key, 3600 * 24)
//                // 为什么这么早就能关闭呢？因为我们只需要isNew的值呀。拿到isNew，我们就可以把线程还回去咯
//                jedis.close()
//
//                if (isNew == 1L) {
//                    true
//                } else {
//                    false
//                }
//            }
//        }
        dauDstream.count().print()
        //dauDstream.print(1000)

        ssc.start()
        ssc.awaitTermination()
    }
}
