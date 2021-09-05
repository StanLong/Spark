package com.stanlong.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.stanlong.spark.util.JdbcUtil
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ListBuffer


object SparkStreaming03_Req1_BlackList {

    def main(args: Array[String]): Unit = {

        // 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkString")
        val ssc = new StreamingContext(sparkConf, Seconds(3)) // (环境配置， 采集周期)  这里设置每3秒采集一次


        //3.定义 Kafka 参数
        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node02:9092,node03:9092,node04:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "spark-kafka",
            "key.deserializer" ->  "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )


        val kafkaDataDS = KafkaUtils.createDirectStream[String, String]( // k,v都是String类型
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("spark-kafka"), kafkaPara) // spark-kafka 为 topic名称
        )

        val adClickData = kafkaDataDS.map(
            kafkaData => {
                val data = kafkaData.value()
                val datas = data.split(" ")
                AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
            }
        )


        val ds = adClickData.transform(
            rdd => {
                // 周期性查询mysql获取黑名单数据
                val blackList = ListBuffer[String]()
                val conn = JdbcUtil.getConnection
                val pstmt = conn.prepareStatement("select userid from black_list")
                val rs = pstmt.executeQuery()
                while (rs.next()) {
                    blackList.append(rs.getString(1))

                }
                rs.close()
                pstmt.close()
                conn.close()


                // 判断点击用户是否在黑名单中
                val filterRdd = rdd.filter(
                    data => {
                        !blackList.contains(data.userid) // 过滤掉黑名单中的用户
                    }
                )

                // 如果不在黑名单，则开始统计 （每个采集周期）
                filterRdd.map(
                    data => {
                        val sdf = new SimpleDateFormat("yyyy-MM-dd")
                        val day = sdf.format(new Date(data.ts.toLong))
                        val user = data.userid
                        val ad = data.adid
                        ((day, user, ad), 1)
                    }
                ).reduceByKey(_+_)
            }
        )

        // 业务逻辑处理
        ds.foreachRDD(

            rdd => {

                // foreachPartition 可以一个分区创建一个连接对象，这样可以大幅度减少连接对象的数量，提升效率
                // rdd.foreachPartition(
                //     iter => {
                //         val conn = JdbcUtil.getConnection
                //         iter.foreach{
                //             case((day, user, ad),count) =>{
                //
                //             }
                //         }
                //         conn.close()
                //     }
                // )

                rdd.foreach{ // 每循环一次会创建一个连接对象，性能不高
                    case((day, user, ad), count) =>{
                        println(s"${day} ${user} ${ad} ${count}")
                        // 如果统计数量超过了点击阈值，则将用户拉入黑名单
                        if(count>=30){
                            val conn = JdbcUtil.getConnection
                            val sql =
                                """
                                  |insert into black_list (userid) values(?)
                                  |on DUPLICATE KEY
                                  |UPDATE userid=?
                                  |""".stripMargin
                            JdbcUtil.executeUpdate(conn, sql, Array(user, user))
                            conn.close()

                        }else{
                            // 如果没有超过阈值，那么需要将当天的广告点击数量进行更新
                            val conn = JdbcUtil.getConnection
                            val sql = """
                                        |select * from user_ad_count where dt= ? and userid = ? and adid = ?
                                        |""".stripMargin
                            val flag = JdbcUtil.isExist(conn, sql, Array(day, user, ad))

                            // 查询统计
                            if(flag){
                                // 查询统计表数据，如果存在则更新，判断更新后的点击数量是否超过阈值
                                val sql = """
                                            |update user_ad_count set count= count + ?
                                            |where dt=? and userid=? and adid=?
                                            |""".stripMargin
                                JdbcUtil.executeUpdate(conn, sql, Array(count, day, user, ad))

                                // 如果超过，将用户拉入黑名单

                                val sql2 = """
                                             |select * from user_ad_count where dt=? and userid=? and adid=? and count>30
                                             |""".stripMargin
                                val flag2 = JdbcUtil.isExist(conn, sql2, Array(day, user, ad))
                                if(flag2){
                                    val sql3 = """
                                                 |insert into black_list (userid) values(?)
                                                 |on DUPLICATE KEY
                                                 |UPDATE userid=?
                                                 |""".stripMargin
                                    JdbcUtil.executeUpdate(conn, sql3, Array(user, user))
                                }
                            }else{
                                // 不存在则新增
                                val sql4 = """
                                         |insert into user_ad_count(dt, userid, adid, count) values(?,?,?,?)
                                         |""".stripMargin
                                JdbcUtil.executeUpdate(conn, sql4, Array(day, user, ad, count))
                            }
                            conn.close()
                        }
                    }
                }
            }
        )

        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }

    // 广告点击数据样例类
    case class AdClickData(ts:String, area:String, city:String, userid:String, adid:String)
}
