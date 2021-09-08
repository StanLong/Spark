package com.stanlong.spark.streaming

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import com.stanlong.spark.util.JdbcUtil
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


object SparkStreaming05_Req3_AdClick {

    def main(args: Array[String]): Unit = {

        // 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkString")
        val ssc = new StreamingContext(sparkConf, Seconds(5)) // (环境配置， 采集周期)  这里设置每5秒采集一次


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

        // 最近一分钟，每10秒计算一次
        val reduceDS = adClickData.map(
            data => {
                val ts = data.ts.toLong
                val newts = ts / 10000 * 10000
                (newts, 1)
            }
        ).reduceByKeyAndWindow((x:Int, y:Int) => {x+y}, Seconds(60), Seconds(10))
        // 打印数据到控制台
        // reduceDS.print()


        // 效果展示
        reduceDS.foreachRDD(
            rdd => {
                val list = ListBuffer[String]()
                val datas = rdd.sortByKey(true).collect()
                datas.foreach{
                    case(time, cnt) => {
                        val timeString = new SimpleDateFormat("mm:ss").format(new Date(time.toLong))
                        list.append(s"""{ "xtime":"${timeString}", "yval":"${cnt}" }""")
                    }
                }

                // 将生成的数据输出到文件，供页面动态展示
                val out = new PrintWriter(new FileWriter(new File("D:\\StanLong\\git_repository\\Spark\\Spark\\datas\\adclick\\adclick.json")))
                println("[" + list.mkString(",") + "]")
                out.println("[" + list.mkString(",") + "]")
                out.flush()
                out.close()
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
