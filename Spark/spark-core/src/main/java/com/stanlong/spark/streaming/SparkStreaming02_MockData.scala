package com.stanlong.spark.streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object SparkStreaming02_MockData {

    def main(args: Array[String]): Unit = {

        // 生成模拟数据
        // 格式: timestamp area city userid adid
        // 含义: 时间戳 省份 城市 用户 广告

        // 数据流程
        // 模拟数据 =》 Kafka =》 SparkStreaming =》 统计分析

        // 创建配置对象
        val prop = new Properties()
        // 添加配置
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node04:9092")
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

        // 根据配置创建 Kafka 生产者
        val producer = new KafkaProducer[String, String](prop)


        while(true){
            mockData().foreach(
                data => {
                    // 向Kafka中生成数据
                    val record = new ProducerRecord[String, String]("spark-kafka", data)
                    producer.send(record)


                }
            )
            Thread.sleep(2000)

        }
    }

    def mockData(): ListBuffer[String] ={
        val list = ListBuffer[String]()
        val areaList = ListBuffer[String]("华东", "华北" , "华南")
        val cityList = ListBuffer[String]("北京","上海","深圳")
        for (i <- 1 to new Random().nextInt(50)){
            val area = areaList(new Random().nextInt(3))
            val city = cityList(new Random().nextInt(3))
            var userid = new Random().nextInt(6) + 1 //编号从1开始
            var adid = new Random().nextInt(6) + 1
            list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
        }
        list
    }


}
