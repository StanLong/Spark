package com.stanlong.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {

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
            ConsumerStrategies.Subscribe[String, String](Set("spark-kafka"), kafkaPara)
        ) // spark-kafka 为 topic名称

        kafkaDataDS.map(_.value()).print()


        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }
}