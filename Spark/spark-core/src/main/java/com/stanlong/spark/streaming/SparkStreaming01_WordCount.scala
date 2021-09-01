package com.stanlong.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {

        // 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkString")
        val ssc = new StreamingContext(sparkConf, Seconds(3)) // (环境配置， 采集周期)  这里设置每3秒采集一次

        // 逻辑处理
        // 获取端口数据
        val lines = ssc.socketTextStream("127.0.0.1", 9999)
        val words = lines.flatMap(_.split(" "))
        val wordToOne = words.map((_, 1))
        val wordCount = wordToOne.reduceByKey(_ + _)
        wordCount.print()

        // 由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
        // main方法执行完毕后，应用程序也会自动结束，所以不能让main方法执行完毕
        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }

}
