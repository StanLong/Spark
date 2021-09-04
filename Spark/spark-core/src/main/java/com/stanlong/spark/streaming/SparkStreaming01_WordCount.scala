package com.stanlong.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {

        // 设置数据从检查点恢复
        val ssc = StreamingContext.getActiveOrCreate("cp", () => {
            // 环境准备
            val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkString")
            val ssc = new StreamingContext(sparkConf, Seconds(3)) // (环境配置， 采集周期)  这里设置每3秒采集一次

            val lines = ssc.socketTextStream("localhost", 9999)
            val wordToOne = lines.map((_, 1))

            // 窗口的范围应该是采集周期的整数倍
            // 窗口可以滑动，但是默认情况下，一个采集周期进行滑动，这样的化可能会出现重复的计算
            // 为了避免这种情况，可以改变滑动的步长
            val windowDS = wordToOne.window(Seconds(6), Seconds(6)) // (窗口时长, 滑动步长)
            val wordToCount = windowDS.reduceByKey(_ + _)
            wordToCount.print()
            ssc
        })

        ssc.checkpoint("cp")

        // 1. 启动采集器
        ssc.start()

        // 2. 等待采集器的关闭
        ssc.awaitTermination()

    }


}
