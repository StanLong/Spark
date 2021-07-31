package com.stanlong.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Par {

    def main(args: Array[String]): Unit = {
        // 准备环境
        val spakConf = new SparkConf().setMaster("local[*]").setAppName("RDD") // [*] 表示当前系统最大可用核数，如果省略则表示用单线程模拟单核

        val sc = new SparkContext(spakConf)

        // textFile 默认两个分区
        val rdd = sc.textFile("datas/1.txt")
        // val rdd = sc.textFile("datas/1.txt", 3) 也可以手动指定分区数，比如指定三个分区

        // 将处理的数据保存成分区文件
        rdd.saveAsTextFile("output")

        // 关闭环境
        sc.stop()
    }
}
