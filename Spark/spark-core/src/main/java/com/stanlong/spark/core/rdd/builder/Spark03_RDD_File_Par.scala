package com.stanlong.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Par {

    def main(args: Array[String]): Unit = {
        // 准备环境
        val spakConf = new SparkConf().setMaster("local[*]").setAppName("RDD") // [*] 表示当前系统最大可用核数，如果省略则表示用单线程模拟单核

        val sc = new SparkContext(spakConf)

        // textFile
        // 第一个参数是文件路径，第二个参数是分区数量，默认两个分区
        // val rdd = sc.textFile("datas/1.txt")
        val rdd = sc.textFile("datas/1.txt", 3) // 也可以手动指定分区数，比如指定三个分区

        // 分区数量的计算方式
        // 文件 totalSize
        // goalSize = totalSize / (numSplits == 0 ? 1 : numSplits) 分区数量，如果余数大于商的10%， 则会新增一个分区
        // 24/3 = 8(字节)
        // 24/8=3..0(分区)

        // 分区数据的计算方式
        // 1. spark 读取文件，采用的是hadoop方式读取，所以一行一行的读，和字节数没有关系
        // 2. 读取数据时以偏移量为单位， 偏移量不会被重新读取
        /**
         * Hello World@@ => 偏移量 0-12
         * Hello Scala => 偏移量 13-23
         */
        // 3. 数据分区的偏移量范围的计算
        // 0 => [0, 8] =》 Hello World@@
        // 1 => [8, 16] =》Hello Scala
        // 2 => [16,24] =》空




        // 将处理的数据保存成分区文件
        rdd.saveAsTextFile("output")

        // 关闭环境
        sc.stop()
    }
}
