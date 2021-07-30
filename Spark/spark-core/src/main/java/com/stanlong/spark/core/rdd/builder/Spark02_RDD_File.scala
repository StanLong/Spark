package com.stanlong.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {

    def main(args: Array[String]): Unit = {
        // 准备环境
        val spakConf = new SparkConf().setMaster("local[*]").setAppName("RDD") // [*] 表示当前系统最大可用核数，如果省略则表示用单线程模拟单核
        val sc = new SparkContext(spakConf)

        // 创建RDD
        //从文件中创建RDD 将文件中的数据作为处理的数据源
        // textFile 以行为单位读取数据，读取到的都是字符串
        val rdd = sc.textFile("datas/1.txt") // 路径默认以当前环境的根路径为基准，也可用写绝对路径。如果文件后缀名相同，也可以使用通配符

        // wholeTextFiles 以文件为单位读取数据，读取的结果是个元组，第一个元素表示路径，第二个元素表示文件内容
        // val rdd = sc.wholeTextFiles("datas")

        rdd.collect().foreach(println)

        // 关闭环境
        sc.stop()


    }

}
