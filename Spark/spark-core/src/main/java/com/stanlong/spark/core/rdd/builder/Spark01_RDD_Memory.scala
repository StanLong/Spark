package com.stanlong.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {

    def main(args: Array[String]): Unit = {
        // 准备环境
        val spakConf = new SparkConf().setMaster("local[*]").setAppName("RDD") // [*] 表示当前系统最大可用核数，如果省略则表示用单线程模拟单核
        val sc = new SparkContext(spakConf)

        // 创建RDD
        //从内存中创建RDD 将内存中集合的数据作为处理的数据源
        val seq = Seq[Int](1,2,3,4)
        // val rdd = sc.parallelize(seq) // parallelize 并行, 等同于下面一行
        val rdd = sc.makeRDD(seq) // makeRDD 的底层实现也是调用了 rdd对象的parallelize方法

        rdd.collect().foreach(println)

        // 关闭环境
        sc.stop()


    }

}
