package com.stanlong.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // 算子 -》repartition 扩大分区
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 4)

        // repartition 底层代码调用的就是 coalesce， 而且肯定采用shuffle
        val newRdd = rdd.repartition(3)
        newRdd.saveAsTextFile("output")

        // coalesce 也可以扩大分区吗，但是如果不进行shuffle操作，则不起作用

        sc.stop()
    }
}

