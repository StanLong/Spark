package com.stanlong.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // 算子 -》mapPartitions
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

        // mapPartitions
        // 以分区为单位进行数据转换操作
        // 但是会将整个分区的数据加载到内存进行引用
        // 如果处理完的数据不被释放，在内存较小，数据较大的场合下，容易出现内存溢出
        val mapRdd = rdd.mapPartitions(
            iter => { // 有几个分区 iter 就会迭代几次
                iter.map(_ * 2)
            }
        )

        mapRdd.collect().foreach(println)

        sc.stop()
    }
}

