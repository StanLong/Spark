package com.stanlong.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // 算子 -》glom
        val rdd = sc.makeRDD(List(1,2,3,4), 2)

        // val glomRdd = rdd.glom()

        // 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
        val glomRdd = rdd.glom()

        val maxRdd = glomRdd.map(
            array => {
                array.max
            }
        )
        println(maxRdd.collect().sum)

        sc.stop()
    }
}

