package com.stanlong.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // partitionBy 根据分区的规则对数据进行重新分区
        val rdd = sc.makeRDD(List(1,2,3,4))
        val mapRdd = rdd.map((_, 1))
        mapRdd.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")

        sc.stop()
    }
}

