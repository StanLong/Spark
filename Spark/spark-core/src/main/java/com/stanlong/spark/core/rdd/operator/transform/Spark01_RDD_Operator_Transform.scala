package com.stanlong.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // cogroup = connect + group
        val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
        val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6), ("d", 6)))
        val rdd3 = sc.makeRDD(List(("a", 7), ("b", 8), ("c", 9), ("d", 10), ("e", 0)))
        val coRdd = rdd1.cogroup(rdd2, rdd3)
        coRdd.collect().foreach(println)


        sc.stop()
    }
}

