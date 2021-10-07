package com.stanlong.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("c", 7)))
        val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6), ("d", 6)))
        val coRdd = rdd1.cogroup(rdd2)
        coRdd.collect().foreach(println)







        sc.stop()
    }
}

