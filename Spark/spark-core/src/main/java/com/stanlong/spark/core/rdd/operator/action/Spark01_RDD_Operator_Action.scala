package com.stanlong.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1, 1, 1, 4), 2)
        val result = rdd.countByValue()
        println(result)

        val rdd1 = sc.makeRDD(List(("a", 1),("a", 2),("a" ,3),("b", 1)))
        val result1 = rdd1.countByKey()
        println(result1)




        sc.stop()
    }
}
