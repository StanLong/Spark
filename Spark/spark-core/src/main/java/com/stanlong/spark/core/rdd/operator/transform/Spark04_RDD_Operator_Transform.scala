package com.stanlong.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // 算子 -》flatMap
        val rdd = sc.makeRDD(
            List(List(1, 2), List(3,4))
        )

        val flatRdd = rdd.flatMap(
            list => {
                list
            }
        )

        flatRdd.collect().foreach(println)

        sc.stop()
    }
}

