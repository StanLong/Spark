package com.stanlong.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)


        // 算子 -》mapPartitionsWithIndex
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

        val mapRdd = rdd.mapPartitionsWithIndex( // 打印数字和数字所在的分区
            (index, iter) => {
                iter.map(
                    num => {
                        (index, num)
                    }
                )
            }
        )

        mapRdd.collect().foreach(println)










        sc.stop()
    }
}

