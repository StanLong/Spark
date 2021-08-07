package com.stanlong.spark.core.rdd.part

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(
            ("nba", "#######"),
            ("cba", "#######"),
            ("wnba", "#######"),
            ("nba", "#######")
        ))

        val partRdd = rdd.partitionBy(new MyPartitioner)
        partRdd.saveAsTextFile("output")

        sc.stop()

    }

    /**
     * 自定义分区
     * 1. 继承 Partitioner
     * 2. 重写方法
     */
    class MyPartitioner extends Partitioner {
        // 分区数量
        override def numPartitions: Int = 3

        // 返回数据的分区索引， 索引从0开始
        override def getPartition(key: Any): Int = {
            key match {
                case "nba" => 0
                case "wnba" => 1
                case _ => 2
            }
        }
    }

}

