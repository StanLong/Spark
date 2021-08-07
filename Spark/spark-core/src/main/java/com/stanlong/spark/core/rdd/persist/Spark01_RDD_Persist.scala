package com.stanlong.spark.core.rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        val list = List("Hello Scala", "Hello World")

        val rdd = sc.makeRDD(list)

        val flatRdd = rdd.flatMap(_.split(" "))

        val mapRdd = flatRdd.map(word =>{
            println("@@@@@@@@@@@")
            (word, 1)
        })

        // checkpoint 需要落盘，需要指定检查点保存路径
        // 检查点路径保存的文件，当作业执行完毕后，不会被删除
        sc.setCheckpointDir("cp") // 一般保存路径都是在分布式存储系统中
        mapRdd.checkpoint()

        val reduceRdd = mapRdd.reduceByKey(_ + _)

        reduceRdd.collect().foreach(println)

        println("******************")

        val groupRdd = mapRdd.groupByKey()
        groupRdd.collect().foreach(println)



    }

}
