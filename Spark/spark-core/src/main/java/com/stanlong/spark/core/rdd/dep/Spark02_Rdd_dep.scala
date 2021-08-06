package com.stanlong.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Rdd_dep {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        val lines: RDD[String] = sc.textFile("datas/1.txt")
        println(lines.dependencies) // dependencies 打印依赖关系
        println("/*************************")

        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.dependencies)
        println("/*************************")

        val wordToOne = words.map(word=>(word,1))
        println(wordToOne.dependencies)
        println("/*************************")

        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        println(wordToSum.dependencies)
        println("/*************************")

        val array: Array[(String, Int)] = wordToSum.collect()
        array.foreach(println)


        // 关闭连接
        sc.stop()
    }

}
