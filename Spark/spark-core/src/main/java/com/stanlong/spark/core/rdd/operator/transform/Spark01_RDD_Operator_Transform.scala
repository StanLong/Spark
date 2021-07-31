package com.stanlong.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // 算子 -》map
        val rdd = sc.makeRDD(List(1, 2, 3, 4))

        // 转换函数
        // def mapFunction(num:Int): Int ={
        //     num *2
        // }
        // val mapRdd = rdd.map(mapFunction)

        // 转换函数使用匿名函数的形式
        //val mapRdd = rdd.map((num: Int) => {
        //    num * 2
        //})

        //根据scala的自简原则， 匿名函数可以写成如下形式
        val mapRdd = rdd.map(_ * 2)

        mapRdd.collect().foreach(println)

        sc.stop()

        /**
         * 说明
         * 1. rdd的计算一个分区内的数据是一个一个执行逻辑
         *      只有前面一个数据全部的逻辑执行完毕后，才会执行下一个逻辑
         *      分区内数据的执行是有序的
         * 2. 不同分区数据计算是无序的
         */
    }
}

