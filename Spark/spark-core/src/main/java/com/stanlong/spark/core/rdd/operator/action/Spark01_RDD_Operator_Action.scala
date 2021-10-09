package com.stanlong.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1, 2, 3, 4))

        // 算子 -》map
        println(rdd.map(_ * 2))

        // reduce 聚合
        val result = rdd.reduce(_ + _)
        println(result)

        // collect 方法会将不同分区的数据按照分区的顺序采集到driver端内存中，形成数组
        val ints = rdd.collect()
        println(ints.mkString(","))

        // count 统计数据源中数据的个数
        val l = rdd.count()
        println(l)

        // first 获取数据源中的第一个
        val i = rdd.first()
        println(i)

        // take 获取N个数据
        val ints1 = rdd.take(3)
        println(ints1.mkString(","))

        // takeOrdered 先排序在取N个数据, 默认升序
        val ints2 = rdd.takeOrdered(3)
        println(ints2.mkString(","))

        // aggregateByKey 只会参与分区内计算
        // aggregate 会参与分区内和分区间的运算
        val result1 = rdd.aggregate(10)(_ + _, _ + _)
        println(result1)

        // fold 当分区内和分区间的计算规则相同时aggregate的简化写法
        val result2 = rdd.fold(10)(_ + _)
        print(result2)

        // countByValue 统计每个值出现的次数
        val rdd1 = sc.makeRDD(List(1, 1, 1, 4), 2)
        val result3 = rdd1.countByValue()
        println(result3)

        // countByKey 统计key出现的次数
        val rdd2 = sc.makeRDD(List(("a", 1),("a", 2),("a" ,3),("b", 1)))
        val result4 = rdd2.countByKey()
        println(result4)

        sc.stop()
    }
}

