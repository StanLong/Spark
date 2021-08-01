package com.stanlong.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // 算子 -》groupBy
        //val rdd = sc.makeRDD(List(1,2,3,4), 2)

        //// groupBy 会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
        //// 相同key值的数据会放置在一个组中， 分组和分区没有必然的关系
        //def groupFunction(num:Int): Int ={ // 该函数实现的分组是，奇数放一个组，偶数放一个组
        //    num % 2
        //}
        //val groupRdd = rdd.groupBy(groupFunction)

        val rdd = sc.makeRDD(List("Hello", "Spark", "Scala", "Hadoop"), 2)
        val groupRdd = rdd.groupBy(_.charAt(0)) // 根据首字母进行分组

        groupRdd.collect().foreach(println)

        sc.stop()
    }
}

